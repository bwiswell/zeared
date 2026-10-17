# `_reconnect/` — reconnect orchestration for `ManagedSession`

Pattern B subdir. The probe + reconnect-worker thread pair plus the
post-reopen restoration walks. Never imported by user code; the public
surface is `auto_reconnect=True` on `peer()` / `client()`.

## Files

- `_reconnect.py` — orchestration:
  - `start_probe(managed)` — spawn the probe daemon + the long-lived
    reconnect worker. Idempotent.
  - `_probe_loop(managed)` — periodic liveness check.
  - `_reconnect_worker(managed)` — long-lived worker; consumes triggers.
  - `_trigger_reconnect(managed)` — CAS into `RECONNECTING`, signal
    the worker.
  - `_reconnect(managed)` — the actual reopen + restoration pipeline.
- `_link.py` — router-link detection and the re-attach walk (0.4.0):
  - `_check_link(managed)` — one probe tick of detection;
  - `_note_routers(managed)` — the baseline router set;
  - `_link_lost` / `_link_restored` — the paired signals;
  - `_republish_retained(managed)` — the session's retention caches re-put
    their entries;
  - `_run_link_walk(managed)` — the jittered, capped retained re-fetch.
- `_restore.py` — post-reopen walks + the cancellable backoff:
  - `_open_with_backoff(open_fn, *, initial, cap, max_attempts, label,
    cancel)` — exponential-backoff retry with a cancel `Event`.
  - `_ReconnectAborted` — raised by `_open_with_backoff` when the cancel
    fires (used to set `state = DEAD` cleanly during teardown).
  - `_restore_publishers(managed)` — invalidate the cached
    `zenoh.Publisher` handles on every publisher cache bound to this
    session, so the next `send()` re-declares against the new raw.
  - `_restore_retention(managed)` — redeclare retention queryables on
    every cache bound to this session.
  - `_restore_subscribers(managed)` — re-declare each registered
    Subscriber against the new raw.
  - `_restore_queryables(managed)` — re-declare each registered user
    Queryable against the new raw.
  - `_restore_presence(managed)` — re-declare every held presence token
    on the new raw (peers see the keys go offline, then online).

## Detection paths

Two paths feed into the same reconnect implementation:

1. **Probe** (`_probe_loop`) — daemon thread per `ManagedSession`,
   polls `is_closed()` (or `zid()` fallback) every `probe_interval`s.
   Required for subscriber-only daemons that never call `put()`.
2. **Send-failure** (`_trigger_reconnect`) — called from
   `ManagedSession.put` / `.get` / `.delete` exception paths via
   `_note_failure`. Catches the 0–`probe_interval` gap on
   publisher-heavy paths.

Neither path sees a **lost router link**, which a hub restart causes:
the session stays open and Zenoh re-dials by itself. The probe watches the
link separately (next section).

Both feed `_reconnect`, which:

1. CAS `state` IDLE → RECONNECTING.
2. Open a new raw session via `open_fn` with backoff.
3. Atomically swap the wrapper's raw reference.
4. Walk the publisher registry and invalidate its cached handles.
5. Walk the retention registry and redeclare queryables.
6. Walk the subscriber registry and re-declare each.
7. Walk the queryable registry and re-declare each.
8. Re-declare every held presence token on the new raw.
9. Close the old raw session quietly.

If step 2 exhausts `max_attempts`, set `state = DEAD` and stop the probe.

## Link re-attach (0.4.0)

`_link.py`. Each probe tick, after the dead-session check, `_check_link`
reads `info.routers_zid()` and compares the set of router zids with the
last tick's:

- non-empty → empty: `on_link_lost`;
- empty → non-empty, while lost: the **republish**, the **re-attach
  walk**, then `on_link_restored`;
- a non-empty set replaced by a disjoint one: the router restarted between
  two ticks (it came back with a new zid), so both at once.

A session that never had a router never transitions, so peers are
self-gated. A router that keeps a fixed zid and restarts within one
`probe_interval` is invisible to polling; Zenoh's link events could catch
it, a 0.4.x candidate once upstream confirms they're a stable API.

**The republish** (`_republish_retained`) runs first: every retention cache
bound to the session re-puts its live entries with their original
timestamps and the republish marker, so a subscriber that walked before
this session came back still gets what this session published while
detached (`REPLAY`), and one that already has it dedupes the copy. See
[`retention.md`](../retention.md#republish-on-link-restore-040). It runs on
a reconnect's restore too.

**The walk** (`_run_link_walk`) re-runs the retained fetch for every
subscriber on the session that replays (`RETAINED`, `retained_fetch`,
`auto_reconnect`): `Subscriber._replay_retained`, no redeclare, replies
delivered as `REPLAY` and dedupe-safe. Presence needs no step: tokens come
back through the liveliness subscribers themselves (the presence watch's
`history=True` keeper is what makes a client session learn, and so lose
and regain, every token the router knows; see
[`presence.md`](../presence/presence.md#the-subscriber-side-subscriber)).

- With no subscriber to replay, it returns at once — no jitter.
- Otherwise it waits `random.uniform(0, resync_jitter)` (default 2 s,
  interruptible by teardown), so a fleet re-attaching to a restarted hub
  doesn't fetch all at once.
- Then it fetches on at most `resync_concurrency` workers (default 4), one
  task per subscriber, templates in order — so no subscriber's callback
  runs concurrently with its own replay.
- `on_link_restored` fires once every task has finished; failures are
  logged.

It runs on the probe thread, so two link walks never overlap. The
reconnect walk is unchanged (serial, no jitter): a reconnect is a local
event, not fleet-correlated. `_reconnect` fires `on_link_lost` before it
opens a new raw and `on_link_restored` after `on_reconnect`'s callbacks.

**One restore per loss.** Two paths restore the link, the probe and a
reconnect, so:
- the reconnect sets `_restoring` for its whole run, and the probe skips
  its link check meanwhile. After the swap the state already reads
  `IDLE`, and a tick that saw routers then would otherwise restore the
  link alongside the restore walks, firing `on_link_restored` before
  `on_reconnect`, or twice;
- `_link_restored` claims the transition under the session lock
  (`_link_state` goes `'lost'` → `'restoring'` → `'up'`), so a second
  caller returns. A loss while a restore is in flight sets `'lost'`
  again without firing `on_link_lost` (callbacks never saw the link come
  back), and the in-flight restore then fires nothing: the loss's own
  restore will.

## Restoration order

Dependencies before dependents:

1. **Publisher caches first** — the `zenoh.Publisher` handles in
   `publisher._registry` are bound to the raw that just got swapped out.
   First because every walk below it can run user code that publishes: a
   restored subscriber's retained-fetch replay fires callbacks, and so do
   the `on_reconnect` hooks at the end. A send that reaches a stale handle
   is lost, so the cache has to be clean before any of them run.
2. **Retention queryables** — publisher-side infrastructure that
   subscribers' retained-fetch will hit. MUST come before subscriber
   redeclare so a same-process publisher+subscriber pair finds a live
   queryable on the retained-fetch round.
3. **Subscribers** — re-declare zenoh subs, re-fire retained fetch
   (dedupe-safe), re-declare the presence watch (its replay reports the
   keys online now as `REPLAY`).
4. **Queryables** — re-declare each user queryable (compute-serving, no
   replayed state) against the new raw.
5. **Presence** — re-declare every held token on the new raw; peers see
   the keys come back online. After retention, so a peer that sees a key
   return can already fetch its record.

Publishers *invalidate* rather than redeclare, unlike retention: nothing
reads a cached publisher until the next `send()`, so there is no ordering
dependency to satisfy and no reason to pay for handles that may never be
used again. Dropping the handles while keeping the cache object
registered also preserves `_emitted`, the process-lifetime history behind
`published_topics()` — which `clear_publisher_cache()` would discard.

## Cancellable backoff

`_open_with_backoff` blocks via `cancel.wait(backoff)` rather than
`time.sleep` so `z.release()` can interrupt a long reconnect. The
`_ReconnectAborted` exception unwinds cleanly into a `state = DEAD`
without polluting logs.
