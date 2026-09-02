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
  - `_restore_wills(managed)` — re-register every presence will under
    the new zid (peers see legitimate offline → online).

## Detection paths

Two paths feed into the same reconnect implementation:

1. **Probe** (`_probe_loop`) — daemon thread per `ManagedSession`,
   polls `is_closed()` (or `zid()` fallback) every `probe_interval`s.
   Required for subscriber-only daemons that never call `put()`.
2. **Send-failure** (`_trigger_reconnect`) — called from
   `ManagedSession.put` / `.get` / `.delete` exception paths via
   `_note_failure`. Catches the 0–`probe_interval` gap on
   publisher-heavy paths.

Both feed `_reconnect`, which:

1. CAS `state` IDLE → RECONNECTING.
2. Open a new raw session via `open_fn` with backoff.
3. Atomically swap the wrapper's raw reference.
4. Walk the publisher registry and invalidate its cached handles.
5. Walk the retention registry and redeclare queryables.
6. Walk the subscriber registry and re-declare each.
7. Walk the queryable registry and re-declare each.
8. Replay every registered presence will under the new zid.
9. Close the old raw session quietly.

If step 2 exhausts `max_attempts`, set `state = DEAD` and stop the probe.

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
   (dedupe-safe), re-register presence dispatcher.
4. **Queryables** — re-declare each user queryable (compute-serving, no
   replayed state) against the new raw.
5. **Wills** — re-register every previously-registered envelope under
   the new zid; peers see legitimate offline → online.

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
