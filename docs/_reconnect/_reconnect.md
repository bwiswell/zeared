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
  - `_restore_retention(managed)` — redeclare retention queryables on
    every cache bound to this session.
  - `_restore_subscribers(managed)` — re-declare each registered
    Subscriber against the new raw.
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
4. Walk the retention registry and redeclare queryables.
5. Walk the subscriber registry and re-declare each.
6. Replay every registered presence will under the new zid.
7. Close the old raw session quietly.

If step 2 exhausts `max_attempts`, set `state = DEAD` and stop the probe.

## Restoration order

Dependencies before dependents:

1. **Retention queryables first** — publisher-side infrastructure that
   subscribers' retained-fetch will hit. MUST come before subscriber
   redeclare so a same-process publisher+subscriber pair finds a live
   queryable on the retained-fetch round.
2. **Subscribers** — re-declare zenoh subs, re-fire retained fetch
   (dedupe-safe), re-register presence dispatcher.
3. **Wills** — re-register every previously-registered envelope under
   the new zid; peers see legitimate offline → online.

## Cancellable backoff

`_open_with_backoff` blocks via `cancel.wait(backoff)` rather than
`time.sleep` so `z.release()` can interrupt a long reconnect. The
`_ReconnectAborted` exception unwinds cleanly into a `state = DEAD`
without polluting logs.
