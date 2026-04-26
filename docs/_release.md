# `_release.py` — session-tearing-down helpers

`release()` / `release_all()`. Lives outside `__init__.py` so the
package init stays a thin re-export module.

## Public API (re-exported by `__init__.py`)

- `release(*, session) -> None` — walk every zeared-owned resource for
  `session` in the right order, then close the session itself.
  Idempotent. Keyword-only `session=` is intentional — no implicit
  module-default fallback.
- `release_all() -> None` — release every zeared-managed session in
  the process. Walks per-session registries (subscribers, publisher
  caches, retention caches, presence state, presence observers) plus
  the `ManagedSession` WeakSet. Idempotent. Useful for
  `atexit.register(release_all)`.

## Order matters

`release(session=)` runs six steps:

1. If a `ManagedSession`, call `_teardown(call_close=False)` first so
   the probe + reconnect worker stop before everything else (no
   probe-detected death triggering reconnect mid-shutdown).
2. Close zeared subscribers on this session (cancel watchdogs,
   undeclare Zenoh subs, deregister presence dispatchers).
3. Drop cached publishers — undeclare each declared `zenoh.Publisher`.
4. Drop the retention cache and undeclare its queryable.
5. Stop the presence observer (undeclare alive-sub + will-sub).
6. Clear presence state (undeclare liveliness token + will queryable).
7. Close the Zenoh session itself.

Step 5 (presence-state clear, which undeclares the liveliness token)
**must** run before step 7 (`session.close()`) so the token DELETE
propagates to peers BEFORE the local transport tears down. Flipping
these silently breaks subscribers of peer wills.

## `release_all` walk

Aggregates session refs from every per-session registry into a
`{id(session): session}` dict (deduplicates concurrent refs from
multiple registries), then calls `release(session=)` on each.

The `ManagedSession` WeakSet is walked **first** so wrappers whose only
live state is the probe + reconnect threads (no subscribers,
publishers, retention, or presence) still get torn down — per-resource
walks below would miss them otherwise.
