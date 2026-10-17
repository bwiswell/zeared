# `_release.py` — session-tearing-down helpers

`release()` / `release_all()`. Lives outside `__init__.py` so the
package init stays a thin re-export module.

## Public API (re-exported by `__init__.py`)

- `release(*, session) -> None` — walk every zeared-owned resource for
  `session` in the right order, then close the session itself.
  Idempotent. Keyword-only `session=` is intentional — no implicit
  module-default fallback.
- `release_all() -> None` — release every zeared-managed session in
  the process. Walks per-session registries (subscribers, user
  queryables, publisher caches, retention caches, presence state) plus
  the `ManagedSession` WeakSet. Idempotent. Useful for
  `atexit.register(release_all)`.

## Order matters

`release(session=)` runs these steps:

1. If a `ManagedSession`, call `_teardown(call_close=False)` first so
   the probe + reconnect worker stop before everything else (no
   probe-detected death triggering reconnect mid-shutdown).
2. Close zeared subscribers on this session (cancel watchdogs,
   undeclare Zenoh subs and their liveliness subs).
3. Close user queryables on this session — undeclare each `Cls.on_query`
   `zenoh.Queryable`.
4. Drop cached publishers — undeclare each declared `zenoh.Publisher`.
5. Drop the retention cache and undeclare its queryable.
6. Clear presence state (withdraw every liveliness token the session holds).
7. Close the Zenoh session itself.

Step 6 (presence-state clear) **must** run before step 7
(`session.close()`) so the token DELETEs propagate to peers BEFORE the
local transport tears down.

## `release_all` walk

Aggregates session refs from every per-session registry into a
`{id(session): session}` dict (deduplicates concurrent refs from
multiple registries), then calls `release(session=)` on each.

The `ManagedSession` WeakSet is walked **first** so wrappers whose only
live state is the probe + reconnect threads (no subscribers, queryables,
publishers, retention, or presence) still get torn down — per-resource
walks below would miss them otherwise. Then subscribers, user queryables,
publisher caches, retention caches and presence state, each yielding the
session refs its entries carry.
