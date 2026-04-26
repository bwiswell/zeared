# `_factories.py` — session-opening factories

`peer()` / `client()` / `open()` plus the shared retry,
config-building, and managed-wrap helpers. Lives outside `__init__.py`
so the package init stays a thin re-export module.

## Public API (re-exported by `__init__.py`)

- `peer(*, connect=, listen=, config=, zenoh_config=, retry=,
  initial_backoff=, max_backoff=, max_attempts=, auto_reconnect=False,
  probe_interval=10.0, timestamping=True, gc_interval=60.0,
  retention_ttl=None) -> Session | ManagedSession`
- `client(router=None, *, config=, zenoh_config=, retry=,
  initial_backoff=, max_backoff=, max_attempts=, auto_reconnect=False,
  probe_interval=10.0, timestamping=True, gc_interval=60.0,
  retention_ttl=None) -> Session | ManagedSession`
- `open(cfg: SessionConfig) -> Session | ManagedSession` — dispatches
  on `cfg.mode`.

`config=` provides a declarative base spec; explicit kwargs override
per-call. `zenoh_config=` layers raw Zenoh overrides on top.
`auto_reconnect=True` returns a `ManagedSession` wrapper instead of a
raw `zenoh.Session`.

## Internal helpers (used by `peer` / `client`)

- `_open_with_retry(open_fn, *, retry, initial_backoff, max_backoff,
  max_attempts, endpoint_label)` — exponential-backoff retry on
  initial open. Uses `time.sleep`; sync callers can interrupt via
  `KeyboardInterrupt`.
- `_build_config_for_peer(connect, listen, zenoh_config, *,
  timestamping)` / `_build_config_for_client(endpoints, zenoh_config,
  *, timestamping)` — build the `zenoh.Config` for each mode. When
  `zenoh_config is None` we set the mode and inject
  `timestamping/enabled=true` (RETAINED + DEDUPE need it). When the
  user supplies a config, we don't touch it (silent-respect).
- `_resolve_retry_knobs(config, retry, initial_backoff, max_backoff,
  max_attempts)` — layer kwargs over the config's retry knobs; shared
  by `peer` / `client`.
- `_finalise_session(raw, _open, label, *, ...)` — post-open: return
  raw or wrap as `ManagedSession`; reject `retention_ttl=` on raw
  sessions (no place to stash a session-level fallback). Shared by
  `peer` / `client`.
- `_wrap_managed(raw, open_fn, label, ...)` — instantiate
  `ManagedSession` and call `start_probe` to spawn the probe +
  reconnect worker threads.

## `retention_ttl` rejection on raw sessions

`peer(retention_ttl=N, auto_reconnect=False)` raises `TypeError` —
raw `zenoh.Session` objects can't accept attribute assignment, so
there's nowhere to stash a per-session TTL fallback. Either set
`auto_reconnect=True` (so the kwarg lands on the `ManagedSession`
wrapper) or use class-level `Cls.RETENTION_TTL`.
