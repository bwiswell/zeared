# `_factories.py` — session-opening factories

`peer()` / `client()` / `hub()` / `open()` plus the shared retry,
config-building, and managed-wrap helpers. Lives outside `__init__.py`
so the package init stays a thin re-export module.

## Public API (re-exported by `__init__.py`)

- `peer(*, connect=, listen=, config=, zenoh_config=, retry=,
  initial_backoff=, max_backoff=, max_attempts=, auto_reconnect=,
  probe_interval=, timestamping=True, retention_ttl=None,
  resync_jitter=, resync_concurrency=) -> Session | ManagedSession`
- `client(router=None, *, config=, zenoh_config=, retry=,
  initial_backoff=, max_backoff=, max_attempts=, auto_reconnect=,
  probe_interval=, timestamping=True, retention_ttl=None,
  resync_jitter=, resync_concurrency=) -> Session | ManagedSession`

  The four reconnect knobs (0.4.0) resolve like the retry ones: an explicit
  keyword, else `config`'s field, else `False` / `10.0` / `2.0` / `4`
  (`_resolve_session_knobs`).
- `hub(*, listen=, connect=, config=, zenoh_config=, retry=,
  initial_backoff=, max_backoff=, max_attempts=, timestamping=True) ->
  Session` — a **router-mode** relay for NAT-gated nodes (see
  [`hubd.md`](hubd.md)). Relays pub/sub, queries, and liveliness in-process;
  no `zenohd` binary. Returns a raw `zenoh.Session` (a listener has no
  zeared-owned resources to supervise). Without `listen` (or
  `config.listen`), the hub binds the listen endpoints its Zenoh config
  already names, and `['tcp/0.0.0.0:7447']` only when nothing names any
  (0.3.9); `connect` links to other hubs.
- `open(cfg: SessionConfig) -> Session | ManagedSession` — dispatches
  on `cfg.mode` (`PEER` / `CLIENT` / `ROUTER`); a `ManagedSession` when
  `cfg.auto_reconnect` is set (0.4.0). Router mode refuses it.

`config=` provides a declarative base spec; explicit kwargs override
per-call. `zenoh_config=` replaces the config zeared would build: it is
taken as given, and only the endpoint fields are written over it, and its
mode when it sets none (0.4.2). Each factory opens its own mode: `peer`
a peer, `client` a client, `hub` a router. A raw config without a mode
gets the factory's, and one naming another raises `ValueError`, naming
the factory that opens it, before anything opens (`_claim_mode`). Until
0.4.2 a config without a mode opened Zenoh's default, a peer, from
`client()` or `hub()`.

**Endpoints, in every mode** (0.3.9): an endpoint kwarg or field (`router`,
`connect`, `listen`) is written over the Zenoh config when set; when unset,
the endpoints the config already names stand, whether they came from
`zenoh_config=`, a `zenoh_config_file` or `zenoh_overrides`. So `client()`
opens on a config's own connect endpoints and raises `TypeError` only when
there are none anywhere.
`auto_reconnect=True` returns a `ManagedSession` wrapper instead of a
raw `zenoh.Session`.

## Internal helpers (used by `peer` / `client` / `hub`)

- `_open_with_retry(open_fn, *, retry, initial_backoff, max_backoff,
  max_attempts, endpoint_label)` — exponential-backoff retry on
  initial open. Uses `time.sleep`; sync callers can interrupt via
  `KeyboardInterrupt`.
- `_build_config_for_peer(connect, listen, zenoh_config, *,
  timestamping)` / `_build_config_for_client(endpoints, zenoh_config,
  *, timestamping)` — build the `zenoh.Config` for each mode. When
  `zenoh_config is None` we apply zeared's own settings
  (`_apply_defaults`): the mode, `timestamping/enabled=true` unless
  `timestamping=False` (see `config.md` → "Default `timestamping=True`
  injection") and `transport/shared_memory/enabled=false` (see
  `config.md` → "Shared memory off"). When handed a config, we don't
  touch it, except to write endpoints that were given. For a
  `zenoh_config=` kwarg that is silent-respect; a config built from
  `zenoh_config_file` / `zenoh_overrides` arrives with zeared's settings
  already applied by `_resolve_zenoh_config`.
  `_build_config_for_router(listen, connect, zenoh_config, *,
  timestamping)` is the hub's; `listen=None` keeps the config's own.
- `_resolve_zenoh_config(config, zenoh_config)` — the config the
  builders above are handed. A `zenoh_config=` kwarg is returned as is;
  else, when the `SessionConfig` sets `zenoh_config_file` or
  `zenoh_overrides`, a config built from the file, then zeared's own
  settings (`_apply_defaults` with timestamping always on, so the
  `timestamping=` kwarg doesn't reach it), then the overrides; else
  `None`, and the builders build the whole config.
- `_endpoints_set(c, key)` / `_config_endpoints(c, key)` — whether a
  config names its own `listen/endpoints` or `connect/endpoints` (an unset
  key reads back as Zenoh's default), and which.
- `_resolve_hub_listen(listen, config, zenoh_config)` — a hub's listen
  endpoints: `listen=`, then `config.listen`, then the config's own (left
  in place), then `tcp/0.0.0.0:7447`. Returns `(to_write, bound)`; the
  daemon logs `bound`.
- `_resolve_retry_knobs(config, retry, initial_backoff, max_backoff,
  max_attempts)` — layer kwargs over the config's retry knobs; shared
  by `peer` / `client` / `hub`.
- `_check_retention_ttl(retention_ttl, *, auto_reconnect, factory_name)`
  — before opening: reject `retention_ttl=` on a session that won't be
  managed (no place to stash a session-level fallback). Shared by
  `peer` / `client` (0.4.2; it ran after opening before, and leaked the
  session).
- `_finalise_session(raw, _open, label, *, ...)` — post-open: return
  raw or wrap as `ManagedSession`. Shared by `peer` / `client`.
- `_wrap_managed(raw, open_fn, label, ...)` — instantiate
  `ManagedSession` and call `start_probe` to spawn the probe +
  reconnect worker threads.

## `retention_ttl` rejection on raw sessions

`peer(retention_ttl=N, auto_reconnect=False)` raises `TypeError` —
raw `zenoh.Session` objects can't accept attribute assignment, so
there's nowhere to stash a per-session TTL fallback. Either set
`auto_reconnect=True` (so the kwarg lands on the `ManagedSession`
wrapper) or use class-level `Cls.RETENTION_TTL`. It raises before
anything opens (0.4.2); until then the session opened first, and the
error leaked it.
