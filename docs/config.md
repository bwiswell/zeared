# `config.py`

Declarative connection spec for a zeared session.

## `z.SessionConfig`

A `@z.zeared` dataclass holding the full set of knobs that `z.peer()` and
`z.client()` accept, plus retry behaviour. Can be constructed, dumped,
loaded, stored, compared, or logged like any seared dataclass.

Named `SessionConfig` (not `Config`) so it doesn't collide with
`zenoh.Config` — the two cohabit when users want raw Zenoh overrides via
the `zenoh_config=` factory kwarg.

```python
@z.zeared
class SessionConfig(z.Zeared):
    mode:            z.Mode        = z.Enum(enum=z.Mode, required=True)
    router:          Optional[str] = z.Str()                # client shortcut
    connect:         list          = z.Str(many=True, missing=[])
    listen:          list          = z.Str(many=True, missing=[])
    retry:           bool          = z.Bool(missing=False)
    initial_backoff: float         = z.Float(missing=0.1)
    max_backoff:     float         = z.Float(missing=30.0)
    max_attempts:    Optional[int] = z.Int()
```

`mode` is a `z.Mode` enum (`Mode.PEER` / `Mode.CLIENT`). Strings still
load via seared's `Enum` field auto-coercion — YAML / JSON configs keep
working — but natural construction needs the enum value:

```python
z.SessionConfig(mode=z.Mode.PEER)         # natural construction
z.SessionConfig.load({'mode': 'peer'})    # string → enum on load
z.SessionConfig.load({'mode': 'galactic'}) # → ValidationError up front
```

## Builders

All builders are immutable: they return a new `SessionConfig` and leave
the original unchanged. Chain them fluently for readable composition.

```python
cfg.replace(max_backoff=60)                       # generic, any fields
cfg.with_retry(retry=True, max_backoff=60)        # retry knobs at once
cfg.with_connect('tcp/foo:7447', 'tcp/bar:7447')  # APPEND to connect
cfg.with_listen('tcp/0.0.0.0:7447')               # APPEND to listen
cfg.set_router('tcp/router:7447')                 # replace router scalar
```

Fluent:

```python
cfg = (
    z.SessionConfig(mode=z.Mode.CLIENT)
    .set_router('tcp/router:7447')
    .with_retry(max_backoff=30)
    .with_connect('tcp/backup:7447')
)
```

`replace(**changes)` rejects unknown keys with `TypeError` (via
`dataclasses.replace`). `with_retry()` with no knobs just enables retry
(pass `retry=False` to disable); specific knobs override only the values
you supply and leave the rest untouched.

## Factory integration

The `peer()` / `client()` factories accept either a
`config=SessionConfig(...)` object, explicit kwargs, or both. When both
are supplied, the explicit kwargs override the corresponding fields on
the config — useful when a shared base spec needs a per-call tweak.

```python
# Declarative
cfg = z.SessionConfig(
    mode=z.Mode.CLIENT,
    router='tcp/router.example.com:7447',
    retry=True, max_backoff=30.0,
)
sess = z.open(cfg)               # unified entry; dispatches on cfg.mode

# Or equivalently
sess = z.client(config=cfg)

# Explicit-kwargs form
sess = z.client(
    'tcp/router.example.com:7447',
    retry=True, max_backoff=30.0,
)

# Mix and match — kwargs override the config's values
sess = z.client(config=cfg, retry=False)   # disable retry just this call
```

## `zenoh_config=` passthrough

Use `zenoh_config: zenoh.Config` to layer raw Zenoh configuration overrides
on top of whichever form you picked:

```python
raw = zenoh.Config()
raw.insert_json5('transport/unicast/compression/enabled', 'true')
sess = z.peer(config=my_cfg, zenoh_config=raw)
```

### Layering rule

zeared's factory kwargs split into two categories:

| Category | Behavior under `zenoh_config=` |
|----------|--------------------------------|
| **Maps to a `zenoh.Config` field** (`timestamping`) | Silenced — you took over the Config; layer Config-level settings yourself or drop the high-level kwarg. |
| **Out-of-band** (`retry`, `initial_backoff`, `max_backoff`, `max_attempts`, `auto_reconnect`, `probe_interval`, `gc_interval`) | Layered freely on top — they don't touch the `zenoh.Config`. |

So `z.peer(zenoh_config=raw, timestamping=True)` does NOT inject
timestamping; `raw` is your responsibility. But
`z.peer(zenoh_config=raw, retry=True, gc_interval=30)` honors both
out-of-band kwargs while leaving `raw` untouched.

## Default `timestamping=True` injection

When you don't pass `zenoh_config=`, the factory injects
`timestamping/enabled=true` into the built `zenoh.Config` by default.
This is what makes `RETAINED + DEDUPE` Just Work — Zenoh's default
config doesn't add HLC timestamps, and without timestamps retention
dedupe is a no-op. Pass `timestamping=False` to opt back out (e.g.
debugging, or a config that for some reason can't have timestamping
enabled).

## Loading from external sources

`SessionConfig` is a seared dataclass — load from env or YAML one-shot:

```python
# Env: ZEARED_SESSION_MODE=peer, ZEARED_SESSION_RETRY=true, ...
cfg = z.SessionConfig.from_env()

# Or with a custom prefix:
cfg = z.SessionConfig.from_env(prefix='MYAPP_')

# YAML — string or path:
cfg = z.SessionConfig.from_yaml('config.yaml')
cfg = z.SessionConfig.from_yaml('mode: peer\nretry: true\n')
```

`from_env` reads `{prefix}MODE`, `{prefix}ROUTER`, `{prefix}CONNECT`
(comma-separated list), `{prefix}LISTEN` (likewise), `{prefix}RETRY`
(true/false), `{prefix}INITIAL_BACKOFF`, `{prefix}MAX_BACKOFF`,
`{prefix}MAX_ATTEMPTS`. Missing optional fields fall back to seared
defaults; missing required `MODE` raises `ValidationError`.

`from_yaml` uses `PyYAML` (lazy import — raises `ImportError` with an
install hint if missing). Top-level YAML must be a mapping; values
flow into `SessionConfig.load`.

## `gc_interval=` per-session presence GC

Each `ManagedSession` runs a daemon GC sweep over the presence
observer's stashed will entries every `gc_interval` seconds, dropping
any whose peer is no longer in `_alive_zids`. Default 60s. Short-lived
peer processes can lower this; long-lived observers can raise it. The
value is stashed on the wrapper and read by `_PresenceObserver`
on first declaration; the existing test pattern of poking
`observer._gc_interval = 0.05` directly still works for fine-grained
test control.

Raw `zenoh.Session` callers (no `auto_reconnect=True`) fall back to the
module default — `ManagedSession` is the strategic path for per-session
tuning.

## Retry semantics

When `retry=True`, zeared wraps `zenoh.open()` in an exponential-backoff
loop. The backoff starts at `initial_backoff` seconds, doubles on each
failure, and caps at `max_backoff`. Stops after `max_attempts` if set,
otherwise retries forever.

Retry attempts log at `INFO` for the first three, escalating to `WARNING`
thereafter — operators see slow-start noise once, then louder if things
stay broken. Logger name: `zeared.connect`.

Retry triggers on `zenoh.open()` raising. Session-alive-but-no-peers
reachability checks are NOT in scope (backlog).
