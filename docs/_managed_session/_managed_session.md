# `_managed_session.py` + `_reconnect.py`

Reconnect-aware wrapper around `zenoh.Session`, plus the detection +
restoration machinery. Opt-in via `auto_reconnect=True` on the session
factories.

## User surface

### Context-managed lifecycle (recommended)

```python
# Sync — auto-release on block exit.
with z.peer(auto_reconnect=True) as sess:
    z.session = sess
    Telemetry(id=1, x=1.0, y=2.0).send()
# z.release(session=sess) ran automatically — probe + reconnect
# threads cancelled and joined, registries cleared, raw closed.

# Async sibling.
async with z.apeer(auto_reconnect=True) as sess:
    await Telemetry(id=1, x=1.0, y=2.0).asend(session=sess)
# Same release semantics, run on a thread-pool worker so the event
# loop stays unblocked.
```

`__enter__` returns the wrapper itself (NOT `self.raw()`) — code
inside the block must bind to the `ManagedSession` so it survives
reconnects. Exceptions inside the block don't suppress; release runs
unconditionally.

`auto_reconnect=False` returns a raw `zenoh.Session`; the `with` form
works through Zenoh's own context-manager protocol (which calls
`session.close()` on exit — no zeared-level state to clean up).

### Manual lifecycle

```python
sess = z.peer(auto_reconnect=True, probe_interval=10.0)
# sess is a `ManagedSession`, not a raw `zenoh.Session`.

sess.put('topic', payload)            # delegates to current raw
sess.zid()                            # always reflects the current raw
raw = sess.raw()                      # explicit escape hatch (don't cache)

z.session = sess                      # works exactly like a raw session
z.session.current                     # → the wrapper

z.release(session=sess)               # cancels the probe, tears down state,
                                      # closes the current raw
```

The wrapper passes the same surface as `zenoh.Session` for the methods
zeared cares about (`put`, `get`, `delete`, `declare_publisher`,
`declare_subscriber`, `declare_queryable`, `liveliness`, `zid`,
`info`). Anything else falls through `__getattr__` to the current raw.

## Detection

Two detection paths feed one reconnect:

1. **Active probe** — daemon thread per `ManagedSession`, polls
   `is_closed()` (or `zid()` fallback) every `probe_interval` seconds.
   Required for subscriber-only daemons that never call `put`.
   `probe_interval=0` or `None` disables the active path entirely.
2. **Send-failure fallback** — `put` / `get` / `delete` exceptions on a
   dead raw trigger `_trigger_reconnect` immediately. Catches the
   0–`probe_interval` gap on publisher-heavy paths.

Both feed a single CAS path (`state IDLE → RECONNECTING`); concurrent
triggers collapse to one reconnect attempt.

## Restoration

After the new raw session opens (with backoff via
`SessionConfig.{initial_backoff, max_backoff, max_attempts}`):

1. Atomically swap the wrapper's raw reference (`_swap_raw`).
2. Walk the subscriber registry keyed on `id(managed_session)` and call
   `Subscriber._redeclare(new_raw, managed)` on each. Re-fires retained
   fetch (dedupe-safe via 0.0.9 `DEDUPE`); re-registers the presence
   dispatcher.
3. Walk presence state: `_SessionPresence.replay_to(managed)` re-runs
   `register_will` for every previously-registered envelope under the
   NEW zid. Peers see legitimate offline → online.
4. Quietly close the old raw.

Subscribers opted out via `Cls.on_message(cb, auto_reconnect=False)`
are skipped during the walk — their `_zenoh_subs` stay bound to the
dead raw. Power users who need manual control retain it.

## State machine

| State | Meaning | Wrapped op behaviour |
|-------|---------|----------------------|
| `IDLE`         | Raw session healthy. | Delegates to raw. |
| `RECONNECTING` | Detection fired; reconnect in progress. | Raises `SessionDeadError`. |
| `DEAD`         | Reconnect terminally failed (`max_attempts` exhausted). | Raises `SessionDeadError`. |

The guard is **symmetric** — every wrapper method that observes the raw
session raises `SessionDeadError` during `RECONNECTING` / `DEAD`,
including `zid()`, `liveliness()`, `info`, `put()`, `get()`,
`delete()`, and the `declare_*` methods. One rule to remember: ops
through `ManagedSession` during reconnect raise; never fall through to
the dead raw.

`SessionDeadError` is a subclass of `ZearedError` distinct from generic
transport errors — callers that want to queue, retry, or drop on
reconnect can branch on it without losing reach to other exceptions.

## `on_reconnect(cb)` hook

Daemons that need to know a reconnect happened — to log it, refresh
caller-side caches, re-publish authoritative state, etc. — register a
callback:

```python
def refresh_caches(sess):
    my_app.peer_registry.invalidate()
    my_app.metrics.record_reconnect()

handle = sess.on_reconnect(refresh_caches)
...
handle.cancel()                        # idempotent deregister
```

Multiple callbacks fire in registration order. Sync callables run
inline on the reconnect thread; `async def` callables are scheduled via
`run_coroutine_threadsafe` on the loop captured at registration time
(registering an async callback without a running loop raises
`RuntimeError` immediately — failing at registration beats failing
silently on the reconnect thread at 3am). Exceptions from one callback
log and continue — never break the reconnect itself.

> **Sync callbacks block the reconnect worker.** They run inline on the
> reconnect worker thread, which means a slow sync callback delays the
> wrapper's transition back to `IDLE` and blocks any subsequent reconnect
> detection on this session. Keep sync callbacks quick (cache invalidation,
> log emission, metrics increment); offload anything slow to a worker
> thread (e.g. `threading.Thread(target=heavy, daemon=True).start()`)
> or use an `async def` callback (which schedules on the captured loop,
> not the worker thread).

The cancel handle (`OnReconnectHandle`) mirrors the `Subscriber.close()`
/ `z.batch()` patterns — explicit, no callback-identity ambiguity for
lambdas / bound methods, no zombie callbacks if the registering object
dies.

## `declare_*` returns raw-bound handles

`ManagedSession.declare_publisher` / `.declare_subscriber` /
`.declare_queryable` return handles bound to the **current** raw
session. They do NOT survive reconnect — after the swap, the old
handle's `undeclare()` will raise (the underlying transport is gone)
and the handle no longer routes traffic. Calling these on a managed
session emits a `RuntimeWarning` ("does NOT survive reconnect") to
steer users toward the wrappers that DO compose with reconnect:

| Use case | Reconnect-aware surface | Notes |
|----------|-------------------------|-------|
| Subscribe to a class | `Cls.on_message(cb)` | Subscriber registry walked on reconnect; `_redeclare` rebuilds against new raw. |
| Publish a class    | `msg.send()` | Publisher cache rebuilds lazily; retention queryables rebuilt eagerly. |
| Bulk publish       | `Cls.send_batch(items)` / `with z.batch(): ...` | Batch buffer survives reconnect; mid-flush ops raise `SessionDeadError`. |
| Custom queryable   | Re-declare inside an `on_reconnect(cb)` hook. | No first-class wrapper today — register via the hook. |

`sess.raw().declare_publisher(...)` is the explicit opt-in to the
raw-only contract: you're telling zeared you want the dead-on-reconnect
handle, deliberately. The wrapper still warns when reached via
`sess.declare_publisher(...)`; `sess.raw()` skips the warning entirely.

## Process-wide tracking — `_managed_sessions`

Every `ManagedSession` registers itself in a module-level
`weakref.WeakSet` at `__init__`. `z.release_all()` walks this set first
when tearing down, so a wrapper that has no zeared-level state
registered against it (no subscribers, no publishers, no retained
values) still gets its probe + reconnect threads cancelled and its raw
session closed.

Pre-0.0.17 `release_all` only iterated per-resource registries. A
wrapper opened with `auto_reconnect=True` but never used was invisible
— its threads stayed alive (daemon, so they exited on interpreter
shutdown anyway, but mid-process restarts leaked).

The set is `WeakSet` rather than `set` because the probe + reconnect
threads hold strong refs to the wrapper via their thread-target
arguments — the wrapper stays in the set as long as those threads
run, and vanishes naturally after `_teardown` joins them and the user
drops their reference.

## Session-level retention TTL

`peer(retention_ttl=N)` (or `client(retention_ttl=N)`) sets a
session-wide TTL fallback for retained values:

```python
sess = z.peer(auto_reconnect=True, retention_ttl=300.0)
```

Precedence:
1. **Class-level `Cls.RETENTION_TTL`** — explicit, intentional; always wins.
2. **Session-level `_retention_ttl`** — fallback for classes that
   don't declare their own.
3. **`None`** — no expiration.

Read on every `_handle_query` invocation, so runtime mutation of
either `Cls.RETENTION_TTL` or `managed._retention_ttl` propagates
without restart.

`retention_ttl` is `ManagedSession`-only — passing it on a raw session
(`auto_reconnect=False`) raises `TypeError`. Raw sessions have no
place to stash the per-session value; class-level `RETENTION_TTL` is
the only knob for them. (Same precedent as `gc_interval`.)

## Threads per `ManagedSession`

| Thread | Lifetime | Role |
|--------|----------|------|
| Probe (optional) | start_probe → _probe_cancel | Polls `is_closed()` / `zid()` every `probe_interval`s; signals worker on detected death. Skipped when `probe_interval ∈ {0, None}`. |
| Reconnect worker | start_probe → _probe_cancel | Single long-lived daemon; blocks on `_reconnect_signal`; runs `_reconnect` on each wake. Coalesces concurrent triggers via Event semantics. |

Test paths that bypass `start_probe` and call `_trigger_reconnect`
directly fall back to a one-shot reconnect thread — preserves the
orchestration-driven test ergonomics without holding a worker for
short-lived test fixtures.

## `raw()` — escape hatch

For handing a Zenoh session to non-zeared code (another library, raw
Zenoh APIs not exposed by the wrapper):

```python
raw = sess.raw()
some_other_lib.consume_zenoh_session(raw)
```

**Don't cache `raw()` across reconnect windows.** The returned object
is the underlying `zenoh.Session`; on reconnect we swap in a new one
and the old becomes invalid. Call `raw()` each time you need the
current handle.

## New zid on reconnect

Zenoh sessions get a fresh `zid` on every `zenoh.open()`. Our presence
wills are keyed on zid, so:

- The OLD zid's liveliness token DELETE propagates to peers as we
  reconnect — peers fire our wills locally.
- The NEW zid registers fresh wills via `_SessionPresence.replay_to`.
- Peers then see the new zid come online.

This is correct semantics — for any practical observer, we genuinely
were offline for the reconnect window. Don't try to suppress the
offline signal; it's a feature.

## Lifecycle

| Event | Probe | Raw |
|-------|-------|-----|
| `peer(auto_reconnect=True)` succeeds  | thread declared, started | held |
| Probe / send-failure detects death    | wakes; CAS into RECONNECTING; spawns reconnect thread | unchanged until swap |
| Reconnect open succeeds               | continues | atomic swap; old raw closed quietly |
| Reconnect open exhausts `max_attempts`| `state = DEAD`; cancel set; thread exits | unchanged |
| `z.release(session=managed)`          | cancel set; thread joined (1s) | closed |

## Caveats

- **Lazy publisher / queryable rebuild.** Cached publishers and
  retention queryables are not proactively re-declared during reconnect
  restoration; the next `send()` / next incoming query rebuilds them on
  demand. Keeps the restoration walk cheap.
- **Send during reconnect window: raise.** No buffering or blocking.
  Callers that want either build it on top of the `SessionDeadError`
  contract.
- **Mid-batch reconnect.** A `with z.batch():` buffer doesn't care
  about session identity; the eventual `flush()` hits the current raw.
  Pinned by test, no design.
