# zeared

`zeared` is a typed [Zenoh](https://zenoh.io/) pub/sub wrapper built on top of
[`seared`](https://www.github.com/bwiswell/seared). Declare a message class
once, get publish, subscribe, and topic routing for free.

> Looking for the serialization library underneath? See its sister package
> [`seared`](https://www.github.com/bwiswell/seared). Its entire public
> surface is re-exported from `zeared`, so `import zeared as z` is enough
> for most use cases.

## Why zeared

- **Typed messages.** Messages are ordinary `@z.zeared` dataclasses with a `TOPIC` class attribute. `send()` publishes; `on_message(cb)` subscribes with a typed callback.
- **Format-string topics.** `TOPIC = 'robot/{id}/telemetry'` auto-derives the Zenoh wildcard `robot/*/telemetry` for subscribers and populates `msg.id` from the actual key expression on receive.
- **Multi-topic classes.** Declare `EXTRA_TOPICS = (...)` to subscribe to several wildcards with one class; a restrictive `send(topic=...)` override lets you publish to any declared template.
- **Multi-segment wildcards.** Trailing `**` in a template matches any number of path segments (MQTT `#` equivalent). Classes with `**` in a template are subscribe-only for that template.
- **Capture-only slots.** Template slots that aren't declared as seared fields become routing-only captures — always surfaced via `meta.captures: dict[str, str]`, never on the payload schema.
- **Tagged-union payloads.** `s.Union` encodes `{action, args}`-style envelopes and decodes to a typed variant instance — pattern-match on `msg.action` at the dispatch site.
- **Long-lived publishers by default.** Every message class transparently caches `zenoh.Publisher` instances per concrete topic (soft-capped, user-overridable). No `Publisher` ceremony — `msg.send()` is the one path.
- **Retained messages (MQTT-style).** Opt in with `RETAINED = True`: publishers cache the last payload per concrete topic and answer peer `session.get()` queries; late subscribers automatically fetch cached values at subscribe time. Tombstones via `msg.unretain()` / `Cls.unretain(**fields)`; consume removals with `on_message(cb, on_remove=...)` or reconcile against `Cls.fetch_retained()`.
- **Presence / Last-Will-Testament.** Opt in with `LIVELINESS = True` + `msg.register_will()`: the session declares a Zenoh liveliness token, subscribers synthesise the will as a regular sample when the producer's session disappears (graceful close OR crash). The graceful-vs-crashed-shutdown distinction stops being your problem.
- **Queries / queryables (request/response).** `Cls.on_query(handler)` serves computed replies to peer `session.get()`; `Cls.query(...)` / `Cls.query_one(...)` return typed instances. The compute-serving generalization of `RETAINED`. Typed request payloads via `REQUEST`; async via `z.aon_query` / `z.aquery`.
- **Subscriber watchdog.** Optional per-subscription freshness detector — `Cls.on_message(cb, expected_interval=N, on_quiet=, on_active=)` fires callbacks when a subscription goes silent and again when it resumes. Optimistic by default (waits for first message); `startup_grace=N` opts into "tell me if I haven't heard within N seconds of subscribing."
- **Unified shutdown.** `z.release(session=sess)` walks every zeared-owned resource for the session in the right order — subscribers, queryables, publisher cache, retention queryable, presence observer, presence state, then `session.close()`. The graceful-shutdown path that used to take six separate calls is one.
- **Retention dedupe by default.** `RETAINED` classes auto-deduplicate retention-fetch replies against live publishes via `(key_expr, timestamp)`. Opt out per class with `DEDUPE = False`.
- **Replay-vs-live signal.** `meta.origin` tells a subscriber how each sample arrived — `z.Origin.LIVE` (a real publish), `z.Origin.REPLAY` (delivered from a retention cache at subscribe time or after reconnect), or `z.Origin.WILL` (presence-synthesised). Determined by the local delivery path, never read from the wire. Live-only subscriptions via `on_message(cb, retained_fetch=False)`; async-iterator access via `Cls.alisten(meta=True)`.
- **Sync + async.** Every send/subscribe has an `a`-prefixed sibling: `await msg.asend()`, `async for msg in Cls.alisten(): ...`, `async with z.abatch(): ...`. `on_message(cb)` detects `async def` callbacks and schedules them on the running loop.
- **Batching primitives.** `with z.batch(): ...` collects sends for atomic flush-or-discard on exception; `Cls.send_batch(items)` is the homogeneous-bulk shortcut. `asyncio` tasks get per-task isolation via `contextvars`.
- **Multi-session from day one.** Module-level default, thread-local scoped override via `with z.session(other): ...`, and per-call `session=` kwarg — all three resolve in a fixed precedence.
- **msgpack default, JSON opt-in.** `ENCODING = 'json'` per class for human-readable traffic, or `zeared.debug = True` globally to force JSON everywhere (handy during development).
- **One-import ergonomics.** `import zeared as z` re-exports every `seared` field type, the decorator (`@z.zeared` — renamed for flavour), and the error hierarchy alongside the zeared surface.
- **Transport-mode-agnostic.** Peer, router/client, whatever — you build the session; zeared takes it.
- **Declarative connection specs.** `z.SessionConfig` is a seared dataclass for the full connection spec; pass it via `config=` to `z.peer()` / `z.client()` / `z.open()`. Shareable, loggable, diffable.
- **Connect-with-retry built in.** `z.client('tcp/router:7447', retry=True, max_backoff=30)` wraps `zenoh.open()` in an exponential-backoff loop — the boilerplate every daemon writes, now one kwarg.
- **Topic introspection.** `Cls.published_topics()` and `z.published_topics()` surface the set of concrete keys emitted during the process lifetime — for dashboards, diagnostics, and audit tooling.

## Setup

```sh
# pip
pip install git+https://www.github.com/bwiswell/zeared.git

# uv
uv add git+https://www.github.com/bwiswell/zeared.git
```

Requires Python ≥ 3.14. Pulls in
[`seared`](https://www.github.com/bwiswell/seared), `eclipse-zenoh`, and
`msgpack`.

**Note:** if the consuming project uses `hatchling` as its build backend,
adding `zeared` as a direct git reference may require enabling
`allow-direct-references` in that project's `pyproject.toml`:

```toml
[tool.hatch.metadata]
allow-direct-references = true
```

## Quick start

```python
from typing import Optional

import zeared as z


@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    id:    int            = z.Int(required=True)
    x:     float          = z.Float(required=True)
    y:     float          = z.Float(required=True)
    label: str | None     = z.Str(default=None)


# Open a session and set it as the default.
z.session = z.peer()

# Subscribe before publishing.
sub = Telemetry.on_message(
    lambda msg: print(f'robot {msg.id}: ({msg.x}, {msg.y})')
)

# Publish.
Telemetry(id=1,  x=1.5, y=2.5).send()
Telemetry(id=42, x=0.0, y=0.0, label='home').send()

sub.close()
z.session.current.close()
```

`z.Int`, `z.Float`, `z.Str`, `z.Zeared`, `z.ValidationError`, etc. are
re-exports of the identically-named symbols in
[`seared`](https://www.github.com/bwiswell/seared); the sole rename is
`z.zeared` (which is `seared.seared` under the hood). If you prefer the
two-package style, `import seared as s; import zeared as z` still works
exactly as it did before — and `@s.seared` remains available.

## Multi-session: a node on two networks

The common "this node owns network A and talks to an external node on network
B" pattern is first-class. Three mechanisms resolve against a fixed precedence:

1. Explicit `session=` kwarg on the call.
2. Innermost `with z.session(sess):` block (thread-local stack).
3. Module-level `z.session = sess` default.

```python
internal = z.peer(listen=['tcp/0.0.0.0:7447'])
external = z.client('tcp/router.example.com:7447')

z.session = internal                            # default everywhere
Telemetry(id=1, x=0.0, y=0.0).send()             # → internal

with z.session(external):
    ExternalCommand.on_message(handler)          # → external
    ResponseMsg(ok=True).send()                  # → external

Telemetry(id=2, x=0.0, y=0.0).send()             # → internal again

# Or one-off overrides:
Telemetry(id=3, x=0.0, y=0.0).send(session=external)
```

## Async

Every sync primitive has an `a`-prefixed sibling. Zenoh's Python bindings
aren't natively async, so these wrap sync calls in `asyncio.to_thread`
(for publishes and session-opens) and bridge the Zenoh callback thread to
an `asyncio.Queue` (for `alisten`). Your event loop stays unblocked.

```python
import asyncio
import zeared as z


@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    id: int   = z.Int(required=True)
    x:  float = z.Float(required=True)


async def main():
    z.session = await z.apeer()              # async session factory

    async def producer():
        for i in range(100):
            await Telemetry(id=i, x=float(i)).asend()

    async def consumer():
        async for msg in Telemetry.alisten():
            print(msg.id, msg.x)
            if msg.id == 99:
                break                         # closes the subscriber

    # Or group into an atomic batch
    async with z.abatch():
        await Telemetry(id=42, x=1.0).asend()
        await Telemetry(id=43, x=2.0).asend()

    await asyncio.gather(producer(), consumer())


asyncio.run(main())
```

Pass an `async def` to `on_message` and zeared detects it — each incoming
sample schedules the coroutine on the loop via
`asyncio.run_coroutine_threadsafe`. This keeps the sync publish path fast
while letting the handler `await` downstream work.

`alisten(meta=True)` yields `(msg, meta)` tuples instead of bare
messages — the async-iterator path's access to `meta.captures`,
`meta.schema`, and `meta.origin` (the replay-vs-live signal):

```python
async for msg, meta in Telemetry.alisten(meta=True):
    if meta.origin is z.Origin.REPLAY:
        seed(msg)          # initial state sync
    else:
        react(msg)         # live change
```

Sync and async calls share state: `z.session`, `z.debug`, the publisher
cache, and the batch buffer (stored in a `ContextVar` so each `asyncio`
task gets its own). Mix freely.

## Retained messages

MQTT-style retention, emulated on Zenoh. Opt in with `RETAINED = True` on
the class; retained publishes update a per-`(class, session)` cache and a
Zenoh `Queryable` answers peer `session.get()` requests with cached values.
Late subscribers on `RETAINED` classes automatically fetch at subscribe
time — before live samples start flowing.

```python
@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    RETAINED = True
    id: int   = z.Int(required=True)
    x: float  = z.Float(required=True)
    y: float  = z.Float(required=True)

# Publish: retained by default on a RETAINED class.
Telemetry(id=1, x=1.0, y=2.0).send()

# Publish live-only — bypass the cache but still deliver to live subs.
Telemetry(id=1, x=3.0, y=4.0).send(retain=False)

# Drop the retained value and emit a DELETE sample ("tombstone").
Telemetry(id=1, x=0.0, y=0.0).unretain()    # instance form
Telemetry.unretain(id=1)                    # class form, key fields as kwargs

# Late subscriber automatically receives any cached values from peer
# queryables before live samples begin.
Telemetry.on_message(handler)
```

Rules:

- `send(retain=True)` on a `RETAINED = False` class raises `TopicError` — opt-in is explicit.
- `send(retain=False)` on a `RETAINED = True` class publishes live without touching the cache.
- `unretain()` requires `RETAINED = True`; the wire signal is a native Zenoh `DELETE` sample. `on_message`'s `cb` never sees DELETEs — register `on_message(cb, on_remove=...)` for the tombstone feed (below).
- Queryables are declared lazily on the first retained publish — a class that declares `RETAINED = True` but never sends wastes nothing.
- Retained fetches are deduplicated against the live stream by default — a retained reply whose wire payload matches the most recent live sample for the same concrete topic is suppressed. Set `DEDUPE = False` on the subscriber class to opt out.
- Retained-fetch deliveries are marked `meta.origin = z.Origin.REPLAY` (live samples are `LIVE`), so a 2-arg callback can seed state from replays without re-acting on them. The fetch drains before `on_message` returns — after that, everything is live or marked. A subscription that never wants replay passes `retained_fetch=False` (skips the fetch — and its blocking `session.get` — at subscribe time and on every reconnect).
- TTL via `RETENTION_TTL = N` (seconds) on the class, or `peer(retention_ttl=N)` for a session-wide fallback (`auto_reconnect=True` only). Class-level always wins; session-level fills in for unconfigured classes. Lazy expiration — entries are checked + pruned on the next subscriber retained-fetch.

### Removals: the tombstone feed and full-set reconcile

`cb` is an add/update feed — a DELETE tombstone (a peer's `unretain()`)
carries no payload to decode, so `cb` never fires for it. Two surfaces
close the removal gap:

```python
# 1. on_remove: low-latency incremental removals. Fires on each DELETE with
#    a ZenohMeta whose captures identify the removed key (raw strings).
def gone(meta: z.ZenohMeta) -> None:
    reader_id = Telemetry.coerce_captures(meta.captures)['id']   # typed
    desired.discard(reader_id)

Telemetry.on_message(add_or_update, on_remove=gone)

# 2. fetch_retained: a one-shot typed snapshot of the whole retained set —
#    the *reliable* reconcile path. A subscriber offline during a DELETE
#    misses the tombstone forever (the retained value is already gone, no
#    tombstone is replayed), so poll this periodically and reconcile against
#    the returned set.
current = Telemetry.fetch_retained()          # list[Telemetry]
reconcile(desired={m.id for m in current})
```

- `on_remove` is optional and off by default — DELETEs stay silently dropped unless you register one. `async def` on_remove is supported (scheduled like an async `cb`). It survives reconnect. A raised `on_remove` routes through `on_error` as `CallbackError`.
- `coerce_captures` runs raw string captures through the declared fields' deserializers (`{id}` bound to `z.Int` → `int`); capture-only slots pass through unchanged.
- `fetch_retained()` requires `RETAINED = True` and returns only successfully decoded results; dedupe duplicate keys by identity at the call site. Async: `await z.afetch_retained(Cls)`.
- **Why both:** `on_remove` is fast but not durable (a missed DELETE is missed forever); `fetch_retained` is durable but polled. Use the incremental feed for latency and the periodic snapshot for correctness — the same honesty boundary as presence (removals are subscriber-observed, never a broker guarantee).

## Queries and queryables

Request/response (Zenoh query/reply) for `@z.zeared` classes. If retention
is *"a queryable that serves a cached value"*, this is *"a queryable that
serves a **computed** value"*: the message class's `TOPIC` template is the
queryable key, its fields are the reply body, and a handler computes the
answer on demand.

```python
@z.zeared
class TagState(z.Message):
    TOPIC = 'rio/state/tag/{epc}'
    epc: str   = z.Str(required=True)   # key capture
    x: float   = z.Float(required=True) # ─┐ reply body
    y: float   = z.Float(required=True)  #─┘

# --- serving side ---
def handler(ctx: z.QueryContext) -> TagState | list[TagState] | None:
    epc = ctx.captures['epc']          # ctx.params / ctx.request also available
    x, y = lookup(epc)
    return TagState(epc=epc, x=x, y=y)  # or ctx.reply(...) explicitly

qbl = TagState.on_query(handler)        # z.Queryable — .close() / context manager

# --- getting side ---
one  = TagState.query_one(epc='E280 1234', timeout=2.0)  # first reply or None
many = TagState.query(epc='E280*', timeout=2.0)          # wildcard fan-out
all_ = TagState.query(timeout=2.0)                        # omitted slots → '*'
```

The handler either **returns** an instance / iterable / `None`, or replies
explicitly with `ctx.reply(...)` / `ctx.reply_err(...)` / `ctx.reply_del(...)`
for multi-reply, streaming, or error cases. `QueryContext` exposes the
request: `key_expr`, parsed selector `params`, template `captures`, and a
decoded `request` payload.

Rules:

- `query()` blocks up to `timeout` and returns only successfully decoded OK replies. Error replies (`reply_err`) and decode failures route to `on_error=` (else log), each surfaced as a `z.QueryError`.
- `query()` defaults to **no consolidation** — a query is a fan-out and each queryable may reply more than once; Zenoh's default consolidation would collapse same-key replies. Pass `consolidation=` to opt into dedup.
- Omitted key slots widen to `*` (whole-template wildcard); embed `*` in a value for a partial wildcard. `params=` appends `?k=v`; `REQUEST = SomeClass` + `request=` sends a typed request payload.
- `async def` handlers: register via `z.aon_query(Cls, handler)`; the query stays live until the coroutine resolves. Async getters: `z.aquery` / `z.aquery_one` / `z.aquery_iter` — generic in the message class since 0.3.5, so they return `list[Cls]` / `Cls | None` / `AsyncIterator[Cls]` and callers need no `cast`.
- **Streaming getters (0.3.4).** `Cls.iter_query(...)` and `z.aquery_iter(...)` yield each reply as it lands instead of collecting the window; `query` is `list()` over the same path. Pair with a generator handler for a query that is O(1) on both ends. `timeout` is the total duration and starts when the getter is *called*, not when iteration begins; `on_error` fires as you consume, so an iterator nobody iterates reports nothing.
- **`query_one` short-circuits (0.3.4).** It returns at the first decoded reply rather than waiting out `timeout`. Consequence: `on_error` sees only failures arriving before that first good reply — use `query()` for the complete error picture.
- **Generator handlers stream.** A `handler` that `yield`s replies each instance as it is produced, so serving a large result set never materialises it — `async def` generators included (0.3.2). A generator that raises part-way through routes to `on_error` and sends an error reply, so the getter knows the stream was truncated.
- `on_query` on a `RETAINED = True` class raises `TopicError` — retention already serves a queryable over the same topic; pick one.
- Queryables survive reconnect (managed sessions, `auto_reconnect=True`), are closed by `z.release()`, and can be dropped with `z.clear_queryable_cache()`.

## Wildcards and capture-only slots

Template slots fall into two categories:

- **Declared slots** — the slot name is also a declared seared field on the class. The captured value is coerced through the field's `deserialize` and populates the instance. Usable for publish (the class renders the template from the instance's values).
- **Capture-only slots** — the slot name isn't a declared field. On receive, the raw string capture lands on `meta.captures[name]`. Subscribe-side only — publish raises `TopicError` because the renderer has no value for the slot.

Use capture-only slots when the key carries routing-only identifiers that
shouldn't live on the payload schema. The canonical case is a correlation
ID for a request/response bridge:

```python
@z.zeared
class CliRequest(z.Message):
    TOPIC = 'workload/cli/request/{corr_id}'   # corr_id not on the payload
    cmd: str = z.Str(required=True)
    args: list = z.Str(many=True, default_factory=list)

def on_request(msg: CliRequest, meta: z.ZenohMeta):
    corr_id = meta.captures['corr_id']         # routing plumbing
    handle(msg.cmd, correlation=corr_id)
```

Anonymous trailing `**` matches one-or-more path segments (MQTT `#`
equivalent). Templates with anonymous `**` are subscribe-only — `send()`
on a non-publishable template raises `TopicError`. Useful as a catch-all
subscriber:

```python
@z.zeared
class AnyEvent(z.Message):
    TOPIC = 'peer/{name}/event'                # canonical (publishable)
    EXTRA_TOPICS = ('peer/**',)                # subscribe-only wildcard
    name:    str  = z.Str(required=True)
    payload: dict = z.Dict(default_factory=dict)
```

For variable-depth tails on **publishable** topics, use a named
multi-segment slot `{name**}` instead. Slashes pass through verbatim;
the slot is publish-and-subscribe capable:

```python
@z.zeared
class LogLine(z.Message):
    TOPIC = 'log/{service}/{path**}'           # publishable + variable depth
    service: str = z.Str(required=True)
    path:    str = z.Str(required=True)        # 'a/b/c/...' as one string
    line:    str = z.Str(required=True)

LogLine(service='api', path='2026/04/24/info', line='boot').send()
# wire key: log/api/2026/04/24/info
```

Rules for `{name**}`:

- Trailing only — must be the final path segment.
- At most one per template.
- Empty value (`path=''`) raises `TopicError` at render — Zenoh's `**`
  is one-or-more segments; an empty tail wouldn't match the wire form.
- Field binding (when declared as a slot on the class): must be
  `z.Str(many=False, keyed=False)`. Other types are rejected at class
  build time; non-string path tails would lose information through
  coercion.
- Capture-only `{name**}` works the same as `{name}` — undeclared slots
  land on `meta.captures[name]` as a single slash-containing string.

Named single-segment slots also work at any position:

```python
@z.zeared
class Status(z.Message):
    TOPIC = 'peer/{cluster}/{host}/status'
    cluster: str = z.Str(required=True)
    host:    str = z.Str(required=True)
    state:   str = z.Str(required=True)
```

`meta.captures` is always populated (at minimum an empty dict) on
subscribers that receive a 2-arg callback.

## Tagged-union payloads (`z.Union`)

For control topics that carry a `{action, args}`-style envelope with
heterogeneous handler shapes, declare variant classes and a `z.Union`
field:

```python
@z.zeared
class StartAction(z.Zeared):
    speed: float = z.Float(required=True)

@z.zeared
class StopAction(z.Zeared):
    pass


@z.zeared
class Control(z.Message):
    TOPIC = 'scenario/main/control'
    action: object = z.Union(
        variants={'start': StartAction, 'stop': StopAction},
        tag_key='action',
        payload_key='args',
        required=True,
    )


# Publish
Control(action=StartAction(speed=10.0)).send()

# Dispatch with pattern matching
def handler(msg: Control):
    match msg.action:
        case StartAction(speed=s): print('starting at', s)
        case StopAction():         print('stopping')

Control.on_message(handler)
```

Wire shape: `{"action": "start", "args": {"speed": 10.0}}`. Set
`payload_key=None` for a flat envelope (variant fields merged at the
top level alongside the tag). Unknown tags raise `ValidationError` on load;
a value whose type isn't one of the declared variants raises on dump.

## Presence / Last-Will-Testament

Opt in with `LIVELINESS = True`. Each session declares a single Zenoh
liveliness token; every `msg.register_will()` call stages a payload that
subscribers will synthesise when the peer's session disappears — graceful
close OR crash, same wire values either way.

```python
@z.zeared
class PeerStatus(z.Message):
    TOPIC = 'peer/{name}/status'
    RETAINED = True
    LIVELINESS = True                 # opt in
    name:   str = z.Str(required=True)
    state:  str = z.Str(required=True)
    detail: str = z.Str(default='')


# Producer — at startup:
PeerStatus(name='alice', state='online', detail='ready').send()
PeerStatus(name='alice', state='offline', detail='lost liveliness').register_will()

# A peer daemon with multiple retained topic families — one session,
# N wills, one liveliness token covering everything. All fan out on death.
PeerStatus(name='alice', state='offline').register_will()
PeerRegistry(name='alice').register_will()
SharedLlmRegistry(peer='alice').register_will()


# Subscriber — receives the will as a synthesised sample, identical shape
# to a real publish (a 2-arg callback sees meta.origin = z.Origin.WILL).
# The caller's callback fires either way.
def handler(msg: PeerStatus):
    print(msg.name, msg.state, msg.detail)

PeerStatus.on_message(handler)
# When alice's session dies: handler(PeerStatus(name='alice', state='offline', ...))
```

**Important caveat** — zeared's presence is **honest about the transport**:
Zenoh has no broker, so "the will fires" happens *in each subscriber* by
synthesising the stashed payload when the peer's liveliness token
disappears. **Non-subscribers never observe the offline signal.** If you're
coming from MQTT expecting "an out-of-band log archive catches the will,"
that pattern doesn't apply here — anything that needs to see the offline
event has to subscribe.

**Graceful close also fires the will.** Zenoh liveliness drops on both
`session.close()` and crash; zeared doesn't distinguish. If you want
different wire values for graceful vs crashed shutdown, publish explicitly
before closing (standard pattern).

**Retention and liveliness are orthogonal.** Registering a will does NOT
touch the retention cache. A late subscriber joining after a peer's
graceful shutdown still sees the last retained value (useful for diagnostic
tooling that wants the most recent real state). Auto-transitioning retained
state to the will would race explicit graceful shutdowns — your code still
owns the publish-before-exit pattern.

Async counterpart: `await msg.aregister_will()`.

## Subscriber watchdog

Per-subscription freshness detection. Pass `expected_interval=N` (seconds)
to `on_message`; `on_quiet` fires the first time the gap between messages
exceeds the interval, and `on_active` fires on the next message after a
quiet period.

```python
def on_quiet():
    log.warning('telemetry stream went silent')

def on_active():
    log.info('telemetry stream resumed')

Telemetry.on_message(
    handler,
    expected_interval=10,        # seconds
    on_quiet=on_quiet,
    on_active=on_active,
)
```

**Optimistic by default:** the watchdog waits for the first message to
establish a cadence; a subscription that never receives anything never
fires `on_quiet`. For a "tell me if I haven't heard anything within N
seconds of subscribing" signal, pass `startup_grace=N`:

```python
# Fires on_quiet if no message arrives within 30 seconds of subscribing,
# then uses expected_interval for cadence checks once messages start.
Telemetry.on_message(
    handler,
    expected_interval=10,
    startup_grace=30,
    on_quiet=on_quiet,
)
```

`on_quiet` and `on_active` accept `async def` callbacks. They fire on a
dedicated watchdog thread — not the Zenoh delivery thread — so code that
mutates shared state needs to handle that.

Only **live** samples feed the watchdog (0.3.0): a retained replay
can't establish cadence from stale cached data, and a synthesised will
— the producer died — can't reset the quiet timer. A `startup_grace`
subscriber whose only traffic was the subscribe-time replay correctly
fires `on_quiet` when no live sample follows.

When the watchdog is constructed without a running event loop (the
common case — `Cls.on_message` called from sync code), async callbacks
dispatch via a fresh `asyncio.run()` per fire. Correct but ~2–5 ms of
loop spin-up overhead each time. Fine for typical watchdog cadences
(tens of seconds); prefer sync callbacks if you're firing more often
than that, or construct the watchdog from inside an async context so
the loop is captured once. See `docs/watchdog.md`.

## Unified shutdown

`z.release(*, session=sess)` walks every zeared-owned resource for the
session in the right order and closes it. Replaces the six-step manual
shutdown dance with one call:

```python
session = z.peer(...)
z.session = session
# ... use session normally ...

# Graceful shutdown:
z.release(session=session)
```

For most daemons, the context-manager form is more ergonomic — no
explicit `release` call needed, and exceptions still trigger cleanup:

```python
with z.peer(auto_reconnect=True) as sess:
    z.session = sess
    Telemetry(id=1, x=1.0, y=2.0).send()
# z.release(session=sess) ran on block exit.

# Async sibling:
async with z.apeer(auto_reconnect=True) as sess:
    await Telemetry(id=1, x=1.0, y=2.0).asend(session=sess)
```

What it does, in order: closes every zeared subscriber on the session
(cancelling watchdogs along the way), drops the publisher cache, drops
the retention cache (undeclares queryables), stops the presence observer,
clears presence state (which fires the liveliness DELETE that triggers
peers' will synthesis), then calls `session.close()`. The order matters
— step 5 must happen before step 6 so the DELETE escapes the transport
before it tears down. Idempotent on a second call.

The `session=` kwarg is keyword-only and required: `z.release()` with no
arguments raises `TypeError`. Explicit-always — no implicit module-default
fallback.

For process-shutdown hooks where you don't track every session you've
opened, `z.release_all()` walks every per-session registry and tears
down each unique session in turn. Idempotent on a second call. As of
0.0.17, the walk also covers `ManagedSession` wrappers that have no
zeared-level state registered against them (a session opened with
`auto_reconnect=True` but never subscribed/published) — their probe +
reconnect threads were previously invisible to `release_all`.

```python
import atexit
atexit.register(z.release_all)
```

## Wire format and binary fields

When a class declares `ENCODING = 'msgpack'` (or relies on the default
when `z.debug = False`), zeared threads `format='msgpack'` into seared's
`dump` / `load` so binary fields (`Bytes`, `NDArray`) emit native bytes
on the wire — saving the ~33% base64 inflation that JSON-safe encoding
imposes.

```python
@z.zeared
class Frame(z.Message):
    TOPIC = 'sensor/{id}/raw'
    ENCODING = 'msgpack'                # default
    id:    int   = z.Int(required=True)
    bytes_: bytes = z.Bytes(required=True)

# Sample's wire payload is msgpack with native bytes — no base64.
Frame(id=1, bytes_=b'\x00\x01\x02').send()
```

Set `ENCODING = 'json'` (or toggle `z.debug = True` globally) to keep
JSON-safe wire shapes for fields that need them. Both directions
round-trip cleanly.

## Seared field types in zeared messages

zeared inherits seared's field types — every field on a seared class
works in a `z.Message`:

```python
from decimal import Decimal as D
from pathlib import Path as P
import pandas as pd

@z.zeared
class Report(z.Message):
    TOPIC = 'report/{id}'
    id:        int    = z.Int(required=True)
    amount:    D      = z.Decimal(required=True)              # 0.1.9+
    location:  P      = z.Path(required=True)                 # 0.1.9+
    rows:      pd.DataFrame = z.PandasFrame(required=True)    # 0.1.10+ (extra)
```

`Decimal` round-trips losslessly via string; `Path` normalises to POSIX
on the wire; `PandasFrame` / `PolarsFrame` use the records form
(`[{col: val}, ...]`). `Union(default=Variant)` from seared 0.1.9
gives graceful fallback for unknown tags. See seared's docs.

## Multiple topics per class

```python
@z.zeared
class Status(z.Message):
    TOPIC = 'robot/{id}/status'
    EXTRA_TOPICS = ('vehicle/{id}/status',)
    id:     int = z.Int(required=True)
    status: str = z.Str(required=True)

# Publish defaults to TOPIC (canonical)
Status(id=1, status='ok').send()                                   # → robot/1/status
# Restrictive override — must match one of the declared templates
Status(id=1, status='ok').send(topic='vehicle/{id}/status')        # → vehicle/1/status

# Subscribe matches all declared templates
Status.on_message(lambda m, meta: print(meta.key_expr, m.status))
# prints both robot/1/status and vehicle/1/status as they arrive
```

Each declared template has an **independent slot set** — `TOPIC` can capture
`{id}`, an entry in `EXTRA_TOPICS` can capture `{name}` (or have no slots,
or use `**`), and they don't have to agree. Captures from any matched
template land on `meta.captures` regardless of name; declared seared fields
that share a name with a slot also get coerced onto the instance.
Arbitrary strings passed to `send(topic=...)` raise `TopicError`.

## Subscriber callbacks

`on_message(cb)` inspects `cb`'s arity once at subscribe time:

- `cb(msg)` — decoded message only.
- `cb(msg, meta)` — `meta` is a `ZenohMeta` seared dataclass carrying the resolved `key_expr`, `timestamp`, wire `encoding`, `source_info`, `origin_zid` (the publishing session's zid, from the HLC — attribution only, never authorization), optional `attachment`, and `origin` (the replay-vs-live signal: `z.Origin.LIVE` / `REPLAY` / `WILL`). No Zenoh types leak into user code.

```python
def on_telemetry(msg: Telemetry, meta: z.ZenohMeta) -> None:
    print(meta.timestamp, msg.id, msg.x, msg.y)

Telemetry.on_message(on_telemetry)
```

## Publisher caching and batching

Every message class sets `PUBLISHER: ClassVar[bool | int] = True` by default.
On the first `msg.send()` against a given session, zeared declares a
`zenoh.Publisher` for the concrete topic and caches it; subsequent sends
on the same key reuse the publisher. The cache is soft-capped at 256
concrete keys per `(class, session)`:

```python
@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    PUBLISHER = True          # default (cap 256)
    # PUBLISHER = 1024        # explicit cap
    # PUBLISHER = False       # disable caching; session.put() per send
    ...
```

Overflowing the cap falls back to `session.put()` and emits a one-time
`warnings.warn`; set `PUBLISHER = False` or raise the cap if you expect
wildly diverse keys.

`with z.batch()` groups sends into an atomic flush — useful when you want
to send N messages together or not at all (exceptions discard the whole
group). `Cls.send_batch(items)` is the homogeneous-bulk shortcut.

```python
with z.batch() as b:
    Telemetry(id=1, x=1.0, y=2.0).send()
    Alert(msg='done').send()
    # b.flush()               # optional: drain pending sends now

Telemetry.send_batch(
    [Telemetry(id=i, x=float(i), y=0.0) for i in range(1000)],
    session=external,
)
```

## Wire format

msgpack by default — compact, binary, declared to Zenoh as
`application/msgpack`. Opt into JSON per class:

```python
@z.zeared
class Event(z.Message):
    TOPIC = 'events/raw'
    ENCODING = 'json'
    payload: str = z.Str(required=True)
```

Or flip the global debug flag to force JSON for every message class, without
editing any of them:

```python
z.debug = True      # every msg.send() and subscriber now uses JSON
```

The subscriber side honours whatever encoding the sender declared on the Zenoh
sample, so mixed fleets interoperate.

## Session factories

```python
z.peer()                                      # discovery via multicast
z.peer(connect=['tcp/peer.example.com:7447'])
z.peer(listen=['tcp/0.0.0.0:7447'])
z.client('tcp/router.example.com:7447')
z.client(['tcp/a:7447', 'tcp/b:7447'])

# Connect-with-retry (exponential backoff) — the daemon boilerplate,
# compressed into one kwarg.
z.client(
    'tcp/router.example.com:7447',
    retry=True,
    initial_backoff=0.1, max_backoff=30.0,
    max_attempts=None,                        # forever
)
```

Async siblings (`z.apeer`, `z.aclient`) take the same kwargs.

For declarative, shareable connection specs, use `z.SessionConfig`:

```python
cfg = z.SessionConfig(
    mode=z.Mode.CLIENT,
    router='tcp/router.example.com:7447',
    retry=True, max_backoff=30.0,
)

sess = z.open(cfg)              # unified entry, dispatches on cfg.mode
# or: sess = z.client(config=cfg)
```

When both `config=` and explicit retry kwargs are passed, the kwargs win
(per-call overrides over a shared base spec). For raw Zenoh config
overrides on top of either form, pass `zenoh_config=<zenoh.Config>` (the
kwarg was renamed from `config=` in 0.0.6).

`SessionConfig` ships with immutable-style builders for composable
tweaks — each returns a new instance:

```python
cfg = (
    z.SessionConfig(mode=z.Mode.CLIENT)
    .set_router('tcp/router.example.com:7447')
    .with_retry(max_backoff=30)
    .with_connect('tcp/backup:7447')
)

# Or tweak an existing config
hot_cfg = prod_cfg.replace(max_backoff=60, max_attempts=5)
```

Builder set: `.replace(**kwargs)`, `.with_retry(retry=True, **knobs)`,
`.with_connect(*endpoints)` / `.with_listen(*endpoints)` (append),
`.set_router(router)` (replace). The `with_*` / `set_*` split mirrors
the field shape — list fields append, scalar fields replace.

## NAT traversal: the relay hub

Two nodes that can't reach each other directly — e.g. both behind NAT,
outbound-only — need a relay in the middle. `z.hub()` opens a **router-mode**
session that does exactly that, in-process: it relays pub/sub, queries, and
liveliness (so retention/queryables **and** presence/LWT all work through it).
The routing runs in Zenoh's Rust core, so throughput matches a standalone
router — and there's **no `zenohd` binary to install**.

```python
# On a publicly-reachable host — bind and relay.
hub = z.hub(listen=['tcp/0.0.0.0:7447'])

# Each NAT-gated node connects OUT to the hub as a client. Client mode routes
# everything through the hub and never attempts the direct peer links that
# would fail under NAT.
sess = z.client(router='tcp/hub.example.com:7447', auto_reconnect=True)
```

Run the hub as a standalone daemon (deploy it as a systemd service):

```bash
python -m zeared.hubd --listen tcp/0.0.0.0:7447
```

```bash
python -m zeared.hubd \
  --listen tcp/0.0.0.0:7447 \
  --connect tcp/hub-b.example.com:7447 \   # link to another hub (multi-hub mesh)
  --config /etc/zeared/hub.json5           # TLS / access-control config
```

The daemon is stateless and holds no zeared message state — it routes opaque
samples, so it needs no schemas or `rio-protocol`. It stops cleanly on
SIGINT/SIGTERM.

Notes and limits:

- **Secure a public hub.** An open relay is reachable by anyone who can hit
  the port. Configure Zenoh **TLS/mTLS + access-control** via `--config`
  (a JSON5 Zenoh config) or `z.hub(zenoh_config=...)`. `--listen`/`--connect`
  layer on top of a `--config` file.
- **Single hub is a SPOF.** For HA, run multiple hubs and link them
  (`--connect` / `z.hub(connect=[...])`); nodes can list several routers.
- **Hub returns a raw `zenoh.Session`** — a listener has no zeared-owned
  resources to supervise, so there's no `ManagedSession` wrapper. The
  *client* side gets `auto_reconnect=True` as usual.
- **`z.client(router=...)` already targets any router** — including a
  standalone `zenohd` if you'd rather run one. The hub just means you don't
  have to.

### Auto-reconnect

Long-running daemons that need to survive a session dying mid-flight
opt in to auto-reconnect:

```python
sess = z.peer(
    auto_reconnect=True,            # opt in
    probe_interval=10.0,            # active liveness probe (seconds)
    retry=True,                     # backoff knobs reused for reconnect
    initial_backoff=1.0, max_backoff=30.0,
)

# `sess` is now a `ManagedSession` — same surface as `zenoh.Session`,
# plus a `raw()` escape hatch and a `state` property.

z.session = sess                    # works as the module default
Telemetry(id=1, x=1.0, y=2.0).send()  # delegates to the current raw

raw = sess.raw()                    # for handing to non-zeared code;
                                    # don't cache across reconnect
```

Detection: an active probe (`is_closed()` / `zid()` poll) catches dead
sessions on subscriber-only daemons; send-failure detection catches the
gap between probe ticks for publisher-heavy paths.

Restoration: cached publishers are invalidated (the next `send()`
re-declares against the new raw, so nothing is dropped on the far side of
a reconnect), subscribers and queryables are re-declared against the new
raw, retained fetches replay (dedupe-safe), and presence wills re-register
under the new zid (peers see legitimate offline → online — Zenoh's
per-session zid changes on each `open()`, not a bug).

During the reconnect window, `send()` / `get()` / `delete()` raise
`SessionDeadError`. Handle, retry, drop, or queue at the call site.

Per-subscriber opt-out: `Cls.on_message(cb, auto_reconnect=False)` keeps
that subscription bound to the original raw — useful when application
state must survive the boundary explicitly.

`probe_interval=0` (or `None`) disables the active probe; only
send-failure detection runs.

Daemons that need to react to a reconnect — log it, refresh
caller-side caches, re-publish authoritative state — register a
callback:

```python
def refresh_caches(s):
    my_app.peer_registry.invalidate()
    my_app.metrics.record_reconnect()

handle = sess.on_reconnect(refresh_caches)
# handle.cancel() to deregister; idempotent.
```

Multiple callbacks fire in registration order. `async def` callbacks
work too (registered from inside an event loop). Exceptions log + continue.

See [`docs/_managed_session/_managed_session.md`](docs/_managed_session/_managed_session.md) for the full lifecycle + state machine.

## Introspection

`Cls.published_topics()` returns a `frozenset[str]` of every concrete topic
that class has emitted during the process lifetime — including topics that
went through the `PUBLISHER = False` fallback and topics since tombstoned
via `unretain()`. Useful for dashboards and audit tooling.

```python
Telemetry.published_topics()
# frozenset({'robot/1/telemetry', 'robot/42/telemetry', ...})

Telemetry.published_topics(session=external)    # scope to one session

# Cross-class, for dashboards:
z.published_topics()
# {(Telemetry, <sid>): frozenset({...}), (Alert, <sid>): frozenset({...}), ...}
z.published_topics(cls=Telemetry)
z.published_topics(session=external)
```

Zero overhead for non-users: tracking is a single `set.add` per send.

## TypeScript types for browser consumers

`zeared.doc` generates TypeScript types for your message classes, so a browser
app consuming mesh traffic over a websocket gets a typed view of the wire.
**Types only** — the mesh→websocket bridge (auth, identity scope, transport)
is yours to build; the generated types describe the JSON shape a bridge hands
the browser after decoding a zeared message.

```sh
python -m zeared.doc.typescript rio_protocol -o rio-types.ts
python -m zeared.doc.typescript rio_protocol -o rio-types.ts --check   # CI guard
```

```ts
// Generated: one interface per message + referenced payloads, enums as unions.
export interface RawRead {
  source: string;
  reader_id: number;
  beam_id?: number;        // optional (has a default)
  epc: string;
  tid?: string | null;     // nullable (declared `str | None`)
  rssi: number;
  // …
}
```

Mapping: scalars → `number`/`string`/`boolean`; `Enum` → literal unions
(`0 | 1`, `"fast" | "slow"`); `many` → `T[]`; `keyed` → `Record<string, T>`;
`z.T(Nested)` → an interface reference; `z.Union` → a discriminated union;
`z.Tuple` → a TS tuple. Keys are **wire keys** (`data_key` when set). `required`
→ no `?`; a default → `?`; `X | None` → `?: T | null`. `dump=False` fields are
omitted. `Bytes` is base64 (`string`) on the JSON wire; frames/ndarrays are
`unknown` — decode them in your bridge. Also available programmatically:
`from zeared.doc import generate_ts; generate_ts('rio_protocol')` returns the
`.ts` source as a string.

## Errors

```
ZearedError
├── NoSessionError
├── TopicError
├── SessionDeadError
├── SubscriptionError              # declare-time, fatal to one subscription
└── SubscriberError                # dispatch-time, routed to on_error
    ├── DecodeError
    │   └── SchemaMismatchError
    ├── CallbackError
    └── RetainedFetchError
```

| Exception | Raised when |
|-----------|-------------|
| `z.NoSessionError`      | No session is resolvable (no kwarg, no scope, no default) |
| `z.TopicError`          | `TOPIC` is missing, malformed, references undeclared fields, or violates the namespace reservation (`__zeared/...`) |
| `z.SessionDeadError`    | A `ManagedSession` op (`put`/`get`/`delete`/`zid`/...) is invoked while the session is `RECONNECTING` or `DEAD` |
| `z.SubscriptionError`   | `Subscriber._declare` — declaring the underlying Zenoh subscriber failed (fatal to that subscription) |
| `z.SubscriberError`     | Parent for dispatch-time errors. Routed to `on_error=`; subscription continues |
| `z.DecodeError`         | Sample's payload failed to `codec.unpack` / `Message.load`. Subclass of `SubscriberError` |
| `z.SchemaMismatchError` | Wire-attached `schema` ≠ class's `SCHEMA`. Subclass of `DecodeError` |
| `z.CallbackError`       | User's `cb(msg)` itself raised. Subclass of `SubscriberError` |
| `z.RetainedFetchError`  | A retained-fetch reply failed to dispatch. Subclass of `SubscriberError` |
| `z.QueryableError`      | `on_query` declare failed, or on a `RETAINED` class (fatal to that queryable) |
| `z.QueryError`          | A `query()` reply was an error or failed to decode. Routed to `on_error=`; the query keeps collecting. Subclass of `SubscriberError` |

All extend `z.ZearedError` (which extends `Exception`). Dispatch-time
exceptions are routed to the `on_error=cb(exc, raw_bytes)` callback if
registered (else logged); they don't propagate out and don't stop the
subscription. The wrapped subclass provides typing for `isinstance`-based
discrimination; the original exception is reachable via `__cause__`.

See `docs/errors.md` for the full hierarchy.

## Benchmarks

End-to-end publish → subscriber-received throughput on a single
in-process Zenoh peer session. Headline numbers, msgpack-cached
default vs. `marshmallow` + Zenoh on the same schema:

| Strategy                          | pub/s | wire (B) |
|-----------------------------------|------:|---------:|
| Zenoh + `marshmallow` (JSON)      | 4,057 | 796      |
| `zeared` sync (msgpack, cached)   | 7,670 | 533      |

zeared is ~89% faster and ~33% smaller on the wire.

**"Pydantic + MQTT, or zeared?"** — the comparison people actually want, and
one that splits into two independent questions. pydantic's compiled core
beats pure-Python seared on serialization (~3.6x); Zenoh beats a
broker-mediated MQTT hop on transport (~2.5x). Netted out at default
configuration against MQTT QoS 0, **pydantic + MQTT comes out ~1.2x ahead** —
the codec gap is the larger of the two, and pure Python doesn't make it back.

Two things change that. Against **QoS 1** — at-least-once, what most
production fleets actually run, and the closer analogue to Zenoh's reliable
`put` — zeared is ~2.3x ahead. And with the optional `rusted` accelerator the
codec gap closes and the ordering flips: ~1.6x against QoS 0, ~4.4x against
QoS 1.

The full matrix (both axes isolated, sync + async variants, JSON / msgpack,
cached / uncached), methodology, run-to-run variance, and caveats live in
[`docs/overview/benchmarks.md`](docs/overview/benchmarks.md). Run them
yourself with `uv run python -m bench`.

## Schema and timestamps

Two convenient pieces of per-message metadata, both opt-in / automatic
where reasonable.

```python
@z.zeared
class PeerStatus(z.Message):
    TOPIC = 'peer/{name}/status'
    SCHEMA = '1.0'                  # opt in to schema stamping
    name:  str = z.Str(required=True)
    state: str = z.Str(required=True)

def on_status(msg: PeerStatus, meta: z.ZenohMeta):
    print(f'{msg.name} → {msg.state}')
    print(f'  schema:    {meta.schema}')      # → '1.0'
    print(f'  issued_at: {meta.issued_at}')   # → datetime in UTC
```

`SCHEMA` rides as a Zenoh sample attachment (msgpack-encoded
`{schema: '1.0'}`). Subscribers with their own `SCHEMA` set compare
against the wire value automatically — mismatches drop the sample and
route via `on_error` as `SchemaMismatchError`. Each
`(sender_zid, observed_schema)` mismatch warns once per subscriber, so
a misaligned peer doesn't spam the log on every message.

`meta.issued_at` is a `datetime.datetime` (UTC) parsed from Zenoh's HLC
timestamp on the sample. Requires timestamping (auto-enabled by
`z.peer()` / `z.client()` since 0.0.13). `None` when unavailable
(synthesised wills, sessions opened without timestamping).

### `SCHEMA` versioning conventions

zeared treats `SCHEMA` as an opaque string — equality is the only
operation. Pick a convention that fits your team's release cadence:

| Convention | Example | When |
|------------|---------|------|
| **Semver** | `'1.0'`, `'2.3.1'` | Stable wire schemas with intentional breaking changes; matches existing release-versioning vocabulary |
| **Hash of fields** | `'a3f72b1'` (first 7 of `sha1(repr(__seared_fields__))`) | Auto-derived; any field change cascades through; useful when you want "any field shape change" to be a schema event |
| **Build/commit ID** | `'2026-04-24.7c3d'` | Per-release rolling versions; useful when message classes are co-deployed with the daemon and you want "is this peer running the same build I am?" |
| **Date-coded** | `'2026.04'` | Calendar-versioned releases; intent: "this schema valid for samples published in this period" |

Whatever you pick, document it in your project's README and stick with
it — drift across daemons (one peer using semver, another using build
IDs) is the same failure mode as any version-string mismatch and will
surface as `SchemaMismatchError`. zeared doesn't enforce a shape; it
just compares strings.

## Structured subscriber errors

`on_error` callbacks receive typed exceptions so handlers can branch:

```python
def on_err(exc: z.SubscriberError, raw: bytes):
    if isinstance(exc, z.SchemaMismatchError):
        log_drift(exc)
    elif isinstance(exc, z.DecodeError):
        log_corrupt_payload(exc, raw)
    elif isinstance(exc, z.CallbackError):
        # user code blew up — surface to the caller
        raise exc.__cause__ from exc

PeerStatus.on_message(handle, on_error=on_err)
```

Hierarchy: `SubscriberError` → `DecodeError` (→ `SchemaMismatchError`)
/ `CallbackError` / `RetainedFetchError`. Each wraps the original
exception via `__cause__`. Catch generically (`except Exception` or
`except SubscriberError`) for unchanged behaviour; use `isinstance`
to discriminate.

## Per-subscriber dedupe override

Class-level `DEDUPE = True` (default for `RETAINED` classes) suppresses
retention-fetch replays that match a live sample's timestamp. A
specific subscriber can opt out:

```python
audit_sub = Telemetry.on_message(audit_cb, dedupe=False)  # see every replay
prod_sub  = Telemetry.on_message(prod_cb)                 # class default (dedupe on)
```

## Limits (v0.3.0)

- **Tombstone removals are not durably delivered.** `on_remove` fires on a *live* DELETE only; a subscriber offline during the `unretain()` never sees it (the retained value is already gone, no tombstone is replayed). For a durable removal guarantee, poll `Cls.fetch_retained()` and reconcile against the returned set — `on_remove` is the latency path, `fetch_retained` the correctness path.
- **`on_remove` delivers identity, not the removed value.** A tombstone has no payload, so `on_remove` gets a `ZenohMeta` (captures identify the key) — never a typed instance. Reconcile keyed on identity; if you need the departed entity's last data, capture it from the add/update `cb`.
- **Queryables are compute-serving, not cache-serving.** `Cls.on_query` computes each reply on demand; it does not persist state. A `RETAINED` class already serves a cache-backed queryable over the same topic, so the two are mutually exclusive on one class (`on_query` raises `TopicError` on a `RETAINED` class).
- **Breaking out of a streaming getter doesn't stop the server.** `Cls.iter_query` / `z.aquery_iter` stop *your* consumption; the serving queryable runs to completion regardless. Zenoh's cancellation is client-side only, so there is no cancel knob to offer. Unlike `alisten`, whose cleanup undeclares a real subscriber.
- **Async `on_query` holds the query until the coroutine resolves.** A slow async handler pins the underlying `zenoh.Query` for its lifetime. Correct, but a long-running handler ties up the query.
- **Query parameters are a `str → str` dict.** `params=` maps to the Zenoh selector (`?k=v`); typed parameters are deferred. For a rich, typed request use `REQUEST = SomeClass` + `request=` (sent as the query payload) instead.
- **Async is a wrapper, not native.** Zenoh's Python bindings are sync-only; `asend` / `alisten` / `aunretain` wrap via `asyncio.to_thread` and a callback-to-queue bridge. Correct, but not zero-overhead — see Benchmarks.
- **Retention dedupe is timestamp-dependent.** Dedupe needs HLC timestamps on samples; without them it's a no-op pass-through. Zeared's factory injects `timestamping/enabled=true` into the built `zenoh.Config` by default (0.0.13), so most callers don't need to think about this — opt out via `z.peer(timestamping=False)` or by managing your own `zenoh_config=`. If you supply `zenoh_config=`, you're in charge: enable timestamping yourself or set `DEDUPE = False` on the subscriber class. Synthesised wills (timestamp=`None`) always pass through dedupe.
- **Retention TTL is lazy.** `RETENTION_TTL = N` opts into time-based expiration of cached entries, but expiration is only checked when a subscriber issues a retained-fetch (`_handle_query`) — topics that nobody ever queries may keep stale entries in memory. Acceptable for typical use; no background sweeper.
- **Retry only on `zenoh.open()` raise (without auto-reconnect).** Vanilla `z.peer()` / `z.client()` retry only on the initial open. With `auto_reconnect=True`, mid-flight session deaths are detected and rebuilt; without it, a session dying silently means the next send fails.
- **`{name**}` and anonymous `**` are trailing-only.** Non-trailing multi-segment forms (`a/**/c`, `peer/{cluster**}/{host}/status`) remain backlog items.
- **Format specs rejected in templates.** `{x:03d}` / `{x!r}` / `{x**:fmt}` raise `TopicError`. Plain `{name}` and `{name**}` are the only slot syntaxes.
- **Publish-side `topic=` is restrictive.** The override must match one of the declared templates (`TOPIC` or an entry in `EXTRA_TOPICS`).
- **One `z.Union` field per class.** Multiple would collide on top-level wire keys.
- **No automatic session-close detection on caches without auto-reconnect.** Zenoh sessions don't support weakrefs; cached publishers and retention queryables are dropped on the next failing send / `clear_*_cache(session=sess)` call when running raw, and lazy-rebuilt after reconnect when running managed.
- **Presence is subscriber-synthesised.** Non-subscribers never observe the offline event. Graceful close fires the will the same as a crash — if you need distinct wire values, publish explicitly before close.
- **`origin_zid` is attribution, not authentication.** The zid comes from the sample's HLC, and `put(timestamp=...)` lets a publisher claim any value. Enforce origin at the router (Zenoh access control on a TLS certificate), not from this field.
- **Dedupe tolerates a bounded clock skew.** A sample whose HLC is more than `DEDUPE_MAX_SKEW` seconds (default 300) ahead of local time is delivered but does not advance the dedupe watermark — otherwise one far-future timestamp blinds the subscriber to that key until restart. Set `None` to disable.
- **`send()` raises during the reconnect window.** With `auto_reconnect=True`, ops on a session in `RECONNECTING` or `DEAD` state raise `SessionDeadError`. No buffering / blocking — callers that want either build it on top.
- **Raw `threading.Thread` doesn't propagate batch context.** `with z.batch():` + raw thread spawn means the worker bypasses the batch. Use `asyncio.to_thread()` or `contextvars.copy_context().run()` for thread-spawned workers. See `docs/batch.md`.

## Development

```sh
uv sync
uv run pytest tests/
```

Four checks gate every commit, wired as `prek` hooks — run
`uv run prek install` once per clone:

```sh
uv run ruff check          # lint
uv run ruff format         # format
uv run deptry .            # dependency hygiene
uv run ty check            # type check
```

The benchmarks and their comparator libraries are repo-only, behind an
opt-in extra:

```sh
uv sync --extra bench
uv run python -m bench
```

Tests mirror source layout exactly — one `test_*.py` per source file.
Integration tests spin up real Zenoh peer sessions in-process; they cover the
full send → subscriber → decode path, two-session isolation, scoped overrides,
the debug flag, and error handling.
