# zeared

`zeared` is a typed [Zenoh](https://zenoh.io/) pub/sub wrapper built on top of
[`seared`](https://github.com/bwiswell/seared). Declare a message class
once, get publish, subscribe, and topic routing for free.

> Looking for the serialization library underneath? See its sister package
> [`seared`](https://github.com/bwiswell/seared). Its entire public
> surface is re-exported from `zeared`, so `import zeared as z` is enough
> for most use cases.

## Why zeared

- **Typed messages.** Messages are ordinary `@z.zeared` dataclasses with a `TOPIC` class attribute. `send()` publishes; `on_message(cb)` subscribes with a typed callback.
- **Format-string topics.** `TOPIC = 'robot/{id}/telemetry'` auto-derives the Zenoh wildcard `robot/*/telemetry` for subscribers and populates `msg.id` from the actual key expression on receive.
- **Multi-topic classes.** Declare `EXTRA_TOPICS = (...)` to subscribe to several wildcards with one class; a restrictive `send(topic=...)` override lets you publish to any declared template.
- **Multi-segment wildcards.** Trailing `**` in a template matches one or more trailing path segments (the MQTT `#` counterpart). An anonymous `**` makes that template subscribe-only; a named `{name**}` slot is publishable.
- **Capture-only slots.** Template slots that aren't declared as seared fields become routing-only captures — always surfaced via `meta.captures: dict[str, str]`, never on the payload schema.
- **Tagged-union payloads.** `s.Union` encodes `{action, args}`-style envelopes and decodes to a typed variant instance — pattern-match on `msg.action` at the dispatch site.
- **Long-lived publishers by default.** Every message class transparently caches `zenoh.Publisher` instances per concrete topic (soft-capped, user-overridable). No `Publisher` ceremony — `msg.send()` is the one path.
- **Retained messages (MQTT-style).** Opt in with `RETAINED = True`: publishers cache the last payload per concrete topic and answer peer `session.get()` queries; late subscribers automatically fetch cached values at subscribe time. Tombstones via `msg.unretain()` / `Cls.unretain(**fields)`; consume removals with `on_message(cb, on_remove=...)` or reconcile against `Cls.fetch_retained()`.
- **Presence.** Opt in with `LIVELINESS = True` + `declare_presence()`: a Zenoh liveliness token at the contract key, which subscribers see on `on_online` / `on_offline`. A producer's death, graceful or a crash, reads as offline; a proxy can withdraw one key while it lives on (0.4.0).
- **Queries / queryables (request/response).** `Cls.on_query(handler)` serves computed replies to peer `session.get()`; `Cls.query(...)` / `Cls.query_one(...)` return typed instances. The compute-serving generalization of `RETAINED`. Typed request payloads via `REQUEST`; async via `z.aon_query` / `z.aquery`.
- **Declared effect (doc-only).** `EFFECT` (`z.Effect.READ`, `WRITE` or `ACTION`) says what acting on a contract does. It's shown in the generated docs and TypeScript, and read by `z.effect_of(cls)` for policy layers (gateways, MCP or MQTT bridges). zeared never reads it at runtime.
- **Subscriber watchdog.** Optional per-subscription freshness detector — `Cls.on_message(cb, expected_interval=N, on_quiet=, on_active=)` fires callbacks when a subscription goes silent and again when it resumes. Optimistic by default (waits for first message); `startup_grace=N` opts into "tell me if I haven't heard within N seconds of subscribing."
- **Unified shutdown.** `z.release(session=sess)` walks every zeared-owned resource for the session in the right order — subscribers, queryables, publisher cache, retention queryable, presence tokens, then `session.close()`. The graceful-shutdown path that used to take several separate calls is one.
- **Zenoh shared memory off.** Every config zeared builds sets `transport/shared_memory/enabled=false` (the hub daemon does too, over its `--config` file). Zenoh's default would carry large same-host messages through a shared-memory pool whose locked pages leak per session until messages are silently dropped. Opt back in through `zenoh_overrides`. A raw `zenoh_config=` is taken as given, so set it there yourself.
- **Retention dedupe by default.** `RETAINED` classes auto-deduplicate retention-fetch replies against live publishes via `(key_expr, timestamp)`: a replay of a value the subscriber already holds is dropped. (Since 0.4.0 — before it, replies carried no timestamp and every replay was delivered.) Opt out per class with `DEDUPE = False`.
- **Replay-vs-live signal.** `meta.origin` tells a subscriber how each sample arrived — `z.Origin.LIVE` (a real publish), `z.Origin.REPLAY` (a retained value delivered again: a retention-fetch reply at subscribe time, after a reconnect or from the re-attach walk, or a publisher's republish after its link comes back); on the presence feed, `REPLAY` marks a key already online at subscribe time and `z.Origin.WILL` a key going offline. Set by the local delivery path, except the republish, which carries a `replay` marker in its attachment (0.4.0). Live-only subscriptions via `on_message(cb, retained_fetch=False)`; async-iterator access via `Cls.alisten(meta=True)`.
- **Sync + async.** The send, subscribe, query and session calls have `a`-prefixed siblings: `await msg.asend()`, `async for msg in Cls.alisten(): ...`, `async with z.abatch(): ...`. `on_message(cb)` detects `async def` callbacks and schedules them on the running loop.
- **Batching primitives.** `with z.batch(): ...` collects sends and flushes them on exit, or discards them all if the block raises; `Cls.send_batch(items)` is the homogeneous-bulk shortcut. `asyncio` tasks get per-task isolation via `contextvars`.
- **Multi-session from day one.** Module-level default, context-local scoped override via `with z.session(other): ...` (per thread and per asyncio task, and honoured by the `a`-prefixed wrappers), and per-call `session=` kwarg — all three resolve in a fixed precedence.
- **msgpack default, JSON opt-in.** `ENCODING = 'json'` per class for human-readable traffic, or `zeared.debug = True` globally to force JSON everywhere (handy during development).
- **One-import ergonomics.** `import zeared as z` re-exports every `seared` field type, the decorator (`@z.zeared` — renamed for flavour), and the error hierarchy alongside the zeared surface.
- **Transport-mode-agnostic.** Peer, router/client, whatever — you build the session; zeared takes it.
- **Declarative connection specs.** `z.SessionConfig` is a seared dataclass for the full connection spec; pass it via `config=` to `z.peer()` / `z.client()` / `z.hub()`, or open it directly with `z.open(cfg)`. Shareable, loggable, diffable.
- **Connect-with-retry built in.** `z.client('tcp/router:7447', retry=True, max_backoff=30)` wraps `zenoh.open()` in an exponential-backoff loop — the boilerplate every daemon writes, now one kwarg.
- **Topic introspection.** `Cls.published_topics()` and `z.published_topics()` surface the set of concrete keys emitted during the process lifetime — for dashboards, diagnostics, and audit tooling.

## Setup

Each release is tagged `v<version>` on GitHub (the first is `v0.3.13`; the
latest is `v0.4.0`). Install one by its tag:

```sh
# uv
uv add git+https://github.com/bwiswell/zeared --tag v0.4.0

# pip
pip install git+https://github.com/bwiswell/zeared.git@v0.4.0
```

`uv add` records the tag as the dependency's source:

```toml
[tool.uv.sources]
zeared = { git = "https://github.com/bwiswell/zeared", tag = "v0.4.0" }
```

**The tag is the pin.** uv doesn't check a version specifier such as
`zeared>=0.4,<0.5` against a git source: it takes whatever the source points
at, and a source without a tag points at the tip of `master`. Moving to a new
release means moving the tag. What changed in each release, and what may
break on the way, is in [`CHANGELOG.md`](CHANGELOG.md).

**zeared pins `seared` the same way,** to `v0.4.0`. A project that declares
no `seared` source of its own gets zeared's. One that declares its own must
pin the same tag, or uv refuses to resolve, reporting conflicting URLs for
`seared`:

```toml
seared = { git = "https://github.com/bwiswell/seared", tag = "v0.4.0" }
```

Requires Python ≥ 3.14. Pulls in
[`seared`](https://github.com/bwiswell/seared), `eclipse-zenoh`, and
`msgpack`.

**Note:** if the consuming project uses `hatchling` as its build backend,
adding `zeared` as a direct git reference may require enabling
`allow-direct-references` in that project's `pyproject.toml`:

```toml
[tool.hatch.metadata]
allow-direct-references = true
```

## Upgrading to 0.4.0

0.4.0 is zeared's breaking release: presence and retained state survive a
restart. Every change below is listed with who it reaches and what to do.
The full list is in [`CHANGELOG.md`](CHANGELOG.md).

**1. Wire and ACL (read this first if you run a hub with access rules).**
Presence no longer uses `__zeared/alive/<zid>` or `__zeared/will/**`; both
key spaces are gone. A session that declares presence holds a liveliness
token on each contract key, so grant `liveliness_token` on those keys and
drop the `__zeared/will` / `__zeared/alive` grants. A subscriber with
`on_online` / `on_offline` declares liveliness subscribers on the contract
keys and runs a liveliness query on them, so it needs
`declare_liveliness_subscriber` and `liveliness_query` there. Taps that
watched the old keys watch the contract keys' liveliness instead.

**2. Presence.**
- `msg.register_will()` / `aregister_will()` → `Cls.declare_presence(**slots)`
  or `msg.declare_presence()`, which returns a `PresenceToken`. The will's
  payload had nowhere to go: a token carries none. Reading `register_will`
  raises `AttributeError` with a pointer.
- Offline no longer reaches `cb` as a typed will. Pass
  `on_message(cb, on_offline=…)` (and `on_online=…`); each gets an
  identity-only `ZenohMeta` (`captures`), `origin` `WILL` for offline.
  A `cb`-only subscriber silently stops seeing offline.
- `alisten` never yields presence events; move presence consumers to
  `on_message`.
- A reconnect, or a hub restart, reads as offline then online. To tell
  "unknown" from "down", use `on_link_lost` / `on_link_restored`, holding
  offline events up to one `probe_interval`: they arrive as the link
  drops, and `on_link_lost` at the next probe tick.
- `z.clear_observer` and the `gc_interval=` factory keyword are removed. A
  test fixture that resets zeared's state drops the `clear_observer()` call
  (or guards it with `hasattr(z, 'clear_observer')` to run on both 0.3.x
  and 0.4.x).

**3. `alisten` and `aquery_iter` return a `z.Stream`.** `alisten` is
bounded at `maxsize=1024` by default (it was unbounded) and drops the
*oldest* message when full, counting drops on `stream.dropped` and logging
the first. Pass `maxsize=0` to keep the old unbounded behaviour. `async for`,
`anext()` and `aclose()` still work; `asend()` / `athrow()` don't. Both now
start (subscribe, or query) when called, so call them inside a running loop.

**4. Replays of a value you already hold are dropped.** A retained reply
now carries its value's original timestamp, so with dedupe on (the default
for `RETAINED` classes) a reconnect or re-attach re-fetch no longer
re-delivers what the subscriber already received; only values it missed
arrive, as `REPLAY`. Before 0.4.0 replies carried no timestamp and every
replay was delivered. A subscriber that relied on that passes
`dedupe=False`, or calls `Cls.fetch_retained()` for the whole set.

**5. Republished retained values carry `replay: true` (wire change).** When
a `ManagedSession`'s router link comes back, it re-puts its retained values
with their original timestamps and `replay: true` next to `schema` in the
attachment. A 0.4.0 subscriber dedupes the copy or delivers it as `REPLAY`;
a 0.3.x subscriber ignores the key and treats the copy as a live publish.

**6. Link signals and the re-attach walk.** After a hub restart, a
`ManagedSession` re-fetches retained values and fires `on_link_restored`;
`on_reconnect` still means only "the session was swapped". A config-opened
session needs `auto_reconnect = true` in its `SessionConfig` to get any of
this — without it `z.open` / `z.aopen` return a plain session, as before.
`OnReconnectHandle` is now `z.CallbackHandle` (the old name is an alias).

**7. Factory keywords defer to `config=`.** `z.peer` / `z.client` (and
`z.apeer` / `z.aclient`) given a `config=` but no `auto_reconnect=` or
`probe_interval=` take those from the `SessionConfig`, so a config that sets
them now applies. Pass the keyword to override it.

**8. Slots.** A single-segment slot bound to `Path`, or to base64 `Bytes`,
raises `TopicError` at first use: use `z.Bytes(encoding='hex')` or a `Str`
slot. Publishing (`send`, `unretain`, `concrete_topic`, `ctx.reply`,
`declare_presence`) with a `/` inside a single-segment value raises, where
it used to go out on a key nobody received.

**9. seared 0.4.0.** A wire `null` on a field that isn't `nullable=True`
reads as absent; a strict `Int` refuses a fractional float, and a strict
`Bool` an int other than `0` or `1`; `ValidationError` messages gain a
`Cls.path:` prefix; `Union(default=)` is `Union(fallback=)`; seared's
`missing=` is gone. See seared's changelog. On the msgpack wire a `Bytes`
slot inside a `z.Tuple` is now raw bytes; 0.3.x nodes still read it.
zeared's generated TypeScript writes `| null` only for `nullable=True`
fields, and generated docs show `None` defaults: regenerate committed docs
(`python -m zeared.doc … -o docs`) and types.

**10. Pins.** Move both together: `zeared` to `tag = "v0.4.0"` and `seared` to
`tag = "v0.4.0"`. uv reports conflicting URLs until the seared pin matches
zeared's.

## Quick start

```python
import zeared as z


@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    id:    int            = z.Int(required=True)
    x:     float          = z.Float(required=True)
    y:     float          = z.Float(required=True)
    label: str | None     = z.Str(default=None)


# Open a session and set it as the default.
sess = z.peer()
z.session = sess

# Subscribe before publishing.
sub = Telemetry.on_message(
    lambda msg: print(f'robot {msg.id}: ({msg.x}, {msg.y})')
)

# Publish.
Telemetry(id=1,  x=1.5, y=2.5).send()
Telemetry(id=42, x=0.0, y=0.0, label='home').send()

sub.close()
z.release(session=sess)    # zeared's cleanup, then session.close()
```

`z.Int`, `z.Float`, `z.Str`, `z.Zeared`, `z.ValidationError`, etc. are
re-exports of the identically-named symbols in
[`seared`](https://github.com/bwiswell/seared); the sole rename is
`z.zeared` (which is `seared.seared` under the hood). If you prefer the
two-package style, `import seared as s; import zeared as z` still works
exactly as it did before — and `@s.seared` remains available.

## Multi-session: a node on two networks

The common "this node owns network A and talks to an external node on network
B" pattern is first-class. Three mechanisms resolve against a fixed precedence:

1. Explicit `session=` kwarg on the call.
2. Innermost `with z.session(sess):` block (context-local stack).
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

Most of the sync surface has an `a`-prefixed sibling. On the class:
`asend`, `asend_batch`, `aunretain`, `alisten` and `adeclare_presence`. On
the module: `asend`, `asend_batch`, `aunretain` and `alisten` again, plus
`aquery`, `aquery_one`, `aquery_iter`, `aon_query` and `afetch_retained`
(each taking the class first); `abatch`; and the session openers `apeer`,
`aclient` and `aopen`, which return async context managers.
`concrete_topic`, `published_topics`, `release` and `hub` have none.

Zenoh's Python bindings aren't natively async, so these wrap sync calls in
`asyncio.to_thread` (for publishes, queries and session-opens) and bridge
the Zenoh callback thread to a `z.Stream` (for `alisten` and `aquery_iter`).
Your event loop stays unblocked, with three exceptions: `alisten` declares
its subscriber on the loop thread, so on a `RETAINED` class the
subscribe-time retained fetch (a blocking `session.get`) runs there too;
`aon_query` declares inline, so an `async def` handler captures the loop;
and leaving `async with z.abatch()` flushes its sends on the loop thread.

`alisten`'s stream is bounded (`maxsize=1024` by default; `0` is
unbounded), drops its oldest message when full and counts the drop on
`stream.dropped` (0.4.0). `aquery_iter`'s stream is unbounded and takes no
`maxsize`: a query ends, and its `timeout` is the bound. A stream exposes
`maxsize` and `dropped`; `await stream.aclose()`, or leaving
`async with stream:`, releases its subscriber or stops the query worker. See
[`docs/stream.md`](docs/stream.md).

```python
import asyncio
import zeared as z


@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    id: int   = z.Int(required=True)
    x:  float = z.Float(required=True)


async def main():
    async with z.apeer() as sess:            # async session factory
        z.session = sess

        async def producer():
            for i in range(100):
                await Telemetry(id=i, x=float(i)).asend()

        async def consumer():
            async for msg in Telemetry.alisten():
                print(msg.id, msg.x)
                if msg.id == 99:
                    break                     # closes the subscriber

        # Or group sends into a batch: flushed on exit, discarded on exception
        async with z.abatch():
            await Telemetry(id=42, x=1.0).asend()
            await Telemetry(id=43, x=2.0).asend()

        await asyncio.gather(producer(), consumer())
    # z.release(session=sess) ran on block exit.


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
- Deliveries are deduplicated by default: per concrete key, the subscriber keeps the HLC timestamp of the newest sample it has delivered and drops any sample, live or replay, whose timestamp isn't newer. A retained reply carries its value's original timestamp, so a replay of a value the subscriber already holds is suppressed. A sample with no timestamp always passes. Set `DEDUPE = False` on the subscriber class to opt out (or `on_message(cb, dedupe=False)` for one subscriber).
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
    robot_id = Telemetry.coerce_captures(meta.captures)['id']   # typed
    desired.discard(robot_id)

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
class Position(z.Message):
    TOPIC = 'fleet/position/{robot}'
    robot: str = z.Str(required=True)   # key capture
    x: float   = z.Float(required=True) # ─┐ reply body
    y: float   = z.Float(required=True) # ─┘

# --- serving side ---
def handler(ctx: z.QueryContext) -> Position | list[Position] | None:
    robot = ctx.captures['robot']       # ctx.params / ctx.request also available
    x, y = locate(robot)
    return Position(robot=robot, x=x, y=y)  # or ctx.reply(...) explicitly

qbl = Position.on_query(handler)        # z.Queryable — .close() / context manager

# --- getting side ---
one  = Position.query_one(robot='r-17', timeout=2.0)  # first reply or None
some = Position.query(robot='r-$*', timeout=2.0)      # every robot whose id starts 'r-'
all_ = Position.query(timeout=2.0)                     # omitted slots → '*'
```

The handler either **returns** an instance / iterable / `None`, or replies
explicitly with `ctx.reply(...)` / `ctx.reply_err(...)` / `ctx.reply_del(...)`
for multi-reply, streaming, or error cases. `QueryContext` exposes the
request: `key_expr`, the full `selector` and its raw `parameters` string,
the parsed `params`, template `captures`, and a decoded `request` payload.

Rules:

- `query()` blocks up to `timeout` and returns only successfully decoded OK replies. Error replies (`reply_err`) and decode failures route to `on_error=` (else log), each surfaced as a `z.QueryError`.
- `query()` defaults to **no consolidation** — a query is a fan-out and each queryable may reply more than once; Zenoh's default consolidation would collapse same-key replies. Pass `consolidation=` to opt into dedup. `target=` (a `zenoh.QueryTarget`) passes through to `session.get` the same way.
- Omitted key slots widen to `*`, and so does a value of `'*'`. To match part of one segment, put Zenoh's `$*` in the value (`robot='r-$*'`); a bare `*` must be a whole segment, and `'r-*'` is a Zenoh error. `params=` appends `?k=v`; `REQUEST = SomeClass` + `request=` sends a typed request payload.
- `async def` handlers: register via `z.aon_query(Cls, handler)`; the query stays live until the coroutine resolves. Async getters: `z.aquery` / `z.aquery_one` / `z.aquery_iter` — generic in the message class since 0.3.5, so they return `list[Cls]` / `Cls | None` / `z.Stream[Cls]` and callers need no `cast`.
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

Anonymous trailing `**` matches one or more trailing path segments (the
MQTT `#` counterpart). Zenoh's `**` also matches zero, so a sample on the
bare prefix (`peer`, for `peer/**`) reaches the subscriber, matches no
template and goes to `on_error` as a `DecodeError`. Templates with
anonymous `**` are subscribe-only — `send()` on a non-publishable template
raises `TopicError`. Useful as a catch-all subscriber:

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
- Empty value (`path=''`) raises `TopicError` at render — the key would
  end in `/`, which no subscriber's `{path**}` capture matches.
- Field binding (when declared as a slot on the class): must be
  `z.Str(many=False, keyed=False)`. Other types raise `TopicError` at
  first use; non-string path tails would lose information through
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

## How a slot value becomes a key segment

Every path that builds a concrete key — `send`, `declare_presence`, a
queryable's `ctx.reply`, `unretain` (both forms), the query getters'
selectors, and `concrete_topic` — renders slots the same way (0.3.8):

- **A slot's key segment is `str(field.serialize(value))`**: the field's
  JSON-carrier string form. It's the same whatever the class's `ENCODING`
  (a key is never msgpack), and it's exactly what a subscriber's capture
  decodes back through the field's `deserialize`. So an `Enum` slot renders
  its value (`'n'`, not `'Zone.NORTH'`), a `DateTime` its ISO form
  (`2026-10-15T12:30:00+00:00`), a `TimeDelta` its seconds (`'5.0'`).
- **This applies to values of the field's own type.** Any other value
  renders as `str(value)`: strings pass through verbatim, so a selector
  wildcard (`Cls.query(robot='r-$*')`) or an already-rendered value
  (`Cls.unretain(id='7')`) keeps working. The type check is exact, so
  a `bool` isn't an `int`.
- **A slot field with a `data_key`** renders from its attribute and decodes
  back under its wire key; the slot is stripped from the payload by that
  wire key.

`concrete_topic` exposes the render directly — use it instead of rendering
a template yourself, so the key always matches what zeared publishes:

```python
@z.zeared
class Sample(z.Message):
    TOPIC = 'site/{site}/sensor/{sensor_id}'
    EXTRA_TOPICS = ('legacy/sensor/{sensor_id}',)
    site:      str   = z.Str(required=True)
    sensor_id: int   = z.Int(required=True)
    value:     float = z.Float(default=0.0)

msg = Sample(site='north', sensor_id=7, value=1.5)
msg.concrete_topic()                                  # 'site/north/sensor/7' — the key msg.send() uses
msg.concrete_topic(topic='legacy/sensor/{sensor_id}')  # a declared EXTRA_TOPICS template

Sample.concrete_topic(site='north', sensor_id=7)      # from slot values alone, no instance
Sample.concrete_topic(**row)                          # non-slot keys are ignored
Sample.concrete_topic(site='north')                   # TopicError: missing slot 'sensor_id'
```

The class form names exactly one key: a missing or `None` slot raises
`TopicError` rather than widening to a wildcard (that's what the query
getters do).

**A single-segment slot can't hold a `/`** (0.4.0), since it would split
the segment and the key would match no template:

- **At first use,** a single-segment slot bound to `Path`, or to `Bytes` in
  its default base64 form, raises `TopicError`. Use
  `z.Bytes(encoding='hex')` for bytes in a key, and a `z.Str` slot (or a
  trailing `{name**}` slot) for a path.
- **When a key is built to send on,** by `send`, `declare_presence`,
  `ctx.reply`, `unretain` or `concrete_topic`, a single-segment value whose
  rendered form contains `/` raises `TopicError`. Before 0.4.0 the message
  went out on a key nobody received. The query getters' selectors don't
  check, since they embed wildcards deliberately.

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

## Presence

Opt in with `LIVELINESS = True`. A publisher declares that one concrete key
is available with `declare_presence()`, which holds a Zenoh liveliness
token at that key; subscribers hear the key come online and go offline.
Closing, crashing or releasing the session withdraws its tokens, so a
peer's death reads as offline with no extra code (0.4.0; this replaces the
will payloads of earlier releases).

```python
@z.zeared
class PeerStatus(z.Message):
    TOPIC = 'peer/{name}/status'
    RETAINED = True
    LIVELINESS = True                 # opt in
    name:  str = z.Str(required=True)
    state: str = z.Str(default='online')


# Producer — at startup: the typed record, then the token.
PeerStatus(name='alice', state='online').send()
token = PeerStatus.declare_presence(name='alice')   # or PeerStatus(...).declare_presence()

# A proxy drops one device's availability while its own session lives on:
token.undeclare()                                    # subscribers see alice go offline


# Subscriber — the presence feed is its own pair of callbacks.
def online(meta: z.ZenohMeta):
    print('up', meta.captures['name'], meta.origin)   # REPLAY if already up, else LIVE

def offline(meta: z.ZenohMeta):
    print('down', meta.captures['name'])               # origin is z.Origin.WILL

PeerStatus.on_message(handle_status, on_online=online, on_offline=offline)
```

- **Identity, not a payload.** A token carries no data, so `on_online` /
  `on_offline` get a `ZenohMeta` whose `captures` hold the key's slots.
  The typed "up" is your retained record; `cb` never sees presence events,
  and `on_remove` stays the tombstone feed.
- **The token.** `declare_presence` takes `topic=` (a declared template) and
  `session=` as `send` does, and returns a `z.PresenceToken`: `.key` is the
  concrete key and `.active` whether the session still holds it through
  this handle. `undeclare()` (idempotent), or leaving `with token:`,
  withdraws it. The handle survives a reconnect.
- **Tokens aggregate by key.** A key reads online when its first token
  appears and offline when its last one goes, from whichever sessions held
  them. Declaring a key the session already holds returns the same
  `PresenceToken`.
- **Only subscribers see it.** Zenoh has no broker to archive an offline
  event: anything that needs it subscribes.
- **Graceful close and crash look the same.** Publish a final record before
  closing if you want them to differ.
- **A reconnect, or a hub restart, reads as offline then online.** The old
  session's tokens go before the new one's are declared; and when a link
  to the hub drops, Zenoh reports every token it learned through the hub
  as gone until the link returns, including the keys a subscriber found
  online when it subscribed. The offline events come as the link drops and
  `on_link_lost` up to one `probe_interval` later, so a consumer that wants
  "unknown" rather than "down" holds offline events that long, then shows
  "unknown" until `on_link_restored`.
- **ACL:** grant `liveliness_token` on the contract keys a session holds. A
  subscriber with `on_online` / `on_offline` declares liveliness
  subscribers on the contract keys and runs a liveliness query on them, so
  it needs `declare_liveliness_subscriber` and `liveliness_query` there
  (Zenoh's access-control message names). The `__zeared/alive/**` and
  `__zeared/will/**` keys of earlier releases are gone.

Async counterpart: `await PeerStatus.adeclare_presence(name='alice')`. See
[`docs/presence/presence.md`](docs/presence/presence.md).

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

Only **live** samples feed the watchdog (0.2.9): a retained replay
can't establish cadence from stale cached data. A `startup_grace`
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

For a `ManagedSession` (`auto_reconnect=True`), the context-manager form
is more ergonomic — no explicit `release` call needed, and exceptions
still trigger cleanup:

```python
with z.peer(auto_reconnect=True) as sess:
    z.session = sess
    Telemetry(id=1, x=1.0, y=2.0).send()
# z.release(session=sess) ran on block exit.

# Async sibling:
async with z.apeer(auto_reconnect=True) as sess:
    await Telemetry(id=1, x=1.0, y=2.0).asend(session=sess)
```

A raw session (no `auto_reconnect`) is a context manager too, but it's
Zenoh's own: leaving `with z.peer() as sess:` closes the session and skips
zeared's release walk, so zeared's per-session registries (subscribers,
publisher and retention caches, presence state) keep holding it. Call
`z.release(session=sess)` for a raw session. The async openers
(`async with z.apeer()` / `z.aclient()` / `z.aopen()`) run `z.release` on
exit either way.

What `z.release` does, in order: for a `ManagedSession`, stops its probe
and reconnect threads; closes every zeared subscriber on the session
(cancelling watchdogs and undeclaring its liveliness subscribers), then
the session's user queryables; drops the publisher cache; drops the
retention cache (undeclaring its queryables); withdraws the session's
presence tokens (which peers see as offline); then calls `session.close()`.
The order matters — the tokens are withdrawn while the session is still
open, so their DELETEs reach peers before the transport tears down.
Idempotent on a second call.

Four of the walks are also public, for dropping zeared's state without
closing the session: `z.clear_publisher_cache`, `z.clear_retention_cache`,
`z.clear_queryable_cache` and `z.clear_presence_state` each take a
keyword-only `session=`, and clear every session's state without it.

The `session=` kwarg is keyword-only and required: `z.release()` with no
arguments raises `TypeError`. Explicit-always — no implicit module-default
fallback.

For process-shutdown hooks where you don't track every session you've
opened, `z.release_all()` walks every per-session registry and tears
down each unique session in turn. Idempotent on a second call. The walk
also covers `ManagedSession` wrappers that have no zeared-level state
registered against them (a session opened with `auto_reconnect=True` but
never subscribed/published), stopping their probe + reconnect threads.

```python
import atexit
atexit.register(z.release_all)
```

## Wire format

msgpack by default — compact, binary, declared to Zenoh as
`application/msgpack`. When a class declares `ENCODING = 'msgpack'` (or
relies on the default when `z.debug = False`), zeared threads
`format='msgpack'` into seared's `dump` / `load`, so `Bytes` fields emit
native bytes on the wire — saving the ~33% base64 inflation that
JSON-safe encoding imposes — and `T` and `Tuple` fields pass the hint to
what they hold. (`NDArray` is nested lists under either encoding.)

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

Opt into JSON per class for human-readable traffic or JSON-safe wire
shapes:

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
sample, so mixed fleets interoperate. Both directions round-trip cleanly.

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
    amount:    D      = z.Decimal(required=True)
    location:  P      = z.Path(required=True)
    rows:      pd.DataFrame = z.PandasFrame(required=True)
```

`Decimal` round-trips losslessly via string; `Path` normalises to POSIX
on the wire; `PandasFrame` / `PolarsFrame` (behind seared's `pandas` /
`polars` extras) use the records form (`[{col: val}, ...]`).
`Union(fallback=Variant)` (named `default=` before seared 0.4.0) gives
graceful fallback for unknown tags. See seared's docs.

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
- `cb(msg, meta)` — `meta` is a `ZenohMeta` seared dataclass carrying the resolved `key_expr`, `timestamp`, wire `encoding`, `source_info`, `origin_zid` (the publishing session's zid, from the HLC — attribution only, never authorization), optional `attachment`, and `origin` (the replay-vs-live signal: `z.Origin.LIVE` / `REPLAY`; `WILL` on the presence feed's offline events). No Zenoh types leak into user code.

```python
def on_telemetry(msg: Telemetry, meta: z.ZenohMeta) -> None:
    print(meta.timestamp, msg.id, msg.x, msg.y)

Telemetry.on_message(on_telemetry)
```

`on_message` returns a `z.Subscriber` handle. `sub.close()` undeclares it
(idempotent), and so does leaving `with Telemetry.on_message(cb) as sub:`.

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

`with z.batch()` holds sends until the block exits, then flushes them in
order; an exception escaping the block discards the whole group, so none
is sent. The flush itself isn't transactional: a send that fails mid-flush
raises, and the sends before it are already out. `Cls.send_batch(items)` is
the homogeneous-bulk shortcut (async: `await Cls.asend_batch(items)` or
`await z.asend_batch(Cls, items)`).

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

Async siblings (`z.apeer`, `z.aclient`, and `z.aopen(cfg)`) take the same
arguments and layer kwargs over `config=` the same way. Each returns an
async context manager (`async with z.apeer() as sess:`) that opens the
session off the loop and runs `z.release` on exit.

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

`z.open` dispatches `z.Mode.PEER` to `z.peer`, `z.Mode.CLIENT` to
`z.client` and `z.Mode.ROUTER` to `z.hub`; a router config with
`auto_reconnect` set raises `ValueError`.

When both `config=` and explicit kwargs are passed (endpoints, retry and
backoff, `auto_reconnect`, `probe_interval`, ...), the kwargs win
(per-call overrides over a shared base spec); a kwarg left out defers to
`config=`. A raw `zenoh_config=<zenoh.Config>` is the escape hatch below
both, and it *replaces* the config zeared would build rather than layering
on it: zeared sets none of its own settings on it (not the mode,
timestamping, or shared memory off), writes only the endpoints you pass,
and ignores a `SessionConfig`'s `zenoh_config_file` / `zenoh_overrides`.
Set what you need in it yourself.

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

`SessionConfig.from_env()` reads a spec from the environment, one variable
per field (`ZEARED_SESSION_MODE`, `ZEARED_SESSION_ROUTER`,
`ZEARED_SESSION_CONNECT` as a comma-separated list, ...; `prefix=` replaces
`ZEARED_SESSION_`), and `SessionConfig.from_toml(path)` reads a TOML file
with the fields at the top level. Raw Zenoh settings the typed fields
don't cover (TLS/mTLS, access control, scouting) go in `zenoh_config_file`,
a path to a JSON5 Zenoh config, and `zenoh_overrides`, a dict of config
paths to values (`{'scouting/multicast/enabled': False}`); the overrides
aren't read from the environment. zeared builds the session's config from
the file, then applies its mode, timestamping and shared memory off, then
the overrides.

## NAT traversal: the relay hub

Two nodes that can't reach each other directly — e.g. both behind NAT,
outbound-only — need a relay in the middle. `z.hub()` opens a **router-mode**
session that does exactly that, in-process: it relays pub/sub, queries, and
liveliness (so retention/queryables **and** presence all work through it).
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
# --connect links to another hub (multi-hub mesh);
# --config is the TLS / access-control config.
python -m zeared.hubd \
  --listen tls/0.0.0.0:7447 \
  --connect tls/hub-b.example.com:7447 \
  --config /etc/zeared/hub.json5
```

The daemon is stateless and holds no zeared message state — it routes opaque
samples, so it needs none of your message classes installed. It stops cleanly on
SIGINT/SIGTERM. `--listen` and `--connect` repeat; `--no-timestamping` leaves
HLC timestamping off (it's on by default), and `--log-level` sets the logging
level (default `INFO`; logs go to stdout).

Notes and limits:

- **Secure a public hub.** An open relay is reachable by anyone who can hit
  the port. Configure Zenoh **TLS/mTLS + access-control** via `--config`
  (a JSON5 Zenoh config) or `z.hub(zenoh_config=...)`. `--listen`/`--connect`
  override the file's endpoints when given; without them, the file's stand
  (and a hub with listen endpoints nowhere binds `tcp/0.0.0.0:7447`).
  The daemon applies router mode, timestamping (unless `--no-timestamping`)
  and shared memory off over its `--config` file, as
  `z.hub(config=z.SessionConfig(mode=z.Mode.ROUTER, zenoh_config_file=...))`
  does over its file. A raw `zenoh_config=` is
  taken as given: set `mode` to `"router"`, `timestamping/enabled` to
  `true` and `transport/shared_memory/enabled` to `false` in it yourself.
- **`z.hub()` takes `config=` too** (a `SessionConfig`; `z.open` on a
  `Mode.ROUTER` config calls it), along with the retry kwargs.
- **Single hub is a SPOF.** For HA, run multiple hubs and link them
  (`--connect` / `z.hub(connect=[...])`); nodes can list several routers.
- **Hub returns a raw `zenoh.Session`** — a listener has no zeared-owned
  resources to supervise, so there's no `ManagedSession` wrapper. The
  *client* side gets `auto_reconnect=True` as usual.
- **`z.client(router=...)` already targets any router** — including a
  standalone `zenohd` if you'd rather run one. The hub just means you don't
  have to.

### Auto-reconnect

Long-running daemons that need to survive a session dying, or a hub
restarting, opt in to auto-reconnect:

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

A config-opened session opts in the same way (0.4.0): set
`auto_reconnect = true` (and optionally `probe_interval`, `resync_jitter`,
`resync_concurrency`) in its `SessionConfig`, whether built in code, read
from a TOML file by `SessionConfig.from_toml(path)` (fields at the top
level), or read from the environment by `SessionConfig.from_env()`
(`ZEARED_SESSION_AUTO_RECONNECT=true`, ...), and `z.open` / `z.aopen`
return a `ManagedSession`. Without it, a config-opened session is a plain
one: no reconnect, no link signals, no re-fetch after a hub restart.

Detection: an active probe (`is_closed()` / `zid()` poll) catches dead
sessions on subscriber-only daemons; send-failure detection catches the
gap between probe ticks for publisher-heavy paths.

**A lost router link is not a dead session** (0.4.0). If a client's hub
restarts, the client's own session stays open and Zenoh re-dials the
router by itself, so nothing is re-declared and `on_reconnect` doesn't
fire. The probe watches the link instead:

- when the session's routers drop to none, it fires `on_link_lost`;
- when they come back, it **republishes** the session's own retained
  values (original timestamps, so subscribers that hold them dedupe the
  copy; `REPLAY` for those that missed them), runs the **re-attach walk** —
  it re-runs the retained fetch for every `RETAINED` subscriber (`REPLAY`,
  dedupe-safe), so a value published while the link was down is delivered —
  then fires `on_link_restored`. Between them, it doesn't matter which side
  re-attaches first;
- a router that came back with a new zid between two probe ticks counts as
  both.

The walk waits a random 0 to `resync_jitter` seconds first (default 2), so a
fleet re-attaching to a restarted hub doesn't fetch at once, then fetches on
at most `resync_concurrency` workers (default 4); with nothing to fetch it
skips both. A router that keeps a fixed zid and restarts within one
`probe_interval` is invisible to the probe.

The link watch compares the session's set of routers, so it needs a session
attached to a router (a client of a hub, say); a pure peer mesh has none,
and the probe never fires the link signals there. A dead-session reconnect
fires `on_link_lost` / `on_link_restored` in any mode.

Restoration: cached publishers are invalidated (the next `send()`
re-declares against the new raw, so nothing is dropped on the far side of
a reconnect), subscribers and queryables are re-declared against the new
raw, retained fetches replay (dedupe-safe), and presence tokens are
re-declared (peers see the keys go offline, then online — a session's
tokens die with it, not a bug).

During the reconnect window, `send()` / `get()` / `delete()` raise
`SessionDeadError`. Handle, retry, drop, or queue at the call site.

Per-subscriber opt-out: `Cls.on_message(cb, auto_reconnect=False)` keeps
that subscription bound to the original raw — useful when application
state must survive the boundary explicitly.

`probe_interval=0` (or `None`) disables the active probe, and with it
everything the probe drives: a dead session is detected only by a failing
send, and the router link isn't watched, so a hub restart fires no
`on_link_lost` / `on_link_restored` and runs no republish or re-attach
walk. A reconnect that a send failure triggers still fires both signals.

Daemons that need to react — log it, refresh caller-side caches, tell
their own clients to re-sync — register a callback:

```python
def refresh_caches(s):
    my_app.peer_registry.invalidate()

sess.on_link_lost(lambda s: my_app.mark_stale())    # the view of the mesh is stale
handle = sess.on_link_restored(refresh_caches)      # whole again: re-sync now
sess.on_reconnect(lambda s: my_app.metrics.record_reconnect())
# handle.cancel() to deregister; idempotent.
```

| Signal | Fires when |
|--------|------------|
| `on_link_lost` | the routers drop to none, or the session is found dead (before the reconnect) |
| `on_link_restored` | after the re-attach walk, or after a reconnect's walks; only after a loss, never on `DEAD` |
| `on_reconnect` | the raw session was swapped and every restore walk ran |

`on_link_lost` / `on_link_restored` don't care what caused the loss, so a
consumer that just needs "re-sync now" registers `on_link_restored` alone.
Each returns a `z.CallbackHandle` (`OnReconnectHandle` is its old name).
Multiple callbacks fire in registration order. `async def` callbacks work
too (registered from inside an event loop). Exceptions log + continue.

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

To compute the key a message *would* publish on (without sending), use
`msg.concrete_topic()` / `Cls.concrete_topic(**slots)` — see
[How a slot value becomes a key segment](#how-a-slot-value-becomes-a-key-segment).

## TypeScript types for browser consumers

`zeared.doc` generates TypeScript types for your message classes, so a browser
app consuming mesh traffic over a websocket gets a typed view of the wire.
**Types only** — the mesh→websocket bridge (auth, identity scope, transport)
is yours to build; the generated types describe the JSON shape a bridge hands
the browser after decoding a zeared message.

```sh
python -m zeared.doc.typescript fleet_messages -o fleet-types.ts
python -m zeared.doc.typescript fleet_messages -o fleet-types.ts --check   # CI guard
```

```ts
// Generated: one interface per message + referenced payloads, enums as unions.
export interface Telemetry {
  robot: string;
  seq: number;
  battery?: number;        // optional (has a default)
  note?: string;           // optional (`str | None`): absent when None
  heading: number | null;  // nullable=True: present, may be null
  x: number;
  // …
}
```

Mapping: scalars → `number`/`string`/`boolean`; `Enum` → literal unions
(`0 | 1`, `"fast" | "slow"`); `many` → `T[]`; `keyed` → `Record<string, T>`;
`z.T(Nested)` → an interface reference; `z.Union` → a discriminated union;
`z.Tuple` → a TS tuple. Keys are **wire keys** (`data_key` when set). `required`
→ no `?`; a default → `?`; `nullable=True` → `| null` (an ordinary `X | None`
field is optional, absent when `None`, never `null`). `dump=False` fields are
omitted. `Bytes` is base64 (`string`) on the JSON wire, and `TimeDelta` is its
seconds (`number`); frames/ndarrays are `unknown` — decode them in your bridge.
Each message interface carries its wire contract as JSDoc: `@topic`,
`@extraTopic`, `@schemaVersion`, `@encoding`, and `@effect` when the class
declares `EFFECT`. Also available programmatically:
`from zeared.doc import generate_ts; generate_ts('fleet_messages')` returns the
`.ts` source as a string.

A downstream package can give a custom field its own TypeScript spelling: subclass a
seared field and set `TS_TYPE = 'SecretRef'` (the type the member gets) and, optionally,
`TS_PRELUDE = 'export type SecretRef = string & {...};'` (an alias emitted once, before the
interfaces). Fields without `TS_TYPE` map by class name as before.

`zeared.doc` also writes Markdown reference docs, a page per class, with a
message's topic and wire contract above its fields:
`python -m zeared.doc fleet_messages -o docs` (the default output is
`./docs`). It's installed as the `zeared-doc` script too; `--check` writes
nothing and exits non-zero when the committed docs have drifted. See
[`docs/doc/doc.md`](docs/doc/doc.md) for both generators.

## Errors

```
ZearedError
├── NoSessionError
├── TopicError
├── SessionDeadError
├── SubscriptionError              # declare-time, fatal to one subscription
├── QueryableError                 # declare-time, or serving-side on_error
└── SubscriberError                # dispatch-time, routed to on_error
    ├── DecodeError
    │   └── SchemaMismatchError
    ├── CallbackError
    ├── RetainedFetchError
    └── QueryError                 # getting side, per reply
```

| Exception | Raised when |
|-----------|-------------|
| `z.NoSessionError`      | No session is resolvable (no kwarg, no scope, no default) |
| `z.TopicError`          | `TOPIC` is missing or malformed (bad template syntax, a format spec, a non-trailing `**`), or violates the namespace reservation (`__zeared/...`); a single-segment slot is bound to `Path` or base64 `Bytes`, or a `{name**}` slot to anything but `Str`; a publish would put a `/` inside a single-segment value; a slot is missing when a key is built; a `topic=` override matches no declared template (or a subscribe-only one); `retain=True`, `unretain` or `fetch_retained` on a non-`RETAINED` class; `on_query` on a `RETAINED` class; presence calls (`declare_presence`, `on_online` / `on_offline`) on a class without `LIVELINESS` |
| `z.SessionDeadError`    | A `ManagedSession` op (`put`/`get`/`delete`/`zid`/...) is invoked while the session is `RECONNECTING` or `DEAD` |
| `z.SubscriptionError`   | `on_message` — declaring the underlying Zenoh subscriber, or a presence feed's liveliness subscribers, failed; or an `async def` callback was given with no running event loop (fatal to that subscription) |
| `z.SubscriberError`     | Parent for dispatch-time errors. Routed to `on_error=`; subscription continues |
| `z.DecodeError`         | Sample's payload failed to `codec.unpack` / `Message.load`. Subclass of `SubscriberError` |
| `z.SchemaMismatchError` | Wire-attached `schema` ≠ class's `SCHEMA`. Subclass of `DecodeError` |
| `z.CallbackError`       | User's `cb(msg)` itself raised. Subclass of `SubscriberError` |
| `z.RetainedFetchError`  | A retained-fetch reply failed to dispatch. Subclass of `SubscriberError` |
| `z.QueryableError`      | Declaring a queryable failed, or an `async def` handler had no running loop at declare time (fatal to that queryable). Also handed to the serving side's `on_error` when a query's context can't be built, a reply fails, or the handler raises; the queryable keeps serving |
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

<!-- bench:readme-headline -->
| Strategy                        | pub/s | wire (B) |
|---------------------------------|------:|---------:|
| Zenoh + `marshmallow` (JSON)    | 3,785 | 796      |
| `zeared` sync (msgpack, cached) | 6,699 | 533      |
<!-- /bench:readme-headline -->

zeared is <!-- bench:readme-faster -->~77%<!-- /bench:readme-faster --> faster and <!-- bench:readme-smaller -->~33%<!-- /bench:readme-smaller --> smaller on the wire.

**"Pydantic + MQTT, or zeared?"** — the comparison people actually want, and
one that splits into two independent questions. pydantic's compiled core
beats pure-Python seared on serialization (<!-- bench:readme-codec -->~3.6x<!-- /bench:readme-codec -->); Zenoh beats a
broker-mediated MQTT hop on transport (<!-- bench:readme-transport -->~2.3x<!-- /bench:readme-transport -->). Netted out at default
configuration against MQTT QoS 0 <!-- bench:readme-runs -->(medians of three runs, here and below)<!-- /bench:readme-runs -->,
**pydantic + MQTT comes out <!-- bench:readme-behind -->~1.4x<!-- /bench:readme-behind --> ahead** —
the codec gap is the larger of the two, and pure Python doesn't make it back.

Two things change that. Against **QoS 1** — at-least-once, what most
production fleets actually run, and the closer analogue to Zenoh's reliable
`put` — zeared is <!-- bench:readme-qos1 -->~2.3x<!-- /bench:readme-qos1 --> ahead. And with the optional `rusted` accelerator the
codec gap closes and the ordering flips: <!-- bench:readme-rusted-q0 -->~1.3x<!-- /bench:readme-rusted-q0 --> against QoS 0, <!-- bench:readme-rusted-q1 -->~4.2x<!-- /bench:readme-rusted-q1 --> against
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
against the wire value automatically and drop every sample whose schema
differs (or is missing). The first mismatch from each
`(sender zid, observed schema)` pair goes to `on_error` as a
`SchemaMismatchError` (or is logged as a warning without one); later ones
from the same pair drop silently, so a misaligned peer doesn't spam the log
on every message. The sender is the zid in the sample's HLC timestamp
(0.4.0); a sample without a timestamp has no sender zid, so all such
samples share one pair per schema. Each subscriber keeps its own pairs and
forgets them after a reconnect.

`meta.issued_at` is a `datetime.datetime` (UTC) parsed from Zenoh's HLC
timestamp on the sample. Requires a timestamp: zeared's factories enable
timestamping in the configs they build, and a retained send stamps its own
(0.4.0). `None` when the sample carries none (e.g. a non-retained send from
a session opened with `timestamping=False`, or with a raw `zenoh_config=`
that doesn't enable it).

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
import queue

failures: queue.Queue[Exception] = queue.Queue()

def on_err(exc: z.SubscriberError, raw: bytes):
    if isinstance(exc, z.SchemaMismatchError):
        log_drift(exc)
    elif isinstance(exc, z.DecodeError):
        log_corrupt_payload(exc, raw)
    elif isinstance(exc, z.CallbackError):
        # user code blew up — hand it to the main thread
        failures.put(exc)

PeerStatus.on_message(handle, on_error=on_err)
```

A subscriber's `on_error` runs on Zenoh's callback thread, and anything it
raises is caught and logged alongside the original error (a queryable's
`on_error` too), so it can't surface to the caller. To act on a failure
elsewhere, hand it off, as above. The getters' `on_error` (`query()`,
`fetch_retained()`) runs on the calling thread instead.

Hierarchy: `SubscriberError` → `DecodeError` (→ `SchemaMismatchError`)
/ `CallbackError` / `RetainedFetchError` / `QueryError`. Each wraps the
original exception via `__cause__`. Handle them generically (any
`SubscriberError`) or use `isinstance` to discriminate.

## Per-subscriber dedupe override

Class-level `DEDUPE = True` (default for `RETAINED` classes) drops any
sample, live or replay, whose timestamp isn't newer than the last one the
subscriber delivered on that key, so a retention-fetch replay of a value it
already received is suppressed: each retained reply carries the timestamp
its value was published with (0.4.0; earlier replies carried none, so every
replay got through). A specific subscriber can opt out:

```python
audit_sub = Telemetry.on_message(audit_cb, dedupe=False)  # see every replay
prod_sub  = Telemetry.on_message(prod_cb)                 # class default (dedupe on)
```

## Limits (0.4.0)

- **Tombstone removals are not durably delivered.** `on_remove` fires on a *live* DELETE only; a subscriber offline during the `unretain()` never sees it (the retained value is already gone, no tombstone is replayed). For a durable removal guarantee, poll `Cls.fetch_retained()` and reconcile against the returned set — `on_remove` is the latency path, `fetch_retained` the correctness path.
- **`on_remove` delivers identity, not the removed value.** A tombstone has no payload, so `on_remove` gets a `ZenohMeta` (captures identify the key) — never a typed instance. Reconcile keyed on identity; if you need the departed entity's last data, capture it from the add/update `cb`.
- **Queryables are compute-serving, not cache-serving.** `Cls.on_query` computes each reply on demand; it does not persist state. A `RETAINED` class already serves a cache-backed queryable over the same topic, so the two are mutually exclusive on one class (`on_query` raises `TopicError` on a `RETAINED` class).
- **Breaking out of a streaming getter doesn't stop the server.** `Cls.iter_query` / `z.aquery_iter` stop *your* consumption; the serving queryable runs to completion regardless. Zenoh's cancellation is client-side only, so there is no cancel knob to offer. Unlike `alisten`, whose cleanup undeclares a real subscriber.
- **Async `on_query` holds the query until the coroutine resolves.** A slow async handler pins the underlying `zenoh.Query` for its lifetime. Correct, but a long-running handler ties up the query.
- **Query parameters are a `str → str` dict.** `params=` maps to the Zenoh selector (`?k=v`); typed parameters are deferred. For a rich, typed request use `REQUEST = SomeClass` + `request=` (sent as the query payload) instead.
- **Async is a wrapper, not native.** Zenoh's Python bindings are sync-only; `asend` / `alisten` / `aunretain` wrap via `asyncio.to_thread` and a callback-to-queue bridge. Correct, but not zero-overhead — see Benchmarks.
- **Retention dedupe is timestamp-dependent.** Dedupe orders samples by their HLC timestamps; a sample with no timestamp always passes through and doesn't move the watermark. Retained sends stamp their own puts (0.4.0), so retained publishes and replies dedupe whatever the session's config; a `send(retain=False)` carries a timestamp only from a session with timestamping on. zeared's factories enable `timestamping/enabled=true` in the config they build, so most callers don't need to think about this — opt out via `z.peer(timestamping=False)`. A raw `zenoh_config=` is taken as given, so you're in charge: enable timestamping in it yourself, or that session's non-retained sends carry none.
- **Retention TTL is lazy.** `RETENTION_TTL = N` opts into time-based expiration of cached entries, but expiration is only checked when a subscriber issues a retained-fetch (`_handle_query`) — topics that nobody ever queries may keep stale entries in memory. Acceptable for typical use; no background sweeper.
- **Retry only on `zenoh.open()` raise (without auto-reconnect).** Vanilla `z.peer()` / `z.client()` retry only on the initial open. With `auto_reconnect=True`, mid-flight session deaths are detected and rebuilt; without it, a session dying silently means the next send fails.
- **`{name**}` and anonymous `**` are trailing-only.** Non-trailing multi-segment forms (`a/**/c`, `peer/{cluster**}/{host}/status`) aren't supported.
- **A single-segment slot can't hold a `/`.** A slot bound to `Path`, or to base64 `Bytes`, raises `TopicError` at first use, and a value containing `/` raises when a key is built to send on (0.4.0). Use `Bytes(encoding='hex')` or a `Str` slot, or a trailing `{name**}` slot for a path tail.
- **`alisten` drops, it doesn't block.** Its `Stream` is bounded (`maxsize=1024` by default; `0` is unbounded). A full stream drops its oldest message, counts it on `stream.dropped` and logs the first drop; there's no backpressure (0.4.0).
- **Format specs rejected in templates.** `{x:03d}` / `{x!r}` / `{x**:fmt}` raise `TopicError`. Plain `{name}` and `{name**}` are the only slot syntaxes.
- **Publish-side `topic=` is restrictive.** The override must match one of the declared templates (`TOPIC` or an entry in `EXTRA_TOPICS`).
- **One `z.Union` field per class.** Multiple would collide on top-level wire keys.
- **No automatic session-close detection on caches without auto-reconnect.** Zenoh sessions don't support weakrefs; when running raw, cached publishers are undeclared on the next failing send (the cache, and its `published_topics()` history, stay until `z.release(session=sess)` or `clear_publisher_cache(session=sess)`), and retention queryables go on a `z.clear_retention_cache(session=sess)` call (or `z.release`). When running managed they are lazy-rebuilt after reconnect.
- **Presence is seen by subscribers only.** Non-subscribers never observe an offline event. Graceful close withdraws a token the same as a crash — if you need distinct records, publish explicitly before close. A session that holds a key *and* watches it sees the key go offline when another holder leaves; to everyone else a key goes offline only when its last token goes.
- **`origin_zid` is attribution, not authentication.** The zid comes from the sample's HLC, and `put(timestamp=...)` lets a publisher claim any value. Enforce origin at the router (Zenoh access control on a TLS certificate), not from this field.
- **Dedupe tolerates a bounded clock skew.** A sample whose HLC is more than `DEDUPE_MAX_SKEW` seconds (default 300) ahead of local time is delivered but does not advance the dedupe watermark — otherwise one far-future timestamp blinds the subscriber to that key until restart. Set `None` to disable.
- **Link detection is a poll.** The probe checks the router link every `probe_interval` (default 10 s), so a hub outage is noticed up to that late, and a router that keeps a fixed zid and restarts within one interval goes unseen. Link signals and the re-attach walk need a `ManagedSession` (`auto_reconnect=True`), and the probe's link watch needs a router: in a pure peer mesh only a dead-session reconnect fires the link signals. See *Auto-reconnect*.
- **`send()` raises during the reconnect window.** With `auto_reconnect=True`, ops on a session in `RECONNECTING` or `DEAD` state raise `SessionDeadError`. No buffering / blocking — callers that want either build it on top.
- **Raw `threading.Thread` doesn't propagate batch context.** `with z.batch():` + raw thread spawn means the worker bypasses the batch. Use `asyncio.to_thread()` or `contextvars.copy_context().run()` for thread-spawned workers. See `docs/batch.md`.

## Development

```sh
uv sync
uv run pytest tests/
```

Six checks gate every commit, wired as `prek` hooks — run
`uv run prek install` once per clone:

```sh
uv run ruff check                    # lint
uv run ruff format                   # format
uv run deptry .                      # dependency hygiene
uv run ty check                      # type check
uv run python -m bench.tables --check  # benchmark numbers match bench/results.json
```

The sixth refuses any commit on `master`.

**Branches and releases.**
- **`master` holds released commits only,** and moves only by merge.
- **A release is a `version/x.y.z` branch** off `master`.
- **Work on it happens in `feat/…` and `fix/…` branches,** each a pull
  request squash-merged into the version branch. Each adds its own lines to
  the `unreleased` entry at the top of `CHANGELOG.md`.
- **Once those are all in, a `chore/version-bump-and-benches` branch** makes
  two commits:
  1. it bumps the version, in `pyproject.toml` and `zeared/__init__.py`
     together (a test checks that they agree), refreshes the lockfile, and
     dates the changelog entry;
  2. it re-records the benchmarks.
- **The version branch then merges into `master`.** There, the `Release Tag`
  workflow (`.github/workflows/release-tag.yml`) tags it `v<version>` and
  publishes a GitHub release.

The benchmarks and their comparator libraries are repo-only, behind an
opt-in extra. Every benchmark number in this README and in
`docs/overview/benchmarks.md` is generated from the committed
`bench/results.json`, as the median of its runs; recording a run re-renders
them:

```sh
uv sync --extra bench
uv run python -m bench --runs 3      # how the committed artifact is recorded
```

The accelerated rows need [`rusted`](https://github.com/bwiswell/rusted),
which zeared declares nowhere. It isn't on a package index, so install a
wheel built from it for the run, after any `uv sync` (a sync removes what the
lockfile doesn't name), and record with `--no-sync` so `uv` leaves it in
place:

```sh
uv pip install path/to/rusted-*.whl
uv run --no-sync python -m bench --runs 3
```

The MQTT comparison needs `mosquitto` on `PATH`; suites whose requirements
are missing skip with a note.

Tests mirror the source layout: a `test_*.py` per module, in the matching
directory (`tests/test__codec.py` for `zeared/_codec.py`,
`tests/message/test__message_render.py` for
`zeared/message/_message_render.py`). A few files test across modules:
lifecycles and integrations (`tests/test_hub_restart.py`,
`tests/queryable/test_lifecycle.py`, `tests/doc/test_wire.py`, …), which
is where modules such as the queryable dispatch are covered.
Integration tests spin up real Zenoh peer sessions in-process; they cover the
full send → subscriber → decode path, two-session isolation, scoped overrides,
the debug flag, and error handling.
