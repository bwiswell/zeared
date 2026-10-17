# `queryable/`

First-class Zenoh query/reply (request/response) for `@z.zeared` message
classes. Two sides:

- **Serving** — `Cls.on_query(handler)` declares a `zenoh.Queryable` that
  answers peer `session.get()` requests with *computed* message instances.
  Returns a `Queryable` handle.
- **Getting** — `Cls.query(**key_fields, ...)` issues a `session.get()`,
  decodes each reply through the class's own `_decode`, and returns typed
  instances. `Cls.query_one(...)` returns the first (or `None`), and
  `Cls.iter_query(...)` yields each reply as it lands.

This is the compute-serving sibling of `RETAINED` (which serves a *cached*
value via its own internal queryable) and the request/response sibling of
`Subscriber`. The message class's declared `TOPIC` template is both the
queryable key and the source of reply captures; its seared fields are the
**response** body — serialized on the wire exactly like `send()`.

## Serving: `on_query`

```python
@z.zeared
class Position(z.Message):
    TOPIC = 'fleet/position/{robot}'
    robot: str        = z.Str(required=True)   # ← key capture
    x: float          = z.Float(required=True) # ─┐ response body
    y: float          = z.Float(required=True) # ─┘

def handler(ctx: z.QueryContext) -> Position | list[Position] | None:
    robot = ctx.captures['robot']
    x, y = locate(robot)
    return Position(robot=robot, x=x, y=y)

qbl = Position.on_query(handler)   # z.Queryable; .close() / context manager
```

The handler receives a `QueryContext` and may either **return** a message
instance, an iterable of them, or `None`; or reply explicitly via
`ctx.reply(...)` / `ctx.reply_err(...)` / `ctx.reply_del(...)` (for
multi-reply, streaming, or error cases) and return `None`. Failures route
to `on_error=` (or the log), never into Zenoh; see [Errors](#errors).

One `zenoh.Queryable` is declared per declared template (`TOPIC` +
`EXTRA_TOPICS`), routed through the raw session to skip the user-facing
declare warning. The handle survives reconnect (managed sessions,
`auto_reconnect=True` default) — see reconnect restore below.

### `QueryContext`

A narrow wrapper over `zenoh.Query` (like `ZenohMeta` wraps `Sample`) so
handlers never import Zenoh:

| Attribute | Meaning |
|-----------|---------|
| `key_expr` | the queried key expression (concrete or wildcard) |
| `selector` | full selector (key + parameters) |
| `parameters` / `params` | raw parameter string / parsed `{str: str}` |
| `captures` | template-slot values parsed from `key_expr` (raw strings) |
| `request` | decoded `REQUEST` instance, else raw payload bytes / `None` |

`captures` comes from the first declared template whose pattern matches
`key_expr`, and a wildcard chunk binds literally, as it appears in the
key: a query on `fleet/position/*` gives `{'robot': '*'}`, one on
`fleet/position/r-$*` gives `{'robot': 'r-$*'}`. It is `{}` when no
template matches the key (`fleet/**`, say).

`ctx.reply(instance)` has the same wire shape as `instance.send()`: slot
fields stripped from the body, the instance's effective encoding, the
class `SCHEMA` attachment. It renders the key on the template the query
matched, so a query on an `EXTRA_TOPICS` template is answered on that
template (a reply key must intersect the query's key expression). With no
matching template, or for an instance of another class, it falls back to
the instance's canonical `TOPIC`. A single-segment slot whose value
renders with a `/` raises `TopicError` (0.4.0). `ctx.reply_err(payload)`
takes `str` (UTF-8 encoded) or `bytes`; `ctx.reply_del(key_expr)` answers
with a tombstone for that key.

### Generator handlers

A **generator** handler is the streaming form of the return form: each
yielded instance is replied as it is produced, so the handler never
materialises the full result set. This is the server side of a multi-reply
query — memory stays O(1) in the number of replies.

### Async handlers

`async def` handlers are supported — register via `z.aon_query(Cls,
handler)` (or `Cls.on_query` from within a running loop). The loop is
captured at declare time; the query stays live until the coroutine
resolves, then its return value is replied.

`async def` **generator** handlers are supported on the same footing
(0.3.2) and are drained with `async for`. They need a running loop at
declare time exactly as coroutine handlers do — `Queryable._declare` tests
`iscoroutinefunction` **or** `isasyncgenfunction`, since the former alone
is `False` for an async generator function and used to route one down the
sync path, where it replied nothing at all.

### Errors

Errors never reach Zenoh's callback. Each one goes to `on_error(exc, b'')`
as a `QueryableError` (a query has no sample payload, hence `b''`), or to
the `zeared.queryable` log when there is no `on_error`:

- **A handler that raises** sends a best-effort `reply_err` as well, so
  the getter learns the answer was cut short — including a generator
  raising mid-stream, which surfaces only as the iterator is advanced,
  long after `handler(ctx)` itself returned cleanly.
- **A reply that fails** on one returned or yielded item (`ctx.reply`
  raising `TopicError` for a `/` in a single-segment slot, say) loses
  only that item: the rest of the stream continues, and no `reply_err` is
  sent. A handler that calls `ctx.reply` itself and lets the error escape
  takes the path above instead.
- **A `QueryContext` that fails to build** means the handler never runs.

Since 0.4.0 an `on_error` that itself raises is guarded too: the failure
is logged with the original error, and the getter still gets its
`reply_err`.

## Getting: `query` / `query_one` / `iter_query`

```python
# Concrete — one key.
positions = Position.query(robot='r-17', timeout=2.0)

# Wildcard — omitted slots widen to '*', so this asks the whole template.
everywhere = Position.query(timeout=2.0)

# Part of a segment — Zenoh's '$*' inside a value. A bare '*' must be a
# whole segment: 'r-*' is a Zenoh error.
some = Position.query(robot='r-$*', timeout=2.0)

# First reply (or None).
one = Position.query_one(robot='r-17', timeout=2.0)
```

`query()` blocks up to `timeout`. It returns only successfully decoded OK
replies; error replies (`reply_err`) and per-reply decode failures route to
`on_error(exc, raw)` when supplied, else log — each surfaced as a
`z.QueryError`.

### Streaming: `iter_query` / `z.aquery_iter`

Since 0.3.4 the getter can stream too: `Cls.iter_query(...)` and
`z.aquery_iter(...)` yield each reply as it lands. Paired with a generator
handler that gives a query which is O(1) on both ends and hands the caller
its first answer without waiting out the window. `Cls.query` is `list()`
over `iter_query`, so the decode and error-routing paths cannot drift.

Three properties to know before reaching for it:

- **`timeout` is total duration**, and the underlying `session.get` fires
  when the getter is *called* — not on the first `next()`. Holding an
  iterator and consuming it later spends the window regardless.
- **`on_error` fires on consumption.** An iterator nobody iterates reports
  nothing, where `query` would already have routed every failure.
- **Breaking out does not stop the server.** Measured: a client abandoning
  the channel after 2 of 8 replies — with or without an explicit
  `CancellationToken` — still left the queryable producing all 8. Zenoh's
  cancellation is client-side only, which is why no cancel parameter is
  offered; shipping one would only imply a guarantee that isn't there.
  This is the one place the `alisten` analogy breaks: closing its stream
  undeclares a real subscriber and genuinely stops the work.
  `z.aquery_iter` returns the same [`Stream`](../stream.md) type, unbounded,
  so its `dropped` stays `0` (0.4.0).

`query_one` rides on the same path and short-circuits at the first decoded
reply instead of waiting out `timeout`. It therefore sees only the errors
that arrive before that reply; use `query` when you need all of them.

### Consolidation

`query()` defaults to **no consolidation** (`ConsolidationMode.NONE`): a
zeared query is a fan-out — every matching queryable may answer, possibly
more than once — and Zenoh's default consolidation collapses replies
sharing a key expression, silently dropping legitimate answers. Pass
`consolidation=` to opt into dedup. `target=` also passes straight through.

### Typed request payloads

Set `REQUEST = SomeClass` on the class to send a typed request body:

```python
@z.zeared
class LocateReq(z.Message):
    TOPIC = 'fleet/req/locate'  # unused for routing; carried as payload
    method: str = z.Str(default='odometry')

@z.zeared
class Position(z.Message):
    TOPIC = 'fleet/position/{robot}'
    REQUEST = LocateReq
    ...

Position.query(robot='r-$*', request=LocateReq(method='lidar'))
# handler sees ctx.request == LocateReq(method='lidar')
```

Without `REQUEST`, `ctx.request` is the raw payload bytes (or `None`). Use
`params=` for lightweight scalar filters (`?k=v`) that don't warrant a
typed body.

### Declaring effect

Set `EFFECT` to say what serving a query does (0.3.12):
- `z.Effect.READ`: it changes nothing;
- `WRITE`: it changes state, and repeating it is harmless;
- `ACTION`: anything else.

Declare it on the class you call `on_query` / `query` on, the one with
the `TOPIC`; it doesn't go on the `REQUEST` class. If different requests
have different effects, declare the strongest.

zeared never reads `EFFECT` at runtime. It's surfaced in the generated docs
and read by `z.effect_of` for policy layers. See [`effect.md`](../effect.md).

```python
@z.zeared
class Position(z.Message):
    TOPIC  = 'fleet/position/{robot}'
    EFFECT = z.Effect.READ
    ...
```

## Lifecycle & reconnect

Queryables are registered per `id(session)` in a module-level registry.

- **Close**: `qbl.close()` (idempotent) or context manager; undeclares the
  underlying handles and deregisters.
- **`z.release(session=)`** closes every queryable for the session right
  after subscribers, before publisher/retention/presence teardown.
- **`z.clear_queryable_cache(session=None)`** closes all (or per-session)
  queryables — the symmetric partner of `clear_retention_cache`.
- **Reconnect** (managed sessions): `_restore_queryables` re-declares each
  handle against the new raw. Queryables hold no replayed state — just the
  handler closure — so a failed redeclare closes the handle (subscriber
  policy).

## Interaction with `RETAINED`

`on_query` on a `RETAINED = True` class raises `TopicError`: retention
already owns a cache-serving queryable over the same template wildcard, and
a second compute-serving queryable would answer the same `get` with
competing replies. A class is cache-backed *or* compute-backed, not both.

## What lives where

```
queryable/
├── __init__.py             # re-exports
├── queryable.py            # Queryable handle: _declare / _redeclare / close
├── _queryable_registry.py  # id(session) → set[Queryable] + clear_queryable_cache
├── _query_context.py       # QueryContext wrapper + reply/reply_err/reply_del
├── _query_dispatch.py      # per-query handler closure (sync + async)
└── _query_get.py           # the get side backing Cls.query() / iter_query()
```

The `on_query` / `query` / `query_one` / `iter_query` methods live on
`Message` via `_MessageQueryMixin` (`message/_message_query.py`); the async
façade (`aquery` / `aquery_one` / `aquery_iter` / `aon_query`) lives in
`async_.py`.
