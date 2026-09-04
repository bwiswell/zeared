# `queryable/`

First-class Zenoh query/reply (request/response) for `@z.zeared` message
classes. Two sides:

- **Serving** — `Cls.on_query(handler)` declares a `zenoh.Queryable` that
  answers peer `session.get()` requests with *computed* message instances.
  Returns a `Queryable` handle.
- **Getting** — `Cls.query(**key_fields, ...)` issues a `session.get()`,
  decodes each reply through the class's own `_decode`, and returns typed
  instances. `Cls.query_one(...)` returns the first (or `None`).

This is the compute-serving sibling of `RETAINED` (which serves a *cached*
value via its own internal queryable) and the request/response sibling of
`Subscriber`. The message class's declared `TOPIC` template is both the
queryable key and the source of reply captures; its seared fields are the
**response** body — serialized on the wire exactly like `send()`.

## Serving: `on_query`

```python
@z.zeared
class TagState(z.Message):
    TOPIC = 'rio/state/tag/{epc}'
    epc: str          = z.Str(required=True)   # ← key capture
    x: float          = z.Float(required=True) # ─┐ response body
    y: float          = z.Float(required=True)  #─┘

def handler(ctx: z.QueryContext) -> TagState | list[TagState] | None:
    epc = ctx.captures['epc']
    x, y = lookup(epc)
    return TagState(epc=epc, x=x, y=y)

qbl = TagState.on_query(handler)   # z.Queryable; .close() / context manager
```

The handler receives a `QueryContext` and may either **return** a message
instance, an iterable of them, or `None`; or reply explicitly via
`ctx.reply(...)` / `ctx.reply_err(...)` / `ctx.reply_del(...)` (for
multi-reply, streaming, or error cases) and return `None`. Reply failures
are caught and routed to `on_error=` (or logged) — never propagated to
Zenoh.

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
| `captures` | template-slot values from `key_expr` (empty on wildcard) |
| `request` | decoded `REQUEST` instance, else raw payload bytes / `None` |

### Async handlers

A **generator** handler is the streaming form of the return form: each
yielded instance is replied as it is produced, so the handler never
materialises the full result set. This is the server side of a multi-reply
query — memory stays O(1) in the number of replies.

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
  This is the one place the `alisten` analogy breaks: that iterator's
  cleanup undeclares a real subscriber and genuinely stops the work.

`query_one` rides on the same path and short-circuits at the first decoded
reply instead of waiting out `timeout`. It therefore sees only the errors
that arrive before that reply; use `query` when you need all of them.

`async def` handlers are supported — register via `z.aon_query(Cls,
handler)` (or `Cls.on_query` from within a running loop). The loop is
captured at declare time; the query stays live until the coroutine
resolves, then its return value is replied.

## Getting: `query` / `query_one`

```python
# Concrete — one key.
states = TagState.query(epc='E280 1234', timeout=2.0)

# Wildcard — omitted slots widen to '*', so this asks the whole template.
all_states = TagState.query(timeout=2.0)

# Partial wildcard — embed '*' in a value.
some = TagState.query(epc='E280*', timeout=2.0)

# First reply (or None).
one = TagState.query_one(epc='E280 1234', timeout=2.0)
```

`query()` blocks up to `timeout`. It returns only successfully decoded OK
replies; error replies (`reply_err`) and per-reply decode failures route to
`on_error(exc, raw)` when supplied, else log — each surfaced as a
`z.QueryError`.

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
class LocReq(z.Message):
    TOPIC = 'rio/req/loc'      # unused for routing; carried as payload
    algo: str = z.Str(default='trilat')

@z.zeared
class TagState(z.Message):
    TOPIC = 'rio/state/tag/{epc}'
    REQUEST = LocReq
    ...

TagState.query(epc='E280*', request=LocReq(algo='ml'))
# handler sees ctx.request == LocReq(algo='ml')
```

Without `REQUEST`, `ctx.request` is the raw payload bytes (or `None`). Use
`params=` for lightweight scalar filters (`?k=v`) that don't warrant a
typed body.

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

`async def` **generator** handlers are supported on the same footing
(0.3.2) and are drained with `async for`. They need a running loop at
declare time exactly as coroutine handlers do — `Queryable._declare` tests
`iscoroutinefunction` **or** `isasyncgenfunction`, since the former alone
is `False` for an async generator function and used to route one down the
sync path, where it replied nothing at all.

Errors never reach Zenoh's callback. A handler that raises routes through
`on_error` / logging and sends a best-effort `reply_err` — including a
generator raising mid-stream, which surfaces only as the iterator is
advanced, long after `handler(ctx)` itself returned cleanly.

`on_query` on a `RETAINED = True` class raises `TopicError`: retention
already owns a cache-serving queryable over the same template wildcard, and
a second compute-serving queryable would answer the same `get` with
competing replies. A class is cache-backed *or* compute-backed, not both.

## What lives where

```
queryable/
├── __init__.py             # exports + registry + clear_queryable_cache
├── queryable.py            # Queryable handle: _declare / _redeclare / close
├── _queryable_registry.py  # id(session) → set[Queryable]
├── _query_context.py       # QueryContext wrapper + reply/reply_err/reply_del
├── _query_dispatch.py      # per-query handler closure (sync + async)
└── _query_get.py           # the get side backing Cls.query()
```

The `on_query` / `query` / `query_one` methods live on `Message` via
`_MessageQueryMixin` (`message/_message_query.py`); the async façade
(`aquery` / `aquery_one` / `aon_query`) lives in `async_.py`.
