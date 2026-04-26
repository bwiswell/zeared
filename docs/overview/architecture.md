# Architecture

zeared is a thin layer over `zenoh.Session` that turns `@z.zeared` classes
into typed pub/sub messages. Three crosscutting concerns hold the design
together: session resolution, the TOPIC template lifecycle, and the encoding
layer.

The entire `seared` public surface is re-exported from `zeared` at import
time — ``import zeared as z`` is enough to use everything. When this doc
refers to `@s.seared`, `s.Int`, etc., those are identically usable as
`@z.zeared`, `z.Int`.

## Session resolution

Every operation that touches a Zenoh session (`Message.send`,
`Message.on_message`) resolves the session at call time in fixed precedence:

1. An explicit `session=` kwarg passed to the call.
2. The innermost active `with z.session(sess):` scope (thread-local).
3. The module-level default set via `z.session = sess`.
4. Otherwise — `NoSessionError`.

This lets a single node own two independent Zenoh networks without global
state mutation: set the common network as the default, scope or pass the
other for traffic that belongs there.

`zeared.session` is deliberately a single object — a `_SessionHandle` — that
never gets replaced. Assignment is intercepted at the module level
(`_ZearedModule.__setattr__`) and redirected to the handle's internal default.
Reads (`z.session.current`) see through the handle; calls
(`z.session(other)`) produce a thread-local scope context manager.

See [`_session.md`](../_session.md).

## TOPIC template lifecycle

A TOPIC can be a plain string (`'events/alerts'`) or a format string
(`'robot/{id}/telemetry'`). In either case, the first access parses the
template on the class and caches three derived forms:

| Form | Derived | Used by |
|------|---------|---------|
| `raw` | the user string | reference / errors |
| `field_names` | tuple of `{name}` slots in order | publisher render, receiver coerce |
| `wildcard` | `{name}` → `*` | `session.declare_subscriber` |
| `_regex` | `{name}` → `(?P<name>[^/]+)` | match incoming `key_expr` |

At publish: the sender's field values fill the template, producing the
concrete key expression. Template fields are stripped from the payload —
they live in the key — so the wire carries only what isn't already in the
topic.

At receive: Zenoh delivers a `Sample` with a concrete `key_expr`. zeared
matches it against the regex, coerces the captured strings through each
field's `deserialize`, merges them into the decoded payload dict, and
finally calls `cls.load(merged)`. The decoded message looks exactly like
one constructed directly.

See [`_template.md`](../_template.md), [`message.md`](../message.md).

## Encoding layer

Messages carry `ENCODING: ClassVar[Literal['msgpack', 'json']] = 'msgpack'`.
The effective encoding for any send is derived from two inputs:

```
effective = 'json' if zeared.debug else cls.ENCODING
```

Subscribers prefer the wire-declared encoding hint on incoming samples
(Zenoh's `encoding` attribute) when it parses as msgpack or JSON, so
heterogeneous fleets work: a JSON-speaking sender and msgpack-speaking
sender can share a topic and each be decoded correctly. Debug-mode
subscribers still honour sender-declared encodings — the flag only flips
**outbound** traffic to JSON.

See [`_codec.md`](../_codec.md).

## What lives where

```
zeared/
├── __init__.py     # public re-exports, module-class swap, session factories
├── _codec.py       # pack/unpack dispatch on 'msgpack' / 'json'
├── _session.py     # _SessionHandle (dual-role attribute)
├── _template.py    # Template + Templates (canonical + extras)
├── async_.py       # asend / alisten / abatch / apeer / aclient
├── batch.py        # z.batch() context manager, ContextVar-backed buffer stack
├── errors.py       # ZearedError + NoSessionError / TopicError / SubscriptionError
├── message.py      # Message base class (sync + async methods)
├── meta.py         # ZenohMeta seared dataclass + from_sample helper
├── presence.py     # _SessionPresence + _PresenceObserver + liveliness / LWT
├── publisher.py    # _PublisherCache + module-level registry
├── retention.py    # _RetentionCache + Queryable + module-level registry
└── subscriber.py   # Subscriber handle with arity + coroutine + presence dispatch
```

`msg.send()` routes through the `_PublisherCache` for the sending message's
class by default (`PUBLISHER = True`) — long-lived `zenoh.Publisher`
instances are cached per concrete topic. Opt out with `PUBLISHER = False`
to use `session.put()` directly; pass an integer for a custom cache cap.
See [`publisher.md`](../publisher.md) and [`batch.md`](../batch.md).
