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
2. The innermost active `with z.session(sess):` scope (context-local: per
   thread and per asyncio task, and followed into `asyncio.to_thread`).
3. The module-level default set via `z.session = sess`.
4. Otherwise — `NoSessionError`.

This lets a single node own two independent Zenoh networks without global
state mutation: set the common network as the default, scope or pass the
other for traffic that belongs there.

`zeared.session` is deliberately a single object — a `_SessionHandle` — that
never gets replaced. Assignment is intercepted at the module level
(`_ZearedModule.__setattr__`) and redirected to the handle's internal default.
Reads (`z.session.current`) see through the handle; calls
(`z.session(other)`) produce a context-local scope context manager.

See [`_session.md`](../_session.md).

## TOPIC template lifecycle

A TOPIC can be a plain string (`'events/alerts'`) or a format string
(`'robot/{id}/telemetry'`). In either case, the first access parses the
template on the class and caches four derived forms:

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

See [`_template.md`](../_template/_template.md), [`message.md`](../message/message.md).

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

## Source layout convention ("Pattern B")

Most of the tree is flat modules. When one grows past what reads
comfortably in a single file, it becomes a **subdirectory** instead:

- A **primary file** named after the package (`message/message.py`,
  `_managed_session/_managed_session.py`) holding the class itself —
  declared attributes and anything that can't be lifted out.
- **Sibling modules** holding extracted method clusters, one concern per
  file (`_message_send.py`, `_zenoh_api_mixin.py`).
- An `__init__.py` that re-exports the public names, so the import path
  never changes when a module is split.

Extracted clusters stay **methods**, via mixins composed onto the primary
class through the MRO — not helper functions taking `self` or `cls`. The
mixins hold no state (`__slots__ = ()`) and are never instantiated alone;
they exist to keep `msg.send()` a method while letting `send` live in its
own file. Where a mixin reaches for attributes the composed class owns,
that contract is declared explicitly for the type checker rather than left
implicit — see `_managed_session/_helpers.py`.

Tests mirror this exactly: one `test_*.py` per source file, in a matching
subdirectory.

Subdirs following this shape: `message/`, `presence/`, `queryable/`,
`subscriber/`, `_managed_session/`, `_reconnect/`, `_template/`.

## What lives where

```
zeared/
├── __init__.py                        # public re-exports (seared's surface included), the module-class swap
├── _codec.py                          # pack/unpack dispatch on 'msgpack' / 'json', MIME map
├── _factories.py                      # peer / client / hub / open + retry, config-build and managed-wrap helpers
├── _managed_session/                  # ManagedSession: the reconnect-aware session wrapper
│   ├── _managed_session.py            # ManagedSession: state, raw swap, teardown
│   ├── _callback_handle.py            # CallbackHandle, the cancel handle (OnReconnectHandle alias)
│   ├── _helpers.py                    # resolve_raw, the dead-session check, the process-wide WeakSet
│   ├── _on_reconnect_mixin.py         # on_reconnect / on_link_lost / on_link_restored registries + driver
│   └── _zenoh_api_mixin.py            # zenoh.Session pass-throughs, guarded while reconnecting / dead
├── _mode.py                           # Mode enum: PEER / CLIENT / ROUTER
├── _module_class.py                   # _ZearedModule: turns `zeared.session = sess` into a default update
├── _prefix_index.py                   # trie of topic segments behind retention's query matching
├── _reconnect/                        # reconnect orchestration for ManagedSession
│   ├── _reconnect.py                  # probe loop, reconnect worker, trigger, the reconnect itself
│   ├── _link.py                       # router-link detection, link signals, republish, re-attach walk
│   └── _restore.py                    # post-reopen restore walks + the cancellable backoff
├── _release.py                        # release / release_all
├── _session.py                        # _SessionHandle (dual-role attribute) + context-local scopes
├── _template/                         # TOPIC format-string parsing
│   ├── _template.py                   # Template: one parsed TOPIC
│   └── _templates.py                  # Templates: canonical TOPIC + EXTRA_TOPICS
├── async_.py                          # async façade: apeer / aclient / aopen, asend, alisten, aquery, abatch, ...
├── batch.py                           # z.batch() context manager, ContextVar-backed buffer stack
├── config.py                          # SessionConfig: the declarative connection spec
├── doc/                               # wire-aware docs and TypeScript types for message classes
│   ├── __main__.py                    # `python -m zeared.doc`
│   ├── generate.py                    # doc generation over seared's pipeline + its CLI
│   ├── introspect.py                  # MessageDoc: wire-aware introspection
│   ├── render.py                      # Markdown rendering of a MessageDoc
│   └── typescript.py                  # TypeScript interfaces (`python -m zeared.doc.typescript`)
├── effect.py                          # Effect (READ < WRITE < ACTION) + effect_of: the doc-only EFFECT vocabulary
├── errors.py                          # ZearedError and its subclasses
├── hubd.py                            # `python -m zeared.hubd`: the relay hub daemon
├── message/                           # Message base class, one mixin per concern
│   ├── message.py                     # Message: class attributes, decode, mixin composition
│   ├── _message_async.py              # asend / asend_batch / aunretain / alisten
│   ├── _message_concrete_topic.py     # concrete_topic: instance-vs-class dispatch
│   ├── _message_presence.py           # declare_presence / adeclare_presence
│   ├── _message_query.py              # on_query / query / iter_query / query_one
│   ├── _message_render.py             # canonical topic render: slot values become key segments
│   ├── _message_send.py               # send / send_batch
│   ├── _message_subscribe.py          # on_message / fetch_retained / published_topics
│   ├── _message_topic.py              # template parsing + slot-binding checks, schema attachment cache
│   └── _message_unretain.py           # unretain: instance-vs-class dispatch
├── meta.py                            # ZenohMeta + Origin + from_sample
├── presence/                          # liveliness tokens at contract keys (0.4.0)
│   ├── presence.py                    # PresenceToken
│   └── _presence_session.py           # _SessionPresence: the tokens one session holds
├── publisher.py                       # _PublisherCache + module-level registry
├── queryable/                         # on_query serving + query getting (request/response)
│   ├── queryable.py                   # Queryable handle
│   ├── _query_context.py              # QueryContext, the Zenoh-free view a handler gets
│   ├── _query_dispatch.py             # the per-query handler closure
│   ├── _query_get.py                  # get side: issue, decode, collect replies
│   └── _queryable_registry.py         # registry keyed on id(session)
├── retention.py                       # _RetentionCache + its queryables + module-level registry
├── stream.py                          # Stream: the async iterator alisten (bounded, drop-oldest) and aquery_iter return
├── subscriber/                        # Subscriber handle and per-session registry
│   ├── subscriber.py                  # Subscriber: declare, redeclare, replay, close
│   ├── _subscriber_dispatch.py        # per-sample closure: dedupe, schema check, decode, presence events
│   ├── _subscriber_registry.py        # registry keyed on id(session)
│   └── _subscriber_retained_fetch.py  # the retained fetch (subscribe time, reconnect, re-attach)
└── watchdog.py                        # per-subscription freshness watchdog
```

Each subdirectory also has an `__init__.py` that re-exports its names.
The module pages under `docs/` follow the same layout; the `doc/`
subpackage has one page, [`doc/doc.md`](../doc/doc.md).

`msg.send()` routes through the `_PublisherCache` for the sending message's
class by default (`PUBLISHER = True`) — long-lived `zenoh.Publisher`
instances are cached per concrete topic. Opt out with `PUBLISHER = False`
to use `session.put()` directly; pass an integer for a custom cache cap.
See [`publisher.md`](../publisher.md) and [`batch.md`](../batch.md).

## Query / queryable (request/response)

`Cls.on_query(handler)` declares a `zenoh.Queryable` per template that
answers peer `session.get()` requests with computed message instances;
`Cls.query(...)` / `Cls.query_one(...)` issue the get and decode replies
through the class's own `_decode`. This is the compute-serving
generalization of `RETAINED` (which serves a *cached* value via its own
internal queryable) — the two are mutually exclusive on one class. Reply
decode reuses the same template-capture + payload path as live samples.
See [`queryable/queryable.md`](../queryable/queryable.md).
