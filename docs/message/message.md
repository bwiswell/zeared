# `message.py`

`Message` is the base class every zeared message extends. It layers three
things on top of `seared.Seared`: a topic (possibly format-string), an
encoding, and the `send` / `on_message` methods that connect the class to a
Zenoh session.

## Declaring a message

```python
import zeared as z

@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    ENCODING = 'msgpack'            # optional; this is the default
    id: int   = z.Int(required=True)
    x: float  = z.Float(required=True)
    y: float  = z.Float(required=True)
```

Rules enforced lazily (on first template access):

- `TOPIC` must be a non-empty string.
- A `{name}` slot needn't be a declared field: an undeclared one is
  capture-only (see [Capture-only slots](#capture-only-slots)).
- Format specs (`{x:03d}`) and conversions (`{x!r}`) are rejected.
- Duplicate `{name}` entries are rejected.

## Publishing — `send(self, *, session=None, topic=None, retain=None)`

`send` resolves the session (kwarg > scope > default > raise), picks the
effective encoding (`z.debug` forces JSON), resolves `retain=` against
`RETAINED`, renders the concrete topic from the instance's slot fields (on
`TOPIC`, or on the declared template `topic=` names), strips those fields
from the payload, serializes the rest via `seared` and packs it through the
codec. The put goes through the class's publisher cache
([`publisher.md`](../publisher.md)): a cached `zenoh.Publisher` per
concrete key, or `session.put` when `PUBLISHER = False` or past the cap.
Either way it passes the class's `SCHEMA` attachment as `attachment=`. A
retained send also stores the value in the retention cache and stamps its
put with an explicit `timestamp=` from the session's clock (0.4.0). Inside
a `z.batch()` block the put is buffered instead (see
[`batch.md`](../batch.md)).

```python
Telemetry(id=7, x=1.0, y=2.0).send()                    # default session
Telemetry(id=8, x=0.0, y=0.0).send(session=external)    # one-off override
```

## Concrete topics — `concrete_topic` (0.3.8)

One canonical render builds every concrete key: `send`, `declare_presence`,
`QueryContext.reply`, `unretain` (both forms), the query getters'
selectors, and `concrete_topic` itself (`_message_render.py`). A slot's key
segment is `str(field.serialize(value))` for a value of the field's own
(exact) type, with no `format=` hint, so it's the field's JSON-carrier
string form whatever the class `ENCODING`, and exactly what a subscriber's
capture decodes back. Any other value renders as `str(value)`, so strings
pass through verbatim (selector wildcards, pre-rendered values). A slot
field with a `data_key` renders from its attribute, decodes under its wire
key, and is stripped from the payload by that wire key.

**A single-segment slot can't hold a `/`** (0.4.0), since it would split
the segment and the key would match no template:

- **The class check** (`_message_topic.py`, at the first `_templates()`
  call) raises `TopicError` for a single-segment slot bound to `Path`, or
  to `Bytes` in its default base64 form. `Bytes(encoding='hex')` and `Str`
  pass, and a trailing `{name**}` slot takes a path tail.
- **Publish renders** (`send`, `declare_presence`, `QueryContext.reply`,
  `unretain`, `concrete_topic`) raise `TopicError` for a single-segment
  value whose rendered form contains `/`. The query getters' selectors
  don't check, since they embed wildcards deliberately.

`concrete_topic` is a descriptor (`_message_concrete_topic.py`), dispatching
like `unretain`:

- `msg.concrete_topic(*, topic=None)` — the key `msg.send(topic=topic)`
  publishes on. A `None` slot raises `TopicError`, as `send` does.
- `Cls.concrete_topic(*, topic=None, **slots)` — the key those slot values
  name, without an instance. Every slot must be supplied (a missing or
  `None` slot raises `TopicError`; it never widens to a wildcard). Non-slot
  keyword arguments are ignored, so a full field dict can be passed.

```python
Telemetry(id=7, x=1.0, y=2.0).concrete_topic()   # 'robot/7/telemetry'
Telemetry.concrete_topic(id=7)                   # 'robot/7/telemetry'
```

`Template.render` / `Template.render_selector` stay plain `str.format`
renderers underneath; prefer `concrete_topic` over calling them through
`_templates()`.

## Subscribing — `on_message(cls, cb, *, ...)`

```python
Cls.on_message(
    cb, *,
    session=None, on_error=None,
    expected_interval=None, on_quiet=None, on_active=None, startup_grace=None,
    auto_reconnect=True, dedupe=None, on_remove=None, retained_fetch=True,
    on_online=None, on_offline=None,
)
```

| Keyword | |
|---------|---|
| `session` | the session to subscribe on (kwarg > scope > default) |
| `on_error` | `on_error(exc, raw)` for decode, schema and callback failures; logged when absent |
| `expected_interval` / `on_quiet` / `on_active` / `startup_grace` | the per-subscription watchdog; see [`watchdog.md`](../watchdog.md) |
| `auto_reconnect` | `False` leaves this subscriber out of the reconnect restore and the re-attach walk |
| `dedupe` | overrides the class's `DEDUPE` for this subscriber; `None` takes the class default |
| `on_remove` | the tombstone feed: `on_remove(meta)` for each DELETE sample, which `cb` never sees |
| `retained_fetch` | `False` makes a live-only subscriber: no retained replay at subscribe time, after a reconnect or on the re-attach walk |
| `on_online` / `on_offline` | the presence feed (`LIVELINESS = True` classes only); see [Presence](#presence-opt-in-via-liveliness--true) |

Returns a [`Subscriber`](../subscriber/subscriber.md) handle. The callback is inspected
once at subscribe time:

- `cb(msg)` — decoded message only.
- `cb(msg, meta)` — `meta` is a [`ZenohMeta`](../meta.md) built from the sample.

Template-TOPICs auto-derive a wildcard for the subscribe call, so
`Telemetry.on_message(cb)` sees *every* `robot/*/telemetry`.

## Class attributes

| Name | Type | Purpose |
|------|------|---------|
| `TOPIC` | `ClassVar[str]` | canonical static or format-string topic (used for publish by default) |
| `EXTRA_TOPICS` | `ClassVar[tuple[str, ...]]` | additional topic templates the class also subscribes to; each parses independently, with its own slot set |
| `ENCODING` | `ClassVar[Literal['msgpack', 'json']]` | wire format, default `'msgpack'` |
| `PUBLISHER` | `ClassVar[bool \| int]` | cache policy: `True` (default cap 256), `False` (disabled), `int` (explicit cap). See [`publisher.md`](../publisher.md). |
| `RETAINED` | `ClassVar[bool]` | opt in to MQTT-style retained-message emulation. Default `False`. See [`retention.md`](../retention.md). |
| `LIVELINESS` | `ClassVar[bool]` | opt in to presence (0.4.0): producers hold a key with `declare_presence()`, subscribers pass `on_online=` / `on_offline=` to `on_message`. Default `False`. See [`presence.md`](../presence/presence.md). |
| `DEDUPE` | `ClassVar[bool]` | for `RETAINED` classes, a per-key HLC watermark on every sample, live or replayed: one whose timestamp is `<=` the highest seen on its key is dropped. Default `True`. Samples with no timestamp bypass dedupe. Per-subscriber override: `Cls.on_message(cb, dedupe=False)`. |
| `DEDUPE_MAX_SKEW` | `ClassVar[float \| None]` | ceiling, in seconds, on how far ahead of the local clock a sample's HLC may be and still advance the dedupe watermark. A sample beyond it is still delivered; it just doesn't move the watermark. `None` disables the check. Default `300.0`. |
| `RETENTION_TTL` | `ClassVar[float \| None]` | for `RETAINED` classes, lazy expiry in seconds: entries older than the TTL are skipped and pruned when a retained fetch is served. Default `None` (no expiry). See [`retention.md`](../retention.md). |
| `REQUEST` | `ClassVar[type \| None]` | typed request-payload class for `query` / `on_query`. Default `None`: the handler sees the raw payload bytes on `ctx.request`. See [`queryable.md`](../queryable/queryable.md). |
| `EFFECT` | `ClassVar[Effect \| str \| None]` | doc-only: what acting on this contract does, as `z.Effect.READ` / `WRITE` / `ACTION` or its name. Never read at runtime; `zeared.doc` surfaces it and `z.effect_of` validates it. Default `None` (undeclared, never inferred). See [`effect.md`](../effect.md). (0.3.12) |
| `SCHEMA` | `ClassVar[Optional[str]]` | optional schema-version marker (e.g. `'1.0'`, `'a3f72b1'`). When set, the value rides as a Zenoh sample attachment (msgpack-encoded `{schema: <value>}`); subscribers compare against their own `SCHEMA` and route mismatches via `on_error` as `SchemaMismatchError`. Default `None` (no stamping, no validation). |

### `SCHEMA` semantics

```python
@z.zeared
class PeerStatus(z.Message):
    TOPIC = 'peer/{name}/status'
    SCHEMA = '1.0'                    # opt in to attachment-based schema stamping
    name:  str = z.Str(required=True)
    state: str = z.Str(required=True)
```

- **Publisher side:** zeared serialises `{schema: '1.0'}` as msgpack
  on first send; result is cached on the class (`_SCHEMA_ATTACHMENT_CACHE`)
  and reused for every subsequent publish — zero per-send cost beyond
  passing the cached bytes through to `session.put` / `Publisher.put`.
- **Subscriber side:** if the class declares `SCHEMA = <value>`, the
  dispatch closure checks the wire schema against it. Mismatch →
  drop the sample, route via `on_error` as `SchemaMismatchError(DecodeError)`.
  Each `(sender_zid, observed_schema)` pair fires `on_error` once; later
  mismatches from it drop silently. The warn-once cache is a bounded LRU
  (1024 pairs; an evicted pair warns again), cleared when the subscriber
  is re-declared after a reconnect. Since 0.4.0 the sender half is the
  zid in the sample's HLC timestamp (`''` for an unstamped sample).
- **No-schema subscribers** (`SCHEMA = None`, the default) skip the
  check entirely; `meta.schema` still populates from the wire if the
  publisher stamped one.
- `meta.issued_at` is parsed from the sample's HLC timestamp into a
  UTC `datetime.datetime` — independent of `SCHEMA`, no opt-in
  required (timestamping is auto-enabled by `z.peer()` / `z.client()`
  since 0.0.13).

### Unknown keys stay tolerated

A Message ignores a payload key it doesn't declare, which is what lets a
subscriber read a publisher one version ahead that has added a field. Since
0.4.2 a class can refuse them instead (seared's `unknown=`, which
`@z.zeared(unknown='raise')` takes), but on the wire that costs the
tolerance: every sample from a newer publisher fails to decode, and goes
to `on_error` as a `DecodeError`. Leave it for a closed system that wants
exactly that. To check one payload without refusing it, as for input from
a person or an agent, call `s.unknown_keys(Cls, data)`.

## Multiple topics per class

Declare `EXTRA_TOPICS` to subscribe to a union of templates with one
class. Publish still defaults to `TOPIC`; a restrictive `send(topic=...)`
override picks a non-canonical declared template:

```python
@z.zeared
class Status(z.Message):
    TOPIC = 'robot/{id}/status'
    EXTRA_TOPICS = ('vehicle/{id}/status',)
    id: int = z.Int(required=True)
    status: str = z.Str(required=True)

Status(id=1, status='ok').send()                             # → robot/1/status
Status(id=1, status='ok').send(topic='vehicle/{id}/status')  # → vehicle/1/status
Status.on_message(cb)    # subscribes to robot/*/status AND vehicle/*/status
```

Arbitrary strings passed to `send(topic=...)` raise `TopicError`. Each
declared template has an independent slot set — one can capture `{id}`,
another `{name}`, a third `**`-only — all fine. Slots that happen to share
names with declared seared fields get coerced onto the instance; slots
without a matching field are capture-only (see `meta.captures`).

## Subscribe-only wildcards (`**`)

Any template whose trailing path segment is an *anonymous* `**` is
**subscribe-only**: it matches one or more concrete segments on receive
but can't be rendered for publish. `send()` on a non-publishable
canonical — or `send(topic='robot/**')` picking a subscribe-only extra —
raises `TopicError`. A named `{name**}` slot is publishable, and so is an
anonymous `*` segment (see
[`_template.md`](../_template/_template.md#publishability)).

```python
@z.zeared
class AnyEvent(z.Message):
    TOPIC = 'peer/{name}/event'               # canonical (publishable)
    EXTRA_TOPICS = ('peer/**',)               # subscribe-only wildcard
    name: str = z.Str(required=True)
    payload: dict = z.Dict(default_factory=dict)

AnyEvent(name='alice', payload={'x': 1}).send()   # publishes canonical
# Subscriber receives BOTH peer/alice/event (typed path) AND any
# peer/foo/bar/... messages other producers send (wildcard path).
```

## Capture-only slots

Template slots **do not** need to be declared as seared fields. Slots that
aren't declared become capture-only — they're surfaced on `meta.captures`
(always populated, indexed by slot name, string-valued) but don't appear
as attributes on the decoded instance. This is how routing-only
identifiers (correlation IDs, trace UUIDs, stream names) stay off the
payload schema. See [`meta.md`](../meta.md) for examples.

`msg.send()` renders the key from the instance's fields, so it raises
`TopicError` (missing field) on a template with a capture-only slot.

## Retention (opt-in via `RETAINED = True`)

```python
@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    RETAINED = True
    id: int   = z.Int(required=True)
    x: float  = z.Float(required=True)
    y: float  = z.Float(required=True)

Telemetry(id=1, x=1.0, y=2.0).send()                    # retained by default
Telemetry(id=1, x=1.0, y=2.0).send(retain=False)        # live-only override
Telemetry(id=1, x=0.0, y=0.0).unretain()                # instance tombstone
Telemetry.unretain(id=1)                                # class tombstone
```

- `send(retain=True)` on a `RETAINED = False` class raises `TopicError`.
- `send(retain=False)` on a `RETAINED = True` class publishes without updating the cache.
- `unretain()` and `unretain(**key_fields)` both require `RETAINED = True`.
- Subscribers on a `RETAINED = True` class fetch cached values from peers with a `session.get(wildcard)` per declared template, issued right after the live subscribers are declared and drained before `on_message` returns. Replies arrive as `Origin.REPLAY`. The fetch re-runs after a reconnect and on the re-attach walk when a managed session's link comes back (0.4.0); `retained_fetch=False` opts out of all three.
- DELETE samples (tombstones) never reach `cb`: they go to `on_remove` when one is given, else they are dropped.

See [`retention.md`](../retention.md).

## Presence (opt-in via `LIVELINESS = True`)

```python
# PeerStatus as above, plus LIVELINESS = True.
# Producer — hold this key while the session lives (0.4.0).
token = PeerStatus.declare_presence(name='alice')       # or PeerStatus(...).declare_presence()
token.undeclare()                                         # withdraw it early, session alive

# Subscriber — the presence feed, separate from cb.
PeerStatus.on_message(handler, on_online=online, on_offline=offline)
```

- Requires `LIVELINESS = True` on the class (raises `TopicError` otherwise).
- A Zenoh liveliness token at the concrete contract key; one per key per
  session. Closing, crashing or releasing the session withdraws them all.
- `on_online` / `on_offline` get an identity-only `ZenohMeta` (the key's
  captures; `origin` `REPLAY` / `LIVE` for online, `WILL` for offline).
  `cb` never sees them; `on_remove` stays the tombstone feed.
- No auto-transition of retained state — retention and presence are orthogonal.
- `register_will` / `aregister_will` were removed in 0.4.0; reading them
  raises `AttributeError` pointing here.

See [`presence.md`](../presence/presence.md).

## Async counterparts

Each sync method has an `a`-prefixed sibling, on the message or as a
`z.` function:

| Sync | Async |
|------|-------|
| `msg.send(...)` | `await msg.asend(...)` |
| `Cls.send_batch(...)` | `await Cls.asend_batch(...)` |
| `msg.unretain(...)` | `await msg.aunretain(...)` |
| `Cls.unretain(**)` | `await z.aunretain(Cls, **)` |
| `msg.declare_presence(...)` / `Cls.declare_presence(**)` | `await msg.adeclare_presence(...)` / `await Cls.adeclare_presence(**)` |
| `Cls.on_message(cb, ...)` | `async for msg in Cls.alisten(...):` |
| `Cls.fetch_retained(...)` | `await z.afetch_retained(Cls, ...)` |
| `Cls.query(...)` / `Cls.query_one(...)` | `await z.aquery(Cls, ...)` / `await z.aquery_one(Cls, ...)` |
| `Cls.iter_query(...)` | `async for msg in z.aquery_iter(Cls, ...):` |
| `Cls.on_query(handler, ...)` | `await z.aon_query(Cls, handler, ...)` |

See [`async_.md`](../async_.md).

## Bulk sending — `send_batch(cls, items, *, session=None, topic=None, retain=None)`

Classmethod sugar for a homogeneous batch. Wraps the loop in `z.batch()`,
propagates `session=` / `topic=` / `retain=` to every message, and fails
fast on any element that isn't an instance of `cls`:

```python
Telemetry.send_batch(
    [Telemetry(id=i, x=float(i), y=0.0) for i in range(1000)],
    session=external,
)
```

## Internal helpers

- `_templates()` — parses `TOPIC` + `EXTRA_TOPICS` into a `Templates` once (running the slot field-binding check), cached on the class.
- `_field_specs()` — attribute name → seared `FieldSpec` map, cached on the class (0.3.8); the render and capture coercion look slot fields up through it.
- `_decode(raw, key_expr, encoding)` — reconstruct an instance from bytes + key expression; coerces template captures (the same coercion as `coerce_captures`) and merges them into the payload under each field's wire key before calling `cls.load`. A capture overrides a payload value for the same field.
