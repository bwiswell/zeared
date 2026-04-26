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
- Every `{name}` in TOPIC must also be declared as a `@z.zeared` field on the
  class (violation → `TopicError`).
- Format specs (`{x:03d}`) and conversions (`{x!r}`) are rejected.
- Duplicate `{name}` entries are rejected.

## Publishing — `send(self, *, session=None)`

`send` resolves the session (kwarg > scope > default > raise), picks the
effective encoding (`z.debug` forces JSON), renders the concrete topic from
the instance's template fields, strips those fields from the payload,
serializes the rest via `seared`, packs through the codec, and calls
`session.put(key_expr, payload, encoding=...)`.

```python
Telemetry(id=7, x=1.0, y=2.0).send()                    # default session
Telemetry(id=8, x=0.0, y=0.0).send(session=external)    # one-off override
```

## Subscribing — `on_message(cls, cb, *, session=None, on_error=None)`

Returns a [`Subscriber`](subscriber.md) handle. The callback is inspected
once at subscribe time:

- `cb(msg)` — decoded message only.
- `cb(msg, meta)` — `meta` is a [`ZenohMeta`](meta.md) built from the sample.

Template-TOPICs auto-derive a wildcard for the subscribe call, so
`Telemetry.on_message(cb)` sees *every* `robot/*/telemetry`.

## Class attributes

| Name | Type | Purpose |
|------|------|---------|
| `TOPIC` | `ClassVar[str]` | canonical static or format-string topic (used for publish by default) |
| `EXTRA_TOPICS` | `ClassVar[tuple[str, ...]]` | additional topic templates the class also subscribes to; all must share the canonical's slot set |
| `ENCODING` | `ClassVar[Literal['msgpack', 'json']]` | wire format, default `'msgpack'` |
| `PUBLISHER` | `ClassVar[bool \| int]` | cache policy: `True` (default cap 256), `False` (disabled), `int` (explicit cap). See [`publisher.md`](publisher.md). |
| `RETAINED` | `ClassVar[bool]` | opt in to MQTT-style retained-message emulation. Default `False`. See [`retention.md`](retention.md). |
| `LIVELINESS` | `ClassVar[bool]` | opt in to presence / LWT — producers can `register_will()`, subscribers receive the will as a synthesised sample on peer death. Default `False`. See [`presence.md`](presence.md). |
| `DEDUPE` | `ClassVar[bool]` | for `RETAINED` classes, dedupe retention-fetch replies against live publishes via `(key_expr, timestamp)`. Default `True`. Synthesised will samples (timestamp `None`) bypass dedupe. Per-subscriber override: `Cls.on_message(cb, dedupe=False)`. |
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
  Same `(sender_zid, observed_schema)` pair won't fire `on_error` again
  until the subscriber closes (warn-once cache, cleared on `close()`).
- **No-schema subscribers** (`SCHEMA = None`, the default) skip the
  check entirely; `meta.schema` still populates from the wire if the
  publisher stamped one.
- `meta.issued_at` is parsed from the sample's HLC timestamp into a
  UTC `datetime.datetime` — independent of `SCHEMA`, no opt-in
  required (timestamping is auto-enabled by `z.peer()` / `z.client()`
  since 0.0.13).

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
Status.on_message(cb)    # subscribes to robot/+/status AND vehicle/+/status
```

Arbitrary strings passed to `send(topic=...)` raise `TopicError`. Each
declared template has an independent slot set — one can capture `{id}`,
another `{name}`, a third `**`-only — all fine. Slots that happen to share
names with declared seared fields get coerced onto the instance; slots
without a matching field are capture-only (see `meta.captures`).

## Subscribe-only wildcards (`**`)

Any template containing `**` as its trailing path segment is
**subscribe-only**: it matches one or more concrete segments on receive
but can't be rendered for publish. `send()` on a non-publishable
canonical — or `send(topic='robot/**')` picking a subscribe-only extra —
raises `TopicError`.

```python
@z.zeared
class AnyEvent(z.Message):
    TOPIC = 'peer/{name}/event'               # canonical (publishable)
    EXTRA_TOPICS = ('peer/**',)               # subscribe-only wildcard
    name: str = z.Str(required=True)
    payload: dict = z.Dict()

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
payload schema. See [`meta.md`](meta.md) for examples.

## Retention (opt-in via `RETAINED = True`)

```python
Telemetry(id=1, x=1.0, y=2.0).send()                    # retained by default
Telemetry(id=1, x=1.0, y=2.0).send(retain=False)        # live-only override
Telemetry(id=1, x=0.0, y=0.0).unretain()                # instance tombstone
Telemetry.unretain(id=1)                                # class tombstone
```

- `send(retain=True)` on a `RETAINED = False` class raises `TopicError`.
- `send(retain=False)` on a `RETAINED = True` class publishes without updating the cache.
- `unretain()` and `unretain(**key_fields)` both require `RETAINED = True`.
- Subscribers on a `RETAINED = True` class receive cached values from peers at subscribe time via `session.get(wildcard)` before live samples start flowing. DELETE samples (tombstones) are silently skipped.

See [`retention.md`](retention.md).

## Presence / LWT (opt-in via `LIVELINESS = True`)

```python
# Producer — stage a will payload for this session.
PeerStatus(name='alice', state='offline').register_will()

# Subscribers with LIVELINESS = True automatically observe presence,
# synthesising the will as a regular dispatched sample when the
# producing session's liveliness token disappears.
PeerStatus.on_message(handler)
```

- Requires `LIVELINESS = True` on the class (raises `TopicError` otherwise).
- Per-session primitive — one liveliness token per session covers N wills that fan out on death.
- Graceful `session.close()` fires the will identically to a crash.
- No auto-transition of retained state — retention and presence are orthogonal.
- Non-subscribers never observe offline events.

See [`presence.md`](presence.md).

## Async counterparts

Each sync method has an `a`-prefixed sibling:

| Sync | Async |
|------|-------|
| `msg.send(...)` | `await msg.asend(...)` |
| `Cls.send_batch(...)` | `await Cls.asend_batch(...)` |
| `msg.unretain(...)` | `await msg.aunretain(...)` |
| `Cls.unretain(**)` | `await z.aunretain(Cls, **)` |
| `msg.register_will(...)` | `await msg.aregister_will(...)` |
| `Cls.on_message(cb, ...)` | `async for msg in Cls.alisten(...):` |

See [`async_.md`](async_.md).

## Bulk sending — `send_batch(cls, items, *, session=None)`

Classmethod sugar for a homogeneous batch. Wraps the loop in `z.batch()`,
propagates `session=` to every message, and fails fast on any element that
isn't an instance of `cls`:

```python
Telemetry.send_batch(
    [Telemetry(id=i, x=float(i), y=0.0) for i in range(1000)],
    session=external,
)
```

## Internal helpers

- `_template()` — lazy template parser, cached on the class.
- `_decode(raw, key_expr, encoding)` — reconstruct an instance from bytes + key expression; merges template captures into the payload before calling `cls.load`.
