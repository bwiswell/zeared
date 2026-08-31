# `meta.py`

## `ZenohMeta`

A `@z.zeared` dataclass carrying Zenoh sample metadata to two-argument
subscriber callbacks. Keeps Zenoh types out of user code.

```python
@z.zeared
class ZenohMeta(z.Zeared):
    key_expr:    str                      = z.Str(required=True)
    timestamp:   str | None               = z.Str(default=None)       # raw HLC string
    issued_at:   datetime.datetime | None = z.DateTime(default=None)  # parsed UTC
    encoding:    str | None               = z.Str(default=None)       # e.g. 'application/msgpack'
    source_info: str | None               = z.Str(default=None)       # stringified source id
    attachment:  bytes | None             = z.Bytes(default=None)
    schema:      str | None               = z.Str(default=None)       # publisher's class SCHEMA
    captures:    dict                     = z.Dict(default_factory=dict)
    origin:      Origin                   = z.Enum(enum=Origin, default=Origin.LIVE)
```

## `meta.captures`

Always populated (at minimum an empty dict) when the receiving class has
declared templates. Keys are the `{name}` slots from the matched template;
values are the raw string captures from the incoming key expression. The
intended use is routing-only identifiers that shouldn't live on the payload:

```python
@z.zeared
class CliRequest(z.Message):
    TOPIC = 'workload/cli/request/{corr_id}'   # corr_id is NOT a declared field
    cmd: str = z.Str(required=True)

def on_request(msg: CliRequest, meta: z.ZenohMeta):
    corr_id = meta.captures['corr_id']         # string from the key
    handle(msg.cmd, correlation=corr_id)
```

When a slot IS also a declared seared field (e.g. `TOPIC = 'robot/{id}/...'`
and `id: int = z.Int(...)`), the captured value is additionally coerced
through the field's `deserialize` and set on the instance — both `msg.id`
and `meta.captures['id']` are available.

## `meta.schema` and `meta.issued_at`

`schema` carries the publisher's class-level `SCHEMA` value, decoded
from the sample's attachment (zeared stamps it via msgpack-encoded
`{schema: <value>}` when the publisher class declared `SCHEMA`).
`None` when the publisher didn't stamp a schema.

Subscribers with their own `SCHEMA` set perform a wire-vs-local
comparison automatically; mismatches drop the sample and route via
`on_error` as `SchemaMismatchError`. See `docs/subscriber.md` for the
warn-once-per-(sender, schema) policy. Subscribers with `SCHEMA = None`
skip the check entirely; `meta.schema` still populates from the wire
for diagnostic use.

`issued_at` is parsed from the sample's HLC timestamp (`sample.timestamp`)
into a UTC `datetime.datetime`. Requires Zenoh timestamping to be
enabled; the factories (`z.peer()` / `z.client()`) inject
`timestamping/enabled=true` by default since 0.0.13. Returns `None` when
no timestamp is available (synthesised wills, raw publishes against a
session with timestamping disabled).

For users who need the raw HLC string (e.g. for cross-replay dedupe
ordering), `meta.timestamp` carries it unmodified. `meta.issued_at` is
the friendly form.

## `meta.origin` — the replay-vs-live signal

`Origin` (exported as `z.Origin`) is a `StrEnum` carrying the sample's
provenance — *how it reached this subscriber*:

| Value | Meaning |
|-------|---------|
| `Origin.LIVE`   | Delivered by the live zenoh subscriber — a real publish. |
| `Origin.REPLAY` | Delivered from a retention cache via retained-fetch (at subscribe time or after a reconnect). Cached state, not an event the subscriber witnessed. |
| `Origin.WILL`   | Synthesised by the presence observer when a peer's liveliness token disappeared. |

The value is determined entirely by the **local delivery path** — it is
never read from (or written to) the wire, so mixed-version fleets
interoperate untouched. Set by the dispatch layer; `from_sample`
defaults it to `LIVE`.

The canonical use is the sync-then-react pattern on `RETAINED` classes:

```python
def on_state(msg: TagState, meta: z.ZenohMeta):
    if meta.origin is z.Origin.REPLAY:
        seed(msg)         # initial state sync — don't act on it
    else:
        react(msg)        # live change (or a will)
```

Prefer this over the old `issued_at is None` heuristic for detecting
wills — that is also true for any sample from an unstamped session.

## `from_sample(sample: zenoh.Sample) -> ZenohMeta`

Internal helper: stringifies Zenoh's richer types (`Timestamp`, `Encoding`,
`SourceInfo`) and converts `sample.attachment` to `bytes | None`. Callable
at the module level so tests or advanced users can build a `ZenohMeta` from
a raw Zenoh sample if needed.

## When it's built

Only when the subscriber callback's arity indicates it will be used —
zeared calls `_wants_meta(cb)` once at subscribe time and skips the
`from_sample` allocation entirely for 1-arg callbacks.
