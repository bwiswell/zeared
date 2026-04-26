# `retention.py`

MQTT-style retained messages on Zenoh. Each publisher of a retained-topic
class keeps a local cache of the last payload per concrete topic and
declares a Zenoh `Queryable` on its wildcard(s). Subscribers on a retained
class automatically call `session.get(wildcard)` at subscribe time and
pipe reply samples through the same dispatch path as live messages.

## User surface

```python
@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    RETAINED = True                         # opt in
    id: int   = z.Int(required=True)
    x: float  = z.Float(required=True)
    y: float  = z.Float(required=True)

# Publish (default retain=True on RETAINED class)
Telemetry(id=1, x=1.0, y=2.0).send()
# Publish live-only (bypasses retention cache)
Telemetry(id=1, x=3.0, y=4.0).send(retain=False)

# Drop retained value + emit DELETE sample (tombstone)
Telemetry(id=1, x=0.0, y=0.0).unretain()       # instance form
Telemetry.unretain(id=1)                       # class form

# Subscribe — gets cached values from peers at subscribe time, plus live
Telemetry.on_message(cb)
async for msg in Telemetry.alisten():
    ...
```

## `_RetentionCache`

One per `(Message subclass, zenoh.Session)` pair. Holds:

- `_cache: dict[str, tuple[bytes, str]]` — concrete topic → (raw payload, encoding).
- `_index: _PrefixIndex` — trie of cached concrete topics. Updated in lockstep
  with `_cache`; replaces the prior O(N) iterate-and-intersect loop in
  `_handle_query`. See [`_prefix_index.md`](_prefix_index.md).
- `_queryables: list[zenoh.Queryable]` — one per declared template wildcard, declared lazily on first `store()`.

`store(topic, raw, encoding)` updates the cache + index under a per-cache
lock and ensures queryables are declared. `delete(topic)` drops the entry
from both. `_handle_query(query)` walks the trie to enumerate matching
concrete topics in O(depth × matches), then `query.reply()`s each.

Rolls back partial queryable declarations on failure. Registry keyed on
`(cls, id(session))` since `zenoh.Session` doesn't support weakrefs.

### Reconnect rebuild

When the cache is bound to a `ManagedSession` and a reconnect fires,
`_RetentionCache._redeclare_queryables()` undeclares the dead handles
(best-effort) and re-declares fresh queryables against the (now-current)
raw. Cache content (`_cache`, `_index`) is preserved verbatim — only the
live Zenoh queryable handles change.

The retention restoration step runs **before** subscriber redeclare in
the reconnect pipeline (`_restore_retention` → `_restore_subscribers`
→ `_restore_wills`). This is the **retention-first ordering invariant**:
a same-process publisher + subscriber pair both bound to the same
managed session needs the publisher-side queryable live before the
subscriber's reconnect-triggered retained-fetch fires. Pinned by
`tests/test_reconnect.py::TestRetentionRebuild::test_same_process_retained_fetch_after_reconnect`.

## `effective_retain(cls, arg)`

Resolves `send(retain=...)` against `cls.RETAINED`:

| Class `RETAINED` | `retain=` arg | Result |
|------------------|---------------|--------|
| `False` | `None` | `False` (pass-through) |
| `False` | `True`  | `TopicError` raised |
| `False` | `False` | `False` |
| `True`  | `None` | `True` (class default) |
| `True`  | `True`  | `True` |
| `True`  | `False` | `False` (live-only) |

## Tombstones

`msg.unretain()` / `Cls.unretain(**key_fields)` drops the local cache
entry and calls `session.delete(concrete_topic)`. Subscribers see the
DELETE via `sample.kind == zenoh.SampleKind.DELETE` and silently skip
(no callback fires). Late subscribers get nothing for that topic via the
next `session.get()`.

## TTL — `RETENTION_TTL`

Opt-in time-based expiration of cached entries. When set on a class,
entries older than the TTL are skipped + pruned at query time:

```python
@z.zeared
class StaleSensitive(z.Message):
    TOPIC = 'sensor/{id}/reading'
    RETAINED = True
    RETENTION_TTL = 30.0           # seconds
    id: int   = z.Int(required=True)
    value: float = z.Float(required=True)
```

Lazy semantics — no background sweeper. The TTL is checked when
`_RetentionCache._handle_query` runs (i.e. when a subscriber issues a
retained-fetch). Topics that never get queried may keep stale entries
in the cache; this is acceptable for typical usage and avoids the
overhead of a per-cache sweeper thread. Documented as a limit; revisit
if a real consumer reports needing eager expiration.

`RETENTION_TTL = None` (default) means retained values live until the
publishing session closes or a tombstone is emitted.

### Session-level fallback (`peer(retention_ttl=...)`)

For classes that don't declare their own `RETENTION_TTL`, set a
session-wide default via the factory:

```python
sess = z.peer(auto_reconnect=True, retention_ttl=300.0)
```

Precedence:

| Class `RETENTION_TTL` | Session `_retention_ttl` | Effective TTL |
|-----------------------|--------------------------|---------------|
| `5.0`                 | `300.0`                  | `5.0` (class wins) |
| `None`                | `300.0`                  | `300.0` (session fallback) |
| `None`                | `None`                   | none (no expiration) |
| `5.0`                 | `None`                   | `5.0` |

Resolved per-query via `retention._resolve_retention_ttl(cls, session)`,
so runtime mutation of either knob propagates without restart.

The factory kwarg is `ManagedSession`-only — passing
`retention_ttl=X` to `peer(auto_reconnect=False)` raises `TypeError`.
Class-level `RETENTION_TTL` is the only knob for raw sessions.

## Deduplication

Retained-fetch replays are deduplicated against the live stream by default
(see `Subscriber.DEDUPE = True`). Each retained reply for a given concrete
topic is matched against the most recent live sample for that topic; if
the wire payload is identical, the reply is suppressed. Set
`DEDUPE = False` on the subscriber class to opt out.

## Non-goals (documented sharp edges)

- **No automatic session-close cleanup.** Queryables are undeclared on
  `clear_retention_cache(session=...)` or registry re-init — nothing
  detects a session closing out from under us.
- **No TTL.** Retained values live as long as the publishing session does.

## `clear_retention_cache(*, session=None)`

Parallel to `clear_publisher_cache`. Without `session=`, clears every
entry and undeclares every queryable. With `session=`, drops only entries
keyed on that session.
