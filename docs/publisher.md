# `publisher.py`

Internal publisher cache. Not part of the public API — users interact with
it only indirectly via the `PUBLISHER` class attribute on their message
classes.

## What it does

For every `(Message subclass, zenoh.Session)` pair, zeared lazily declares
`zenoh.Publisher` objects per *concrete* topic string seen, caches them, and
reuses on subsequent sends. The alternative (used for `PUBLISHER = False`)
is to call `session.put(key_expr, payload, encoding=...)` each time, which
is the 0.0.1 behaviour.

## `effective_cap(cls) -> int`

Resolves the `PUBLISHER` class attribute:

| Value | Effective cap |
|-------|---------------|
| `True` (default) | 256 |
| `False` | 0 (cache fully bypassed) |
| `int` | the integer value |

## `_PublisherCache`

Cache scoped to one `(cls, session)` pair. Slots-based, no public API on the
class itself beyond `size` (for testing).

`put(concrete_topic, raw, encoding)`:

1. If cap is 0 → `session.put(...)`.
2. If the concrete topic is already cached → reuse.
3. If cache is full → `session.put(...)` + one-time `warnings.warn`.
4. Otherwise → `session.declare_publisher(...)`, cache, then `pub.put(raw)`.

Any send failure (most commonly: the session was closed out from under us)
drops the entire cache for this pair and raises `ZearedError` with a clear
message.

## `get_cache(cls, session) -> _PublisherCache`

Returns (creating if necessary) the cache for the pair. Keyed on
`(cls, id(session))` — weakrefs aren't supported on `zenoh.Session`, so
stale entries only get cleaned up via explicit calls to
`clear_publisher_cache()` or through send-time failure detection.

## `clear_publisher_cache(*, session=None) -> None`

Module-level helper. Without `session=`, drops every entry. With `session=`,
drops only those targeting that session — useful right before closing a
session in a long-running process.

## `published_topics(*, cls=None, session=None) -> dict`

Snapshot introspection for dashboards / diagnostics. Returns a
`dict[(Message subclass, id(session)), frozenset[str]]` of every concrete
topic this process has emitted. Filters `cls=` and/or `session=` narrow
the view.

Every `put()` path (cached, `session.put` fallback, cap-overflow fallback)
records into `_PublisherCache._emitted: set[str]` — so even
`PUBLISHER = False` classes are tracked. Tombstoned topics stay in the
set: "emitted during this process lifetime" is literal.

Companion class method:
`Cls.published_topics(*, session=None) -> frozenset[str]` — aggregates
across all sessions when `session=None`, or scopes to a single session
when explicit.

