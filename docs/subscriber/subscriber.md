# `subscriber.py`

`Subscriber` is the handle returned by `Message.on_message(...)`. It wraps an
underlying `zenoh.Subscriber` and handles the decode + dispatch loop.

The class is parameterised in the message type via `typing.Generic[M]` —
`Cls.on_message(cb)` is typed as returning `Subscriber[Cls]`, giving IDE
+ type-checker visibility into which kind of subscriber a given handle
represents. Runtime is identical with or without the parameter; bare
`from zeared import Subscriber` continues to work.

## Lifecycle

```python
sub = Telemetry.on_message(cb)
...
sub.close()                     # idempotent
```

Or as a context manager:

```python
with Telemetry.on_message(cb) as sub:
    ...
# closed automatically
```

## Arity-inspected dispatch

`_wants_meta(cb)` inspects `cb`'s signature once at subscribe time. If `cb`
accepts two or more positional parameters — or a `*args` catch-all — the
subscriber invokes it with `(msg, meta)`; otherwise with `(msg,)`.

Bound methods are handled correctly (`self` is excluded from the count).

## Coroutine callback scheduling

If `cb` is an `async def` function, `Subscriber._declare` captures the
running event loop at subscribe time and wraps the callback in a sync
shim that schedules the coroutine via `asyncio.run_coroutine_threadsafe`
on each sample. If there's no running loop, `SubscriptionError` is
raised — use `Cls.alisten()` instead.

## Auto-reconnect interaction

When the resolved session is a `ManagedSession`
(see [`_managed_session.md`](_managed_session.md)), the reconnect
machinery walks the per-session subscriber registry on every successful
swap and calls `Subscriber._redeclare(new_raw, managed)` on each. The
user's `Subscriber` handle stays valid across the boundary — internally
its `_zenoh_subs` tuple is rebuilt against the new raw and any
retained-fetch / presence-dispatcher state is re-wired.

Per-subscriber opt-out:

```python
sub = MyMessage.on_message(cb, auto_reconnect=False)
```

A subscriber opted out is skipped during the restoration walk; its
`_zenoh_subs` stay bound to the dead raw. Power users who need to
manage subscriber lifecycle explicitly across reconnects keep that
control.

## Multi-topic subscriptions

When a class declares `EXTRA_TOPICS`, `Subscriber._declare` registers one
`zenoh.Subscriber` per wildcard — all feeding the same handler.
`Subscriber.close()` undeclares all of them; partial-declaration failures
during setup roll back any subs already created.

## Retained-value fetch on `RETAINED` classes

When `msg_cls.RETAINED` is `True`, `_declare` issues a `session.get()` per
declared template wildcard after bringing up the live subscribers. Reply
samples — cached payloads from peer `_RetentionCache` queryables — flow
through the same dispatch function used for live samples. Decode failures
route through the unified `on_error` hook. See [`retention.md`](retention.md).

## DELETE samples (tombstones)

Subscribers detect `sample.kind == zenoh.SampleKind.DELETE` and skip the
dispatch — no callback fires. This is how `unretain()` propagates the
"forget this key" signal without surfacing a nil message to user code.

## Presence / LWT dispatch (LIVELINESS classes)

When the message class has `LIVELINESS = True`, `Subscriber._declare`
additionally:
1. Ensures the session's `_PresenceObserver` is running (one per session,
   shared by all LIVELINESS subscribers).
2. Registers an interested-party dispatcher with the observer.

On a peer's liveliness-token disappearance, the observer iterates its
stashed wills for that peer and calls each interested dispatcher with a
synthesised sample. The dispatcher matches the will's `target_key_expr`
against this class's templates; if it matches, the subscriber's normal
`dispatch` function fires — the callback sees a regular `cb(msg)` (or
`cb(msg, meta)`) with `meta.source_info = peer_zid`.

`Subscriber.close()` unregisters from the observer. See [`presence.md`](presence.md).

## Encoding resolution (inbound)

Each sample is decoded based on:

1. The sample's Zenoh `encoding` attribute, if it parses as msgpack or JSON.
2. Otherwise, the effective class encoding — `'json'` under `z.debug = True`,
   else `cls.ENCODING`.

A sender using JSON and a sender using msgpack can publish to the same topic
and a single subscriber will decode both correctly.

## Per-subscriber dedupe override

Class-level `DEDUPE` controls whether retention-fetch replays are
deduplicated against live samples (default `True` for `RETAINED`
classes). A specific subscriber can override:

```python
sub = Telemetry.on_message(cb, dedupe=False)   # this sub sees every replay
sub = Telemetry.on_message(cb, dedupe=True)    # force on, even if class disabled
sub = Telemetry.on_message(cb, dedupe=None)    # class default (the real default)
```

Useful for diagnostic / audit subscribers that want every retained
sample, separate from production subscribers that want clean
single-fire semantics.

## Schema check + warn-once

When the class has `SCHEMA = '<value>'`, the subscriber compares each
sample's wire-attached schema against the local class value. Mismatches
drop the sample and route via `on_error` as `SchemaMismatchError`
(subclass of `DecodeError`). Per-subscriber state caches
`(sender_zid, observed_schema)` pairs — the first mismatch from any
given pair fires `on_error`; subsequent samples from the same pair
drop silently to avoid log spam from a misaligned peer publishing
continuously. Cache cleared on `Subscriber.close()`.

Subscriber classes with `SCHEMA = None` (default) skip the check;
`meta.schema` still populates from the wire if present.

## Error handling

Decode failures and callback exceptions are routed to:

- The `on_error=cb(exc, raw_bytes)` kwarg if provided.
- Otherwise a `logging.Logger` named `zeared.subscriber` — `warning` for
  decode failures, `exception` for callback crashes.

A bad sample never takes the subscriber down.

## Attributes (internal)

- `_zenoh_sub: zenoh.Subscriber`
- `_closed: bool`
