# Session lifecycle — `z.release(*, session)`

Single-call shutdown that walks every zeared-owned resource for a session
in the right order, then closes the session.

```python
z.release(session=my_session)
```

Replaces the six-step manual dance of:

```python
# Equivalent (and easy to get wrong):
for sub in my_subs:
    sub.close()
z.clear_publisher_cache(session=my_session)
z.clear_retention_cache(session=my_session)
z.clear_observer(session=my_session)
z.clear_presence_state(session=my_session)
my_session.close()
```

## Order

1. **Close zeared subscribers on this session.** Cancels any active
   watchdogs, undeclares the underlying Zenoh subs, deregisters presence
   dispatchers.
2. **`clear_publisher_cache(session=...)`.** Undeclares cached
   `zenoh.Publisher` instances.
3. **`clear_retention_cache(session=...)`.** Undeclares the retention
   queryable + drops cached payloads.
4. **`clear_observer(session=...)`.** Stops the presence observer
   (alive-sub + will-sub).
5. **`clear_presence_state(session=...)`.** Undeclares the liveliness
   token + will queryable. **This is the call that fires peers' will
   synthesis** — the token DELETE propagates over the live transport
   here.
6. **`session.close()`.** Tears down the local Zenoh transport.

The order between steps 5 and 6 is the easy one to get wrong: flipping
them tears down the transport before the liveliness DELETE escapes,
which silently breaks subscribers of peer wills. The code has an inline
comment spelling this out; the test
`tests/test_release.py::TestReleasePropagatesPeerWill` empirically pins
the invariant via a two-session `connected_pair` fixture.

## API contract

- `session=` is **keyword-only and required**. `z.release()` with no
  arguments raises `TypeError`. Explicit-always — no implicit
  module-default fallback. Daemon code that wants the default session
  must read it explicitly: `z.release(session=z.session.current)`.
- **Idempotent**. A second call against the same session is a clean no-op
  (every cleared registry is empty).
- **Tolerates a closed session.** If the user already called
  `session.close()`, `z.release` silently swallows the resulting
  `RuntimeError` — the registries still get cleaned up.

## Subscriber registry

`subscriber.py` carries a module-level `_subscribers: dict[id(session),
set[Subscriber]]` that `Subscriber._declare` populates and
`Subscriber.close` deregisters from. `z.release` walks this set and calls
`.close()` on each.

Hard references (no weakref) since `Subscriber` uses `__slots__`. Each
subscriber explicitly deregisters on close, so the set stays accurate
without runtime cost.

## Async equivalent

There isn't one. `z.release` is a sync call — it's a shutdown primitive,
not in any hot path. Wrap in `asyncio.to_thread` if you need to call it
from an async context without blocking the loop.
