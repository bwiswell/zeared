# `async_.py`

Async façade over the sync zeared surface. Zenoh's Python bindings have no
native async entry points, so this module wraps the sync path:

- Publish / open calls are offloaded via `asyncio.to_thread`.
- Subscriber delivery bridges the Zenoh callback thread to asyncio via
  `loop.call_soon_threadsafe` feeding an `asyncio.Queue`.

Sync and async calls share state: `z.session`, `z.debug`, the publisher
cache, and the batch buffer (the buffer is a `ContextVar`, so async tasks
get per-task isolation).

### Mixing sync `batch()` with `await asend()`

`asyncio.run(coro)` and `asyncio.to_thread(...)` both call
`contextvars.copy_context()` and run via `ctx.run(...)`, so a sync
`with z.batch():` correctly captures `await msg.asend()` calls placed
inside it. See [`batch.md`](batch.md) for the test that pins this and
the one sharp edge (raw `threading.Thread` does NOT propagate context).

## Public surface

| Name | Signature | Purpose |
|------|-----------|---------|
| `apeer` | `def apeer(*, ...) -> _AsyncSessionContextManager` | async-context-managed peer session |
| `aclient` | `def aclient(router, *, ...) -> _AsyncSessionContextManager` | async-context-managed client session |
| `aopen` | `def aopen(cfg) -> _AsyncSessionContextManager` | async-context-managed dispatch on `SessionConfig` |
| `asend` | `async def asend(msg, *, session=None, topic=None) -> None` | free-function form of `msg.asend(...)` |
| `asend_batch` | `async def asend_batch(cls, items, *, session=None, topic=None) -> None` | free-function form of `Cls.asend_batch(...)` |
| `alisten` | `async def alisten(cls, *, session=None, maxsize=0) -> AsyncIterator` | async generator yielding messages |
| `abatch` | `@asynccontextmanager async def abatch()` | async counterpart of `z.batch()` |

### Async session lifecycle

`apeer` / `aclient` / `aopen` are sync functions returning an
`_AsyncSessionContextManager`. The only valid spelling is:

```python
async with z.apeer(connect=['tcp/x:7447']) as sess:
    await Telemetry(id=1, x=1.0, y=2.0).asend(session=sess)
# z.release(session=sess) ran automatically on a thread-pool worker.
```

`__aenter__` calls the underlying sync factory via `asyncio.to_thread`
(Zenoh's bindings are sync); `__aexit__` runs `z.release(session=sess)`
the same way. Holding the wrapper across the block lets your code
survive reconnects (the wrapper is stable; the underlying raw is
swapped on every reconnect).

> **Pre-0.0.15 break.** Earlier versions had `apeer` / `aclient` /
> `aopen` as `async def` functions returning a session directly via
> `await`. That form is gone — `await z.apeer()` raises `TypeError`
> (the CM isn't awaitable). Migrate to `async with`.

`Message` also exposes instance/class methods for the most common calls:
- `await msg.asend(...)`
- `await Cls.asend_batch(items, ...)`
- `async for msg in Cls.alisten(...):`

## When to use which

| Goal | Call |
|------|------|
| Keep event loop responsive while publishing | `await msg.asend()` — pays one `to_thread` hop per send |
| Consume messages via async iteration | `async for msg in Cls.alisten():` |
| Consume messages via `async def` handler | `Cls.on_message(async_handler)` — handlers scheduled on the loop |
| Batch sends atomically in an async scope | `async with z.abatch():` |

For pure throughput, the sync path is ~60% faster on publish loops (the
per-call `to_thread` cost dominates). For responsive services, the
ergonomic wins are worth it. See `tests/bench_async.py`.

## `alisten` subscriber lifecycle

```python
async def listen_forever():
    async for msg in Telemetry.alisten():
        process(msg)          # async generator; breaks close the sub
```

Breaking out of the `async for` (or cancelling the consuming task) runs
the generator's `finally`, which calls `Subscriber.close()` under the
hood. No explicit cleanup required.

`maxsize=0` (default) gives an unbounded `asyncio.Queue`. Pass a positive
integer for backpressure — delivery into the queue blocks when full, at
which point Zenoh's internal buffering takes over.

## Coroutine-callback detection in `on_message`

When `Cls.on_message(cb, ...)` receives an `async def` callback, it wraps
it at subscribe time: each incoming sample dispatches the coroutine onto
the event loop that was running when `on_message` was called, via
`asyncio.run_coroutine_threadsafe`. The callback thread itself stays
free for Zenoh's delivery machinery.

If called without a running event loop, `on_message` raises
`SubscriptionError` — use `Cls.alisten()` instead.
