# `batch.py`

## `z.batch() -> _BatchContext`

Context manager that defers `msg.send()` calls until the outermost `__exit__`.

```python
with z.batch() as b:
    a.send()
    b.send()
    # b.flush()        # optional: drain pending sends now, keep collecting
# all buffered sends flush here
```

## Semantics

| Behaviour | |
|-----------|---|
| **Flat nesting** | An inner `with z.batch():` shares the outer buffer; it neither flushes nor discards on its own exit. |
| **Discard on exception** | An exception escaping the outermost block drops the buffer without flushing. Use `b.flush()` mid-block if you want a partial send. |
| **Context-scoped** | Buffer is stored in a `contextvars.ContextVar`. Each thread starts empty; each `asyncio` task gets a per-task copy — sibling tasks don't accidentally share a batch. |
| **Mixed sessions** | `send(session=X)` inside a batch records the explicit target; flush partitions by `(cls, session)` and hands each partition to the corresponding `_PublisherCache`. |

### Reconnect during flush

If the underlying session is a `ManagedSession` and enters
`RECONNECTING` mid-flush (active probe or send-failure detection), the
in-progress send raises `SessionDeadError`. Per batch's discard-on-
exception contract, any remaining buffered sends are dropped. Daemons
doing bulk-publish-on-failover should size batches to fit comfortably
inside one reconnect window or wrap the batch context in a try/except
for `SessionDeadError`:

```python
try:
    with z.batch():
        for item in many_items:
            Cls(**item).send()
except z.SessionDeadError:
    queue_for_retry(many_items)        # caller-owned recovery
```

The buffer itself doesn't care about session identity — it holds
`(cls, session_ref, topic, bytes, encoding, retain_mode)` tuples — so
flushing AFTER a reconnect completes (e.g. the buffer was built
pre-reconnect, the block exits post-reconnect) works correctly: the
flush partitions by `(cls, session)` and hands each partition to the
corresponding `_PublisherCache`, which lazy-rebuilds against the new
raw on the first `send()`.

## `_BatchHandle.flush() -> None`

Drains the active buffer to Zenoh immediately without leaving the block. The
batch stays active — subsequent sends keep accumulating.

## `current_buffer() -> list | None`

Internal helper used by `Message.send` to check whether a batch is active.
Returns the innermost buffer or `None`.

## Why `batch()` isn't about perf

With publisher caching on by default, every `msg.send()` already uses a
long-lived `zenoh.Publisher` per concrete topic. `z.batch()` exists for
**atomicity of intent** — grouping a related set of sends so that an
unexpected exception tears the whole group out cleanly — and for
ergonomic bulk sends via `Cls.send_batch(items)`.

## Async counterpart

`z.abatch()` is the async version — same semantics, `async with` syntax.
Because the buffer is a `ContextVar`, `async with z.abatch():` inside a
task is isolated from batches in sibling tasks on the same event loop:

```python
async with z.abatch():
    await a.asend()
    await b.asend()
    # atomic — flush or discard on exception
```

See [`async_.md`](async_.md).

## Mixing sync `batch()` with `await asend()` inside

```python
async def coro():
    await M(id=1, v=10).asend()
    await M(id=2, v=20).asend()

with z.batch():           # SYNC batch
    asyncio.run(coro())   # both asend() calls buffer correctly
# both messages flush here
```

This works because `asyncio.run()` (and `asyncio.to_thread()` inside
`asend()`) propagate the calling context's `ContextVar` bindings via
`contextvars.copy_context()` + `ctx.run()`. The worker thread reads the
same buffer-list reference the main thread set up; mutations are
visible at `__exit__`.

### Sharp edge — raw `threading.Thread` does NOT propagate

If you spawn a worker via `threading.Thread(target=...)` from inside a
`with z.batch():` block, the worker thread starts with a *fresh*
context, so `current_buffer()` in the worker returns `None` — the
worker's `msg.send()` flushes immediately and **bypasses the batch**.

```python
with z.batch():
    threading.Thread(target=lambda: M(id=1).send()).start()  # ← bypasses batch
```

If you need a worker thread to participate in the batch, run it through
`asyncio.to_thread()` (which copies context) or copy the context yourself:

```python
import contextvars
ctx = contextvars.copy_context()
threading.Thread(target=lambda: ctx.run(M(id=1).send)).start()  # ← respects batch
```

The integration test in `tests/test_async_.py::TestAsendInsideSyncBatch`
pins the asyncio path; the raw-thread path is left as documented sharp
edge rather than special-cased.
