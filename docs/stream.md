# `stream.py`

## `Stream`

The async iterator `z.alisten(...)` / `Cls.alisten(...)` and
`z.aquery_iter(...)` return (0.4.0; both were async generators before).
Zenoh delivers on its own threads; a `Stream` hands each item to one event
loop through an `asyncio.Queue` fed with `loop.call_soon_threadsafe`, so
the queue is only touched on the loop thread.

```python
async with Telemetry.alisten(maxsize=1024) as stream:
    async for msg in stream:
        process(msg)
    print(stream.dropped)
```

| Member | |
|--------|---|
| `async for item in stream` | yields each item; ends when the stream closes or a finite source (a query) ends |
| `await anext(stream)` | the next item |
| `dropped` | how many items a full stream has discarded, oldest first; monotonic |
| `maxsize` | the bound, or `0` for unbounded |
| `await stream.aclose()` | releases the source, and ends an `async for` waiting in another task; idempotent |
| `async with stream` | closes on exit |

## The bound and the overflow policy

`alisten` streams are bounded at `maxsize=1024` by default; `maxsize=0` is
unbounded. `aquery_iter` streams are always unbounded, since a query's
replies are already in flight and its bound is `timeout`.

Delivery never applies backpressure: Zenoh's callback thread never waits.
So when a bounded stream is full, the incoming item evicts the **oldest**
queued one. Most zeared traffic is live state or telemetry, where a
consumer that has fallen behind wants the freshest data rather than a
backlog. Each eviction adds one to `dropped`; the first is logged once at
`WARNING` on the `zeared.stream` logger, naming the class and the bound.
Nothing is raised, and nothing reaches the loop's exception handler.

A consumer that must account for loss reads `dropped`, for example as a
difference across a capture window. One that must lose nothing either keeps
up, passes `maxsize=0` (and accepts unbounded memory), or bridges
`on_message` into its own queue.

## Closing

A stream releases its source (the subscriber, or the query worker) when:

- `aclose()` is awaited, or an `async with` block exits;
- a finite source ends (the query completed), or raised: the error
  re-raises in the consumer;
- the stream is collected after the consumer `break`s out of `async for`.
  The source holds only the stream's inner channel, never the `Stream`, so
  dropping the last reference is enough.

`alisten` declares its subscriber, and `aquery_iter` starts its query, when
called, so call them from inside a running event loop (`RuntimeError`
otherwise).
