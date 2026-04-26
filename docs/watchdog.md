# `watchdog.py`

Per-subscription freshness watchdog. Optional — pass `expected_interval`
to `Cls.on_message` to opt in.

## `_SubscriberWatchdog`

One long-running thread per watchdog instance. The thread does
`event.wait(timeout=N)` / `event.clear()` in a loop; the Zenoh delivery
thread calls `ping()` from the dispatch path to wake the wait.

```python
class _SubscriberWatchdog:
    def __init__(self, interval, on_quiet, on_active, startup_grace=None):
        ...
    def ping(self) -> None: ...
    def cancel(self) -> None: ...
```

## Two startup modes

### Optimistic (default — `startup_grace=None`)

The loop thread doesn't spawn until the first `ping()`. A subscriber
that never receives any message will never fire `on_quiet`.

Right answer for "tell me if my live producer is going silent" — you only
care about gaps after you've established that messages flow.

### Grace-window (`startup_grace=N`)

The loop spawns at construction time. If no message arrives within
`startup_grace` seconds, `on_quiet` fires once. After the first message
(or after grace expires), subsequent waits use `interval` as usual.

Right answer for "tell me if I haven't heard anything within N seconds
of subscribing" — daemon liveness checks at startup.

## Threading model

- The watchdog thread is a `daemon=True` thread named `zeared-watchdog`.
- Callbacks (`on_quiet`, `on_active`) fire on the watchdog thread.
- User code that mutates shared state from these callbacks is responsible
  for its own locking.
- `cancel()` is idempotent and safe from any thread.

## Async callbacks

Both `on_quiet` and `on_active` accept `async def`. Two adapter paths,
selected at watchdog construction time:

| Construction context | Dispatch path | Cost |
|----------------------|---------------|------|
| Running event loop on the constructing thread | `run_coroutine_threadsafe` against the captured loop | cheap (queue-and-wake) |
| No running loop | `asyncio.run(cb())` per fire | a few ms per fire (loop spin-up + tear-down) |

Per-fire `asyncio.run()` is correct but heavy. Watchdog events are
inherently low-frequency — `expected_interval` is typically tens of
seconds — so the per-fire cost is negligible in practice. **A watchdog
firing dozens of times per second isn't a watchdog, it's a
misconfiguration.**

If you have a specific reason to use an async callback when no event
loop is available (and you're firing often enough to care about the
overhead), prefer:

1. **Sync callback that schedules its own work.** `on_quiet=lambda: queue.put('quiet')` keeps the watchdog thread fast and lets a separate consumer drain the queue.
2. **Construct the watchdog from inside a running loop.** `Cls.on_message(..., expected_interval=N, on_quiet=async_fn)` called from an async context captures the loop once; subsequent fires route via the cheap path.

Defaulting to sync `on_quiet` / `on_active` is the lowest-friction
choice. The async path exists for ergonomics, not throughput.

## Cancellation on subscriber close

`Subscriber.close()` cancels its watchdog as the FIRST step (before
undeclaring Zenoh subs and deregistering from the presence observer).
A pending `on_quiet` that would have fired after close is suppressed.

## Pinned with tests

- `tests/test_watchdog.py::TestSubscriberWatchdogPrimitive` covers the
  primitive directly (optimism, transitions, cancel, async callbacks).
- `tests/test_watchdog.py::TestStartupGrace` covers the grace-window
  variant.
- `tests/test_watchdog.py::TestWatchdogViaOnMessage` runs the full
  Zenoh round-trip with `Cls.on_message(expected_interval=...)`.
