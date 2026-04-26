# Benchmarks

End-to-end publish → subscriber-received throughput on a single
in-process Zenoh peer session. Compared against `marshmallow` + Zenoh
to measure zeared's overhead relative to the prior pattern.

## Schema

One outer object with a 20-item list of 3-field records plus 3 string
tags — same shape as seared's roundtrip benchmark, layered on top of
zeared's wire path.

## Three benches

| File | Style | When to use |
|------|-------|-------------|
| `bench/bench_wire.py` | Fixed N (5,000 iterations) | Quick smoke check after wire-path changes |
| `bench/bench_throughput.py` | Sync-only, duration-based | Tracking sync regression in long runs |
| `bench/bench_async.py` | Full sync + async matrix | Comprehensive headline numbers |

## Configurations

- **Zenoh + `marshmallow` (JSON)** — apples-to-apples comparison: same
  Zenoh transport, same JSON wire form, marshmallow as the codec.
- **`zeared` sync (JSON, cached)** — `ENCODING='json'` with
  `PUBLISHER=True` (default cache).
- **`zeared` sync (msgpack, cached)** — default `ENCODING='msgpack'`
  + cache. The fastest config.
- **`zeared` sync (msgpack, no cache)** — `PUBLISHER=False` falls back
  to `session.put` per send.
- **`zeared` async variants** — `asend` / `alisten` / async `on_message`
  callbacks. Wraps sync Zenoh via `asyncio.to_thread` and
  `run_coroutine_threadsafe`.

## Headline matrix

10 s publish window per strategy, single in-process Zenoh peer session,
no drops:

| Strategy                              | sent   | pub/s | e2e/s | MB/s | wire (B) |
|---------------------------------------|-------:|------:|------:|-----:|---------:|
| Zenoh + `marshmallow` (JSON)          | 39,508 | 3,950 | 3,872 | 3.14 | 796      |
| `zeared` sync (JSON, cached)          | 62,954 | 6,295 | 6,141 | 5.01 | 796      |
| `zeared` sync (msgpack, cached)       | 70,638 | 7,064 | 6,925 | 3.77 | 533      |
| `zeared` sync (msgpack, no cache)     | 68,400 | 6,833 | 6,666 | 3.64 | 533      |
| `zeared` `asend` + sync `on_message`  | 32,864 | 3,286 | 3,221 | 1.75 | 533      |
| `zeared` sync `send` + `alisten`      | 62,500 | 6,249 | 6,217 | 3.33 | 533      |
| `zeared` `asend` + `alisten` (msgpack)| 28,528 | 2,853 | 2,838 | 1.52 | 533      |
| `zeared` `asend` + `alisten` (json)   | 26,636 | 2,664 | 2,650 | 2.12 | 796      |
| `zeared` sync `send` + `async def` cb | 56,405 | 5,640 | 5,612 | 3.01 | 533      |

## Commentary

Relative to the fastest row (`zeared` sync msgpack cached — the default):

- **`marshmallow` is ~44% slower and ~50% larger on the wire.**
- **Sync JSON is ~11% slower** (same wire, slower codec).
- **`PUBLISHER = False` is ~3% slower** on this static-TOPIC workload —
  the cache earns more on templated-TOPIC hot loops.
- **`await asend()` pays a per-call `asyncio.to_thread` hop and is
  ~55% slower** than the sync loop. Use it when keeping the event
  loop responsive matters more than raw throughput.
- **`alisten` with sync `send` is only ~12% slower than pure sync** —
  cheap way to go async on the consumer side without a publish-side
  tax.
- **`async def` callback via `on_message` is ~20% slower than sync** —
  publish path stays sync-fast; each handler dispatches one coroutine
  on the running loop per message.

The pub/s vs e2e/s gap stays under 3% everywhere — the in-process
subscriber keeps up with the publisher across all strategies.

## Why zeared beats `marshmallow` + Zenoh

- **`__slots__` everywhere** in seared field types — no `__dict__`
  per instance.
- **Pre-baked field spec** computed once at decorator time; each
  `dump` / `load` walks the same `(attr, wire, Field)` triples.
- **Publisher caching** keeps a long-lived `zenoh.Publisher` per
  concrete topic, avoiding the per-send declare cost.
- **msgpack default** with native binary support (zeared threads
  `format='msgpack'` into seared so `Bytes` / `NDArray` skip base64).
- **Single decode pass** on subscribe — `_decode` does
  `codec.unpack` + `cls.load(payload, format=encoding)` in one shot.

## Reproduction

```sh
# Install marshmallow ad-hoc — NOT a zeared dependency.
uv pip install marshmallow

# Run the three benches:
uv run python bench/bench_wire.py             # quick, fixed-N smoke check
uv run python bench/bench_throughput.py 10    # sync-only duration bench
uv run python bench/bench_async.py 10         # full sync + async matrix (headline)
```

The duration-based benches accept a positional argument for seconds per
strategy. `bench_async.py` is the headline source — it's where the
matrix above came from.

## Caveats

- **Single-process, single-thread per role** (one publisher task / one
  subscriber). Real-world contention with N peers / N tasks would
  vary per scenario.
- **No I/O beyond Zenoh's transport.** Disk, network, downstream
  consumers — not measured.
- **Static schema.** Both libraries cache class-level introspection.
  Cold-start (first publish on a freshly-decorated class) isn't
  measured here.
- **In-process Zenoh peer session** uses Zenoh's local transport
  shortcut. Wire numbers are accurate (samples actually serialise),
  but no network hop is involved. Peer-over-TCP would add latency.
- **No retention / presence / wills** in the bench classes — the
  publish path is the simplest possible.
