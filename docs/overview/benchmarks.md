# Benchmarks

End-to-end publish → subscriber-received throughput on a single in-process
Zenoh peer session, compared against `marshmallow` + raw Zenoh to measure
zeared's overhead relative to the prior pattern.

All numbers below come from the committed `bench/results.json` artifact.
Regenerate both with:

```bash
uv run python -m bench
```

## Schema

One outer object with a 20-item list of 3-field records plus 3 string tags —
the same shape as seared's roundtrip benchmark, layered onto zeared's wire
path.

## Suites

`bench/` is a package (`python -m bench`); each suite answers a different
question and contributes rows to the same artifact.

| Suite | Style | Answers |
|-------|-------|---------|
| `suite_wire` | Fixed N (5,000 messages) | What does one message cost, and how big is it on the wire? |
| `suite_rusted` | Fixed N | What does the optional compiled accelerator buy? |
| `suite_throughput` | 5 s publish window | What sustained sync rate holds when pushed flat out? |
| `suite_async` | 5 s publish window | What does each sync/async delivery combination cost? |

Suites needing a package a default dev sync doesn't install skip with a note
rather than failing the run: `marshmallow` (behind the `[bench]` extra) and
`rusted` (see [Accelerated](#accelerated-seared--rusted)).

## Configurations

- **Zenoh + `marshmallow` (JSON)** — apples-to-apples: same Zenoh transport,
  same JSON wire form, marshmallow as the codec. The subscriber decodes every
  sample, matching zeared's decode-on-receive path.
- **`zeared` (JSON, cached)** — `ENCODING='json'` with the default
  `PUBLISHER=True` publisher cache.
- **`zeared` (msgpack, cached)** — default `ENCODING='msgpack'` + cache.
- **`zeared` (msgpack, `PUBLISHER=False`)** — falls back to `session.put`
  per send.
- **`zeared` async variants** — `asend` / `alisten` / `async def`
  `on_message`. Zenoh's Python bindings have no native async entry points, so
  these wrap the sync path via `asyncio.to_thread` and
  `run_coroutine_threadsafe`.

Every non-accelerated strategy is pinned to the pure-Python path with
`accel=False`, so an accelerator wheel that happens to be installed can't
silently retarget them and publish compiled numbers under zeared's own name.

## Wire suite

5,000 messages per strategy, no drops:

| Strategy | pub/s | e2e/s | MB/s | wire (B) |
|----------|------:|------:|-----:|---------:|
| Zenoh + `marshmallow` (JSON) | 3,771 | 3,754 | 3.00 | 796 |
| `zeared` (JSON, cached) | 6,424 | 6,394 | 5.11 | 796 |
| `zeared` (msgpack, cached) | 7,172 | 7,074 | 3.82 | 533 |
| `zeared` (msgpack, `PUBLISHER=False`) | 6,808 | 6,804 | 3.63 | 533 |

msgpack is **33% smaller on the wire** than the JSON form (533 B vs 796 B)
for the same payload.

## Headline matrix

5 s publish window per strategy, no drops:

| Strategy | pub/s | e2e/s | MB/s | wire (B) |
|----------|------:|------:|-----:|---------:|
| Zenoh + `marshmallow` (JSON) | 3,491 | 3,356 | 2.78 | 796 |
| sync `send` + sync `on_message` (JSON) | 6,445 | 6,197 | 5.13 | 796 |
| sync `send` + sync `on_message` (msgpack) | 7,234 | 6,888 | 3.86 | 533 |
| `asend` + sync `on_message` (msgpack) | 2,720 | 2,615 | 1.45 | 533 |
| sync `send` + `alisten` (msgpack) | 5,560 | 5,505 | 2.96 | 533 |
| `asend` + `alisten` (msgpack) | 2,073 | 2,053 | 1.11 | 533 |
| `asend` + `alisten` (JSON) | 2,056 | 2,035 | 1.64 | 796 |
| sync `send` + `async def` `on_message` (msgpack) | 4,800 | 4,752 | 2.56 | 533 |

Relative to the fastest row (sync msgpack — the default):

- **`marshmallow` is ~52% slower and ~50% larger on the wire.**
- **Sync JSON is ~11% slower** — same wire, slower codec.
- **`await asend()` pays a per-call `asyncio.to_thread` hop and is ~62%
  slower** than the sync loop. Reach for it when keeping the event loop
  responsive matters more than raw throughput.
- **`alisten` with sync `send` is ~23% slower** — a cheap way to go async on
  the consumer side without a publish-side tax.
- **`async def` callbacks via `on_message` are ~34% slower** — the publish
  path stays sync-fast; each handler dispatches one coroutine per message.

The pub/s vs e2e/s gap stays under 5% everywhere: the in-process subscriber
keeps up with the publisher across every strategy.

## Accelerated (seared + `rusted`)

`rusted` is seared's optional compiled (Rust/PyO3) accelerator core. When it
is installed and a class is built entirely from seared-native fields, seared
swaps its generated `load`/`dump` for compiled equivalents — and because
zeared `Message` classes are seared classes, zeared's wire path inherits that
for free. Nothing in zeared declares or imports it.

5,000 messages per strategy:

| Strategy | pub/s | e2e/s | MB/s | wire (B) |
|----------|------:|------:|-----:|---------:|
| `zeared` + `rusted` (JSON, cached) | 10,947 | 10,872 | 8.71 | 796 |
| `zeared` + `rusted` (msgpack, cached) | 13,348 | 13,301 | 7.11 | 533 |

Against the same strategies on the pure-Python path, that is **1.70× on the
JSON path and 1.86× on msgpack**, end to end — serialization is a large
enough share of zeared's per-message cost that removing most of it moves the
whole wire path. Wire sizes are unchanged, as expected: the accelerator
changes how bytes are produced, not what they are.

`rusted` is not a zeared dependency and is not on PyPI yet. Until wheels
ship, the accelerated suite runs only where it is already installed, and
skips with a note everywhere else. These numbers were taken against
`rusted 0.1.2`; the artifact records the exact versions behind every row.

## Why zeared beats `marshmallow` + Zenoh

- **`__slots__` everywhere** in seared field types — no per-instance
  `__dict__`.
- **Pre-baked field spec** computed once at decorator time; each `dump` /
  `load` walks the same `(attr, wire, Field)` triples.
- **Publisher caching** keeps a long-lived `zenoh.Publisher` per concrete
  topic, avoiding the per-send declare cost. Worth ~5% on this static-`TOPIC`
  workload; it earns more on templated `TOPIC`s with repeated concrete keys.
- **msgpack by default** — smaller on the wire and cheaper to encode than
  JSON.

## Caveats

- Single in-process peer session: no network, no serialization across a real
  transport hop. These measure zeared's overhead, not Zenoh's throughput
  ceiling.
- Numbers are from one machine (recorded in `results.json` alongside the
  Python and platform versions); treat ratios as the durable signal, not
  absolute rates.
