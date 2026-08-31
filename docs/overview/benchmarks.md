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
| `suite_stacks` | Fixed N (5,000 messages) | Pydantic over MQTT, or zeared? (starts its own broker) |

Suites needing a package a default dev sync doesn't install skip with a note
rather than failing the run: `marshmallow` (behind the `[bench]` extra) and
`rusted` (see [Accelerated](#accelerated-seared--rusted)). `suite_stacks`
additionally needs a `mosquitto` binary on `PATH` and skips without one.

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

## Pydantic + MQTT, or zeared?

The most common question about zeared is whether it beats the other realistic
stack for typed Python pub/sub: Pydantic models over MQTT. `suite_stacks`
answers it — and deliberately refuses to answer it with one number.

**The comparison spans two independent axes.** Pydantic vs seared is a
*codec* question: pydantic-core is compiled Rust, pure-Python seared is not.
MQTT vs Zenoh is a *transport* question: broker-mediated store-and-forward
against a peer-to-peer mesh. A single headline conflates them — and would
conflate them in zeared's favour, since the rest of this page runs an
in-process Zenoh peer with no broker at all while any MQTT number pays a hop
through a broker process.

So this suite holds one axis fixed at a time. Every row is JSON on the wire
except the two labelled `msgpack default`, so encoding doesn't confound the
deltas. The Zenoh rows run through a **router** — a broker-equivalent hop —
except where marked `peer`. `mosquitto` is started by the bench itself.

5,000 messages per row:

| Row | e2e/s | wire (B) |
|-----|------:|---------:|
| pydantic + MQTT (QoS 0) | 8,407 | 670 |
| pydantic + MQTT (QoS 1) | 2,871 | 670 |
| seared + MQTT (QoS 0) | 5,130 | 796 |
| seared + MQTT (QoS 1) | 2,521 | 796 |
| pydantic + Zenoh (router, raw) | 26,447 | 670 |
| seared + Zenoh (router, raw) | 7,657 | 796 |
| zeared + Zenoh (router) | 6,976 | 796 |
| zeared + Zenoh (peer) | 7,311 | 796 |
| zeared + Zenoh (peer, msgpack default) | 8,055 | 533 |
| zeared + `rusted` + Zenoh (peer, JSON) | 12,519 | 796 |
| zeared + `rusted` + Zenoh (peer, msgpack default) | 13,954 | 533 |

> **Read these on end-to-end rate, not publish rate.** `paho`'s `publish()`
> hands off to a network thread and returns, so the MQTT rows' publish-side
> figures measure *client-side enqueue*, not transmission — they look ~2x the
> rate the subscriber actually saw. Zenoh's `put` does the work inline, so its
> two rates track each other. `results.json` records both; only e2e is
> comparable across transports.

### What each pair says

| Comparison | Result |
|------------|--------|
| **Codec**, same raw Zenoh transport | pydantic **3.45x** faster than seared |
| **Codec**, same MQTT transport (QoS 0) | pydantic **1.64x** faster |
| **Transport**, same pydantic codec | Zenoh **3.15x** faster than MQTT |
| zeared's `Message` wrapper over raw seared + Zenoh | **9%** cost |
| Zenoh peer vs router topology | **5%** faster |
| msgpack vs JSON on the same path | **10%** faster, **33%** smaller |
| `rusted` accelerator on the native stack | **1.73x** |

**pydantic wins the codec axis, and by a lot.** That is the expected result,
not a surprise: its core is compiled and seared's is not — seared's own
benchmarks already record pydantic-core as several times faster on the dict
path. The codec gap is narrower over MQTT (1.64x) simply because the
transport dominates there and there is less headroom to win.

### The answer

Against the stack as usually deployed:

| zeared config | vs pydantic + MQTT QoS 0 | vs QoS 1 |
|---------------|-------------------------:|---------:|
| default (msgpack, peer) | **0.96x** | **2.81x** |
| with `rusted` | **1.66x** | **4.86x** |

At default configuration against QoS 0, it is **a dead heat** — pydantic's
codec advantage very nearly cancels Zenoh's transport advantage. Anyone
choosing between the two on raw throughput alone, at this payload size, is
choosing between equals.

Two things move it:

- **QoS 1** — at-least-once delivery, which is what most production MQTT
  fleets actually run — costs MQTT roughly 3x. Against that baseline zeared
  is 2.8x ahead before any accelerator.
- **`rusted`** closes the codec gap, and the transport advantage stops being
  cancelled: 1.66x against QoS 0, 4.86x against QoS 1.

### Caveats

- One payload shape, one machine, loopback for both brokers. No real network
  hop, where MQTT's broker round-trip and Zenoh's peer routing would diverge
  much further.
- The raw rows (`pydantic + …`, `seared + … raw`) publish bytes directly with
  no `Message` class — no topic templating, publisher cache, or schema
  attachment. They exist to isolate an axis, not as a usage recommendation.
- Throughput is one axis of a stack choice. Retained messages, liveliness,
  queryables, and the peer topology itself are zeared features with no MQTT
  equivalent, and they don't appear in any of these numbers.

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
