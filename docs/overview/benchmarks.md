# Benchmarks

End-to-end publish → subscriber-received throughput on a single in-process
Zenoh peer session, compared against `marshmallow` + raw Zenoh to measure
zeared's overhead relative to the prior pattern.

Every number below comes from the committed `bench/results.json` and is
generated from it by `bench/tables.py`:
<!-- bench:run -->zeared 0.4.0, seared 0.4.0, `rusted` 0.4.0, `eclipse-zenoh` 1.10.1, CPython 3.14.3; the median of 3 runs, recorded 2026-10-17<!-- /bench:run -->.
Each is the median of the artifact's runs, and each ratio is a ratio of
medians. Re-record (three runs, as the committed artifact is) and re-render with:

```bash
uv run python -m bench --runs 3
```

`python -m bench.tables --check` fails when a doc has drifted from the
artifact; it runs as a `prek` hook. The accelerated rows need `rusted`
installed for the run (it is declared nowhere): `uv pip install ../rusted`,
then `uv run --no-sync python -m bench --runs 3`, so `uv` doesn't remove it.

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

<!-- bench:wire -->
| Strategy | pub/s | e2e/s | MB/s | wire (B) |
|----------|------:|------:|-----:|---------:|
| Zenoh + `marshmallow` (JSON) | 3,879 | 3,845 | 3.09 | 796 |
| `zeared` (JSON, cached) | 6,697 | 6,625 | 5.33 | 796 |
| `zeared` (msgpack, cached) | 7,725 | 7,705 | 4.12 | 533 |
| `zeared` (msgpack, `PUBLISHER=False`) | 7,344 | 7,286 | 3.91 | 533 |
<!-- /bench:wire -->

msgpack is **<!-- bench:wire-smaller -->33%<!-- /bench:wire-smaller --> smaller on the wire** than the JSON form (<!-- bench:wire-msgpack-bytes -->533<!-- /bench:wire-msgpack-bytes --> B vs <!-- bench:wire-json-bytes -->796<!-- /bench:wire-json-bytes --> B)
for the same payload.

## Throughput suite

A 5 s publish window per strategy: the sustained sync rate, pushed flat out.

<!-- bench:throughput -->
| Strategy | pub/s | e2e/s | MB/s | wire (B) |
|----------|------:|------:|-----:|---------:|
| Zenoh + `marshmallow` (JSON) | 3,839 | 3,688 | 3.06 | 796 |
| sync `send` (JSON, cached) | 6,912 | 6,645 | 5.50 | 796 |
| sync `send` (msgpack, cached) | 7,501 | 7,143 | 4.00 | 533 |
| sync `send` (msgpack, `PUBLISHER=False`) | 7,316 | 7,032 | 3.90 | 533 |
<!-- /bench:throughput -->

## Headline matrix

5 s publish window per strategy, no drops:

<!-- bench:async -->
| Strategy | pub/s | e2e/s | MB/s | wire (B) |
|----------|------:|------:|-----:|---------:|
| Zenoh + `marshmallow` (JSON) | 3,868 | 3,719 | 3.08 | 796 |
| sync `send` + sync `on_message` (JSON) | 6,846 | 6,518 | 5.45 | 796 |
| sync `send` + sync `on_message` (msgpack) | 7,598 | 7,298 | 4.05 | 533 |
| `asend` + sync `on_message` (msgpack) | 3,100 | 2,979 | 1.65 | 533 |
| sync `send` + `alisten` (msgpack) | 5,994 | 5,933 | 3.19 | 533 |
| `asend` + `alisten` (msgpack) | 2,607 | 2,581 | 1.39 | 533 |
| `asend` + `alisten` (JSON) | 2,461 | 2,437 | 1.96 | 796 |
| sync `send` + `async def` `on_message` (msgpack) | 5,265 | 5,212 | 2.81 | 533 |
<!-- /bench:async -->

Relative to the fastest row (sync msgpack — the default), end to end:

- **`marshmallow` is <!-- bench:async-marshmallow -->~49%<!-- /bench:async-marshmallow --> slower and <!-- bench:async-larger -->~49%<!-- /bench:async-larger --> larger on the wire.**
- **Sync JSON is <!-- bench:async-json -->~11%<!-- /bench:async-json --> slower** — the same transport, with a slower codec and a bigger payload.
- **`await asend()` pays a per-call `asyncio.to_thread` hop and is <!-- bench:async-asend -->~59%<!-- /bench:async-asend -->
  slower** than the sync loop. Reach for it when keeping the event loop
  responsive matters more than raw throughput.
- **`alisten` with sync `send` is <!-- bench:async-alisten -->~19%<!-- /bench:async-alisten --> slower** — a cheap way to go async on
  the consumer side without a publish-side tax.
- **`async def` callbacks via `on_message` are <!-- bench:async-asyncdef -->~29%<!-- /bench:async-asyncdef --> slower** — the publish
  path stays sync-fast; each handler dispatches one coroutine per message.

The pub/s vs e2e/s gap stays under <!-- bench:async-gap -->5%<!-- /bench:async-gap --> everywhere: the in-process subscriber
keeps up with the publisher across every strategy.

## Accelerated (seared + `rusted`)

`rusted` is seared's optional compiled (Rust/PyO3) accelerator core. When it
is installed and a class is built entirely from seared-native fields, seared
swaps its generated `load`/`dump` for compiled equivalents — and because
zeared `Message` classes are seared classes, zeared's wire path inherits that
for free. Nothing in zeared declares or imports it.

5,000 messages per strategy:

<!-- bench:rusted -->
| Strategy | pub/s | e2e/s | MB/s | wire (B) |
|----------|------:|------:|-----:|---------:|
| `zeared` + `rusted` (JSON, cached) | 11,396 | 11,382 | 9.07 | 796 |
| `zeared` + `rusted` (msgpack, cached) | 13,351 | 13,345 | 7.12 | 533 |
<!-- /bench:rusted -->

Against the same strategies on the pure-Python path, that is **<!-- bench:rusted-json -->1.72×<!-- /bench:rusted-json --> on the
JSON path and <!-- bench:rusted-msgpack -->1.73×<!-- /bench:rusted-msgpack --> on msgpack**, end to end — serialization is a large
enough share of zeared's per-message cost that removing most of it moves the
whole wire path. Wire sizes are unchanged, as expected: the accelerator
changes how bytes are produced, not what they are.

`rusted` is not a zeared dependency and is not on PyPI yet. Until wheels
ship, the accelerated suite runs only where it is already installed, and
skips with a note everywhere else. The artifact records the exact versions
behind every row.

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

<!-- bench:stacks -->
| Row | e2e/s | wire (B) |
|-----|------:|---------:|
| pydantic + MQTT (QoS 0) | 11,453 | 670 |
| pydantic + MQTT (QoS 1) | 3,153 | 670 |
| seared + MQTT (QoS 0) | 6,188 | 796 |
| seared + MQTT (QoS 1) | 2,646 | 796 |
| pydantic + Zenoh (router, raw) | 25,342 | 670 |
| seared + Zenoh (router, raw) | 7,542 | 796 |
| zeared + Zenoh (router) | 6,670 | 796 |
| zeared + Zenoh (peer) | 6,966 | 796 |
| zeared + Zenoh (peer, msgpack default) | 7,585 | 533 |
| zeared + `rusted` + Zenoh (peer, JSON) | 11,512 | 796 |
| zeared + `rusted` + Zenoh (peer, msgpack default) | 14,509 | 533 |
<!-- /bench:stacks -->

> **Read these on end-to-end rate, not publish rate.** `paho`'s `publish()`
> hands off to a network thread and returns, so the MQTT rows' publish-side
> figures measure *client-side enqueue*, not transmission — on the pydantic
> rows they look <!-- bench:mqtt-enqueue-q0 -->~2x<!-- /bench:mqtt-enqueue-q0 --> the rate the subscriber actually saw at QoS 0,
> and <!-- bench:mqtt-enqueue-q1 -->~6x<!-- /bench:mqtt-enqueue-q1 --> at QoS 1. Zenoh's `put` does the work inline, so its
> two rates track each other. `results.json` records both; only e2e is
> comparable across transports.

### What each pair says

<!-- bench:pairs -->
| Comparison | Result |
|------------|--------|
| **Codec**, same raw Zenoh transport | pydantic **3.4x** faster than seared |
| **Codec**, same MQTT transport (QoS 0) | pydantic **1.9x** faster |
| **Transport**, same pydantic codec | Zenoh **2.2x** faster than MQTT |
| zeared's `Message` wrapper over raw seared + Zenoh | **12%** cost |
| Zenoh peer vs router topology | **4%** faster |
| msgpack vs JSON on the same path | **9%** faster, **33%** smaller |
| `rusted` accelerator on the native stack | **1.9x** |
<!-- /bench:pairs -->

**pydantic wins the codec axis, and by a lot.** That is the expected result,
not a surprise: its core is compiled and seared's is not — seared's own
benchmarks already record pydantic-core as several times faster on the dict
path. The codec gap is narrower over MQTT (the table above) simply because the
transport dominates there and there is less headroom to win.

### Run-to-run variance

Single runs move: across every suite in this artifact, a row's end-to-end
rate varied by up to <!-- bench:spread-max -->18%<!-- /bench:spread-max --> between runs. That is why every number on
this page is a median of the artifact's runs (<!-- bench:runs -->3<!-- /bench:runs -->). How far each of these rows
moved (max/min − 1):

<!-- bench:spread -->
| Row | spread |
|-----|-------:|
| pydantic + MQTT (QoS 0) | 13% |
| pydantic + MQTT (QoS 1) | 3% |
| seared + MQTT (QoS 0) | 3% |
| seared + MQTT (QoS 1) | 3% |
| pydantic + Zenoh (router, raw) | 11% |
| seared + Zenoh (router, raw) | 3% |
| zeared + Zenoh (router) | 8% |
| zeared + Zenoh (peer) | 3% |
| zeared + Zenoh (peer, msgpack default) | 2% |
| zeared + `rusted` + Zenoh (peer, JSON) | 6% |
| zeared + `rusted` + Zenoh (peer, msgpack default) | 8% |
<!-- /bench:spread -->

Anything below roughly 1.2x on this page should be read as "comparable",
not as a win.

### The answer

Against the stack as usually deployed:

<!-- bench:answer -->
| zeared config | vs pydantic + MQTT QoS 0 | vs QoS 1 |
|---------------|-------------------------:|---------:|
| default (msgpack, peer) | **0.66x** | **2.41x** |
| with `rusted` | **1.27x** | **4.60x** |
<!-- /bench:answer -->

**At default configuration against QoS 0, pydantic + MQTT is ahead** — around
<!-- bench:answer-behind -->1.5x<!-- /bench:answer-behind -->. pydantic's codec advantage is larger than Zenoh's transport advantage
at this payload size, and pure-Python seared does not make it back. If raw
QoS 0 throughput on one box is the only thing that matters to you, that is
the honest answer, and it is not the one favouring this library.

Two things change it:

- **QoS 1** — at-least-once delivery, which is what most production MQTT
  fleets actually run — costs MQTT roughly <!-- bench:mqtt-qos1-cost -->4x<!-- /bench:mqtt-qos1-cost -->, and it is the more meaningful
  comparison against Zenoh's reliable `put`. Against that baseline zeared is
  ahead before any accelerator (the table above).
- **`rusted`** closes the codec gap, and the ordering flips against both QoS
  levels (the table above). The accelerator is what makes the native stack
  faster on both axes rather than trading one for the other.

### Caveats

- One payload shape, one machine, loopback for both brokers. No real network
  hop, where MQTT's broker round-trip and Zenoh's peer routing would diverge
  much further.
- The raw rows (`pydantic + …`, `seared + … raw`) publish bytes directly with
  no `Message` class — no topic templating, publisher cache, or schema
  attachment. They exist to isolate an axis, not as a usage recommendation.
- Throughput is one axis of a stack choice. MQTT has its own retained
  messages and Last Will, and MQTT 5 has request/response; only the peer
  topology has no MQTT equivalent. How each side's versions of those compare
  doesn't appear in any of these numbers.

## Why zeared beats `marshmallow` + Zenoh

- **`__slots__` everywhere** in seared field types — no per-instance
  `__dict__`.
- **Pre-baked field spec** computed once at decorator time; each `dump` /
  `load` walks the same `(attr, wire, Field)` triples.
- **Publisher caching** keeps a long-lived `zenoh.Publisher` per concrete
  topic, avoiding the per-send declare cost. On this static-`TOPIC`
  workload the cached and `PUBLISHER=False` rows sit within run-to-run
  noise of each other, in both the wire and throughput tables; it earns
  more on templated `TOPIC`s with repeated concrete keys.
- **msgpack by default** — smaller on the wire and cheaper to encode than
  JSON.

## Caveats

- Single in-process peer session: no network, no serialization across a real
  transport hop. These measure zeared's overhead, not Zenoh's throughput
  ceiling.
- Numbers are from one machine (recorded in `results.json` alongside the
  Python and platform versions); treat ratios as the durable signal, not
  absolute rates.
