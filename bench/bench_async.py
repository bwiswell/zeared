"""Comprehensive duration-based benchmark covering sync and async paths.

Nine strategies on the same nested schema (one outer object with a 20-item
list of 3-field records plus 3 string tags):

Sync baselines:
    1. Zenoh + marshmallow (json)                        — external baseline
    2. Zenoh + zeared (json, cached)
    3. Zenoh + zeared (msgpack, cached)                  — the default
    4. Zenoh + zeared (msgpack, PUBLISHER=False)         — cache off

Async variants (all cached msgpack unless noted):
    5. asend pub + sync on_message sub
    6. sync send pub + alisten sub
    7. asend pub + alisten sub                           — fully async
    8. sync send pub + async-def on_message sub          — coroutine scheduling
    9. asend pub + alisten sub (json)

Reports: sent, pub/s, e2e/s, MB/s, wire size, overhead vs sync-default.

Run directly (marshmallow must be installed ad-hoc):

    uv pip install 'marshmallow>=3.26.1,<4.0'
    uv run python tests/bench_async.py          # 5s per strategy (default)
    uv run python tests/bench_async.py 10       # 10s per strategy

Recorded baseline (2026-04-24, zeared 0.0.8, 10s per strategy on the dev machine):

    strategy                              sent    pub/s   e2e/s   MB/s  wire   overhead
    zenoh + marshmallow (json)         39,508    3,950   3,872   3.14   796   -44.1%
    sync  (json, cached)               62,954    6,295   6,141   5.01   796   -10.9%
    sync  (msgpack, cached) [baseline] 70,638    7,064   6,925   3.77   533     0.0%
    sync  (msgpack, PUBLISHER=False)   68,400    6,833   6,666   3.64   533    -3.3%
    async asend + sync on_message      32,864    3,286   3,221   1.75   533   -53.5%
    sync send  + alisten               62,500    6,249   6,217   3.33   533   -11.5%
    async asend + alisten (msgpack)    28,528    2,853   2,838   1.52   533   -59.6%
    async asend + alisten (json)       26,636    2,664   2,650   2.12   796   -62.3%
    sync send  + async-def on_message  56,405    5,640   5,612   3.01   533   -20.2%

Takeaways: (1) sync msgpack cached is the unambiguous peak for raw throughput.
(2) ``asend``'s per-call ``asyncio.to_thread`` hop is the big async tax —
~55% slower on hot publish loops. (3) ``alisten`` on the receive side only
costs ~12% vs. callback-mode ``on_message`` — a cheap way to go async on
consumers without paying the publish-side tax. (4) Async-def callbacks via
``on_message`` keep the publish path sync-fast (~-20%) by scheduling one
coroutine per message on the running loop.
"""
from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass
from typing import Callable, Optional

import zenoh
from marshmallow import EXCLUDE, Schema
from marshmallow.fields import Float as MFloat
from marshmallow.fields import Integer, List as MList, Nested, String

import zeared as z
from zeared import _codec as codec


# ---------------------------------------------------------------------------
# Schemas (identical shape to bench_wire.py / bench_throughput.py).
# ---------------------------------------------------------------------------

_N_ITEMS = 20
_N_TAGS = 3


def _payload_dict() -> dict:
    return {
        'name': 'demo',
        'items': [{'x': i, 'y': i * 1.5, 'label': f'i{i}'} for i in range(_N_ITEMS)],
        'tags': ['alpha', 'beta', 'gamma'],
    }


class InnerSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    x = Integer(required=True)
    y = MFloat(required=True)
    label = String(load_default=None)


class OuterSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    name = String(required=True)
    items = MList(Nested(InnerSchema()), required=True)
    tags = MList(String(), load_default=[])


_outer_schema = OuterSchema()


@z.zeared
class Inner(z.Zeared):
    x: int = z.Int(required=True)
    y: float = z.Float(required=True)
    label: Optional[str] = z.Str()


def _make_zeared_class(topic: str, encoding: str = 'msgpack', publisher=True):
    """Build a fresh Message subclass per-bench so caches don't collide."""

    @z.zeared
    class Outer(z.Message):
        TOPIC = topic
        ENCODING = encoding
        PUBLISHER = publisher
        name: str = z.Str(required=True)
        items: list = z.T(Inner, many=True, required=True)
        tags: list = z.Str(many=True, missing=[])

    return Outer


# ---------------------------------------------------------------------------
# Result / harness.
# ---------------------------------------------------------------------------

@dataclass
class Result:
    label: str
    sent: int
    received: int
    publish_secs: float
    total_secs: float
    wire_bytes: int

    @property
    def pub_rate(self) -> float:
        return self.sent / self.publish_secs if self.publish_secs > 0 else 0.0

    @property
    def e2e_rate(self) -> float:
        return self.received / self.total_secs if self.total_secs > 0 else 0.0

    @property
    def mb_per_sec(self) -> float:
        return self.pub_rate * self.wire_bytes / 1_000_000

    @property
    def drops(self) -> int:
        return self.sent - self.received


def _peer_session() -> zenoh.Session:
    c = zenoh.Config()
    c.insert_json5('mode', '"peer"')
    c.insert_json5('scouting/multicast/enabled', 'false')
    return zenoh.open(c)


def _drain(received_ref, sent, t_pub_end) -> float:
    """Wait until subscriber catches up; return wall-clock time when stable."""
    last = -1
    stable = 0
    while received_ref[0] < sent or stable < 3:
        if received_ref[0] == last:
            stable += 1
        else:
            stable = 0
        last = received_ref[0]
        time.sleep(0.05)
        if time.perf_counter() - t_pub_end > 15.0:
            break  # safety
    return time.perf_counter()


# --- Sync strategies ---

def _sync_marshmallow(session, duration_s) -> Result:
    topic = 'bench/async/marshmallow'
    payload = _payload_dict()
    raw_ref = _outer_schema.dumps(payload).encode('utf-8')

    received = [0]

    def handler(sample):
        _outer_schema.loads(bytes(sample.payload).decode('utf-8'))
        received[0] += 1

    sub = session.declare_subscriber(topic, handler)
    time.sleep(0.15)

    sent = 0
    t0 = time.perf_counter()
    deadline = t0 + duration_s
    while time.perf_counter() < deadline:
        session.put(topic, raw_ref, encoding='application/json')
        sent += 1
    t_pub = time.perf_counter()
    t_end = _drain(received, sent, t_pub)
    sub.undeclare()
    return Result(
        'zenoh + marshmallow (json)',
        sent, received[0], t_pub - t0, t_end - t0, len(raw_ref),
    )


def _sync_zeared(session, duration_s, msg_cls, label) -> Result:
    z.session = session
    instance = msg_cls.load(_payload_dict())
    effective = codec.effective_encoding(msg_cls.ENCODING, z.debug)
    wire = len(codec.pack(msg_cls.dump(instance), effective))

    received = [0]
    sub = msg_cls.on_message(lambda m: received.__setitem__(0, received[0] + 1))
    time.sleep(0.15)

    sent = 0
    t0 = time.perf_counter()
    deadline = t0 + duration_s
    while time.perf_counter() < deadline:
        instance.send()
        sent += 1
    t_pub = time.perf_counter()
    t_end = _drain(received, sent, t_pub)
    sub.close()
    return Result(label, sent, received[0], t_pub - t0, t_end - t0, wire)


# --- Async strategies ---

def _async_asend_sync_sub(session, duration_s, msg_cls, label) -> Result:
    z.session = session
    instance = msg_cls.load(_payload_dict())
    effective = codec.effective_encoding(msg_cls.ENCODING, z.debug)
    wire = len(codec.pack(msg_cls.dump(instance), effective))

    received = [0]
    sub = msg_cls.on_message(lambda m: received.__setitem__(0, received[0] + 1))
    time.sleep(0.15)

    async def pub():
        nonlocal_sent = 0
        t0 = time.perf_counter()
        deadline = t0 + duration_s
        while time.perf_counter() < deadline:
            await instance.asend()
            nonlocal_sent += 1
        return nonlocal_sent, t0, time.perf_counter()

    sent, t0, t_pub = asyncio.run(pub())
    t_end = _drain(received, sent, t_pub)
    sub.close()
    return Result(label, sent, received[0], t_pub - t0, t_end - t0, wire)


async def _run_alisten_consumer_with(
    msg_cls,
    duration_s: float,
    do_publish,  # async callable: await-driven loop that issues publishes
) -> tuple[int, int, float, float]:
    """Shared scaffold for ``alisten``-based strategies.

    Spawns a consumer task that accumulates messages; runs ``do_publish`` for
    up to ``duration_s`` wall-clock; drains for up to 3 s; then cancels the
    consumer (which closes the underlying subscriber via the async generator's
    ``finally``). Returns (sent, received, t_pub_end, t_end) relative to the
    caller-owned ``t0``.
    """
    received = 0

    async def consumer():
        nonlocal received
        async for _m in msg_cls.alisten():
            received += 1

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.15)  # subscriber settle

    sent, t_pub_end = await do_publish(duration_s)

    # Drain: wait up to 3 s for received to catch up to sent.
    deadline = t_pub_end + 3.0
    while received < sent and time.perf_counter() < deadline:
        await asyncio.sleep(0.05)
    t_end = time.perf_counter()

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    return sent, received, t_pub_end, t_end


def _sync_send_async_listen(session, duration_s, msg_cls, label) -> Result:
    z.session = session
    instance = msg_cls.load(_payload_dict())
    effective = codec.effective_encoding(msg_cls.ENCODING, z.debug)
    wire = len(codec.pack(msg_cls.dump(instance), effective))

    async def main():
        t0 = time.perf_counter()

        async def do_publish(dur):
            sent = 0
            deadline = t0 + dur
            while time.perf_counter() < deadline:
                instance.send()                          # sync
                sent += 1
                if sent % 500 == 0:
                    await asyncio.sleep(0)               # yield so consumer drains
            return sent, time.perf_counter()

        sent, received, t_pub, t_end = await _run_alisten_consumer_with(
            msg_cls, duration_s, do_publish,
        )
        return Result(label, sent, received, t_pub - t0, t_end - t0, wire)

    return asyncio.run(main())


def _full_async(session, duration_s, msg_cls, label) -> Result:
    z.session = session
    instance = msg_cls.load(_payload_dict())
    effective = codec.effective_encoding(msg_cls.ENCODING, z.debug)
    wire = len(codec.pack(msg_cls.dump(instance), effective))

    async def main():
        t0 = time.perf_counter()

        async def do_publish(dur):
            sent = 0
            deadline = t0 + dur
            while time.perf_counter() < deadline:
                await instance.asend()                   # async
                sent += 1
            return sent, time.perf_counter()

        sent, received, t_pub, t_end = await _run_alisten_consumer_with(
            msg_cls, duration_s, do_publish,
        )
        return Result(label, sent, received, t_pub - t0, t_end - t0, wire)

    return asyncio.run(main())


def _sync_send_async_cb(session, duration_s, msg_cls, label) -> Result:
    """Sync send; handler is an ``async def`` scheduled onto the loop."""
    z.session = session
    instance = msg_cls.load(_payload_dict())
    effective = codec.effective_encoding(msg_cls.ENCODING, z.debug)
    wire = len(codec.pack(msg_cls.dump(instance), effective))

    async def main():
        received = 0
        sent = 0
        t0 = time.perf_counter()

        async def handler(_m):
            nonlocal received
            received += 1

        sub = msg_cls.on_message(handler)
        await asyncio.sleep(0.15)

        deadline = t0 + duration_s
        while time.perf_counter() < deadline:
            instance.send()
            sent += 1
            if sent % 500 == 0:
                await asyncio.sleep(0)  # let handler coroutines run
        t_pub = time.perf_counter()

        # Drain up to 3 s.
        drain_deadline = t_pub + 3.0
        while received < sent and time.perf_counter() < drain_deadline:
            await asyncio.sleep(0.05)
        t_end = time.perf_counter()
        sub.close()

        return Result(label, sent, received, t_pub - t0, t_end - t0, wire)

    return asyncio.run(main())


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------

def run(duration_s: float = 5.0) -> None:
    session = _peer_session()
    try:
        # Build fresh classes each time (unique TOPIC) so publisher caches
        # don't persist across runs.
        msgp_cached    = _make_zeared_class('bench/async/sync/msgp_cached',    'msgpack', True)
        msgp_nocache   = _make_zeared_class('bench/async/sync/msgp_nocache',   'msgpack', False)
        json_cached    = _make_zeared_class('bench/async/sync/json_cached',    'json',    True)
        a_msgp_syncsub = _make_zeared_class('bench/async/asend_syncsub/msgp',  'msgpack', True)
        s_msgp_alsn    = _make_zeared_class('bench/async/sendsync_alisten/msgp', 'msgpack', True)
        full_a_msgp    = _make_zeared_class('bench/async/full_async/msgp',     'msgpack', True)
        full_a_json    = _make_zeared_class('bench/async/full_async/json',     'json',    True)
        asyncb_msgp    = _make_zeared_class('bench/async/asynccb/msgp',        'msgpack', True)

        # Warm-up each path briefly.
        _sync_marshmallow(session, 0.3)
        _sync_zeared(session, 0.3, msgp_cached, 'warm')
        _async_asend_sync_sub(session, 0.3, a_msgp_syncsub, 'warm')
        _sync_send_async_listen(session, 0.3, s_msgp_alsn, 'warm')
        _full_async(session, 0.3, full_a_msgp, 'warm')
        _sync_send_async_cb(session, 0.3, asyncb_msgp, 'warm')

        results = [
            _sync_marshmallow(session, duration_s),
            _sync_zeared(session, duration_s, json_cached,   'sync  (json,    cached)'),
            _sync_zeared(session, duration_s, msgp_cached,   'sync  (msgpack, cached)'),
            _sync_zeared(session, duration_s, msgp_nocache,  'sync  (msgpack, PUBLISHER=False)'),
            _async_asend_sync_sub(session, duration_s, a_msgp_syncsub, 'async asend + sync on_message'),
            _sync_send_async_listen(session, duration_s, s_msgp_alsn,  'sync send  + alisten'),
            _full_async(session, duration_s, full_a_msgp, 'async asend + alisten (msgpack)'),
            _full_async(session, duration_s, full_a_json, 'async asend + alisten (json)'),
            _sync_send_async_cb(session, duration_s, asyncb_msgp, 'sync send  + async-def on_message'),
        ]

        # Report.
        print()
        print(f'Schema: Outer(name, items[{_N_ITEMS}x Inner], tags[{_N_TAGS}])')
        print(f'Duration target: {duration_s:.1f}s publish window per strategy')
        print('-' * 104)
        hdr = (
            f'{"strategy":42s}  {"sent":>8s}  '
            f'{"pub/s":>9s}  {"e2e/s":>9s}  {"MB/s":>6s}  {"wire":>6s}  {"drops":>6s}'
        )
        print(hdr)
        print('-' * 104)
        for r in results:
            print(
                f'{r.label:42s}  '
                f'{r.sent:>8,}  '
                f'{r.pub_rate:>9,.0f}  '
                f'{r.e2e_rate:>9,.0f}  '
                f'{r.mb_per_sec:>6.2f}  '
                f'{r.wire_bytes:>6d}  '
                f'{r.drops:>6d}'
            )
        print('-' * 104)

        # Sanity: no drops, e2e within 5% of pub_rate.
        print()
        for r in results:
            assert r.drops == 0, f'{r.label}: {r.drops} drops'
            if r.pub_rate > 0:
                gap = abs(r.pub_rate - r.e2e_rate) / r.pub_rate
                if gap > 0.15:
                    print(f'WARNING  {r.label}: pub/e2e gap is {gap * 100:.1f}%')

        # Overhead view: compare every row to sync msgpack cached (the default).
        baseline = next(r for r in results if r.label == 'sync  (msgpack, cached)')
        print(f'Overhead vs baseline ({baseline.label.strip()}):')
        for r in results:
            if r.label == baseline.label:
                continue
            ratio = r.pub_rate / baseline.pub_rate if baseline.pub_rate > 0 else 0
            tag = '+' if ratio >= 1 else ''
            print(f'  {r.label:42s}  {tag}{ratio * 100 - 100:6.1f}%')
    finally:
        session.close()


if __name__ == '__main__':
    dur = 5.0 if len(sys.argv) < 2 else float(sys.argv[1])
    run(dur)
