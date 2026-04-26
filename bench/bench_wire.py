"""End-to-end Zenoh round-trip benchmark.

Three strategies on the same schema:
    1. Zenoh + marshmallow (raw Zenoh, marshmallow JSON serialization)
    2. Zenoh + zeared (JSON encoding)
    3. Zenoh + zeared (msgpack encoding, default)

Measures:
    - wire size per message (bytes)
    - publish → subscriber-received round-trip throughput (msgs/s)
    - amortised per-message latency (µs)

Not collected by pytest (filename lacks the ``test_`` prefix). Run directly:

    uv pip install 'marshmallow>=3.26.1,<4.0'
    uv run python tests/bench_wire.py

marshmallow is NOT a zeared dependency; install it ad-hoc before running.

Recorded baseline (2026-04-24, zeared 0.0.8, single in-process Zenoh peer, N=5,000):
    strategy                                      msgs/s  us/msg  wire (B)
    zenoh + marshmallow (json)                     2,962  337.62       796
    zenoh + zeared      (json, cached)             6,389  156.51       796  2.16x
    zenoh + zeared      (msgpack, cached)          7,139  140.07       533  2.41x
    zenoh + zeared      (msgpack, PUBLISHER=False) 6,904  144.85       533  2.33x

The static-TOPIC bench shows a modest ~3% gain from the default cached
publisher (zenoh session.put is already fast in Rust). The cache earns
more on templated TOPICs with repeated concrete keys and on higher-
frequency publishing — separate benches would need to showcase that.
"""
from __future__ import annotations

import threading
import time
from typing import Optional

import zenoh
from marshmallow import EXCLUDE, Schema, post_load
from marshmallow.fields import Float as MFloat
from marshmallow.fields import Integer, List as MList, Nested, String

import zeared as z
from zeared import _codec as codec


# ----------------------------------------------------------------------------
# Schema definitions — one payload, three representations.
# ----------------------------------------------------------------------------

_N_ITEMS = 20
_N_TAGS = 3


def _payload_dict() -> dict:
    return {
        'name': 'demo',
        'items': [{'x': i, 'y': i * 1.5, 'label': f'i{i}'} for i in range(_N_ITEMS)],
        'tags': ['alpha', 'beta', 'gamma'],
    }


# --- marshmallow ---

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


# --- zeared ---

@z.zeared
class Inner(z.Zeared):
    x: int = z.Int(required=True)
    y: float = z.Float(required=True)
    label: Optional[str] = z.Str()


@z.zeared
class OuterJson(z.Message):
    TOPIC = 'bench/zeared/json'
    ENCODING = 'json'
    name: str = z.Str(required=True)
    items: list = z.T(Inner, many=True, required=True)
    tags: list = z.Str(many=True, missing=[])


@z.zeared
class OuterMsgpack(z.Message):
    TOPIC = 'bench/zeared/msgpack'
    ENCODING = 'msgpack'
    # PUBLISHER defaults to True — long-lived publisher cached per concrete topic
    name: str = z.Str(required=True)
    items: list = z.T(Inner, many=True, required=True)
    tags: list = z.Str(many=True, missing=[])


@z.zeared
class OuterMsgpackNoCache(z.Message):
    TOPIC = 'bench/zeared/msgpack_nocache'
    ENCODING = 'msgpack'
    PUBLISHER = False   # every send uses session.put() directly (0.0.1 behaviour)
    name: str = z.Str(required=True)
    items: list = z.T(Inner, many=True, required=True)
    tags: list = z.Str(many=True, missing=[])


# ----------------------------------------------------------------------------
# Benchmark harness.
# ----------------------------------------------------------------------------

def _peer_session() -> zenoh.Session:
    c = zenoh.Config()
    c.insert_json5('mode', '"peer"')
    c.insert_json5('scouting/multicast/enabled', 'false')
    return zenoh.open(c)


def _run_zenoh_marshmallow(session: zenoh.Session, n: int) -> tuple[float, int]:
    """Raw Zenoh + marshmallow JSON round-trip."""
    topic = 'bench/marshmallow/feed'
    done = threading.Event()
    count = [0]

    def on_sample(sample):
        # Decode to ensure apples-to-apples comparison.
        _outer_schema.loads(bytes(sample.payload).decode('utf-8'))
        count[0] += 1
        if count[0] >= n:
            done.set()

    sub = session.declare_subscriber(topic, on_sample)
    time.sleep(0.1)  # let subscriber settle

    payload = _payload_dict()
    # Ensure a single reference msg for wire-size comparison.
    sample_bytes = _outer_schema.dumps(payload).encode('utf-8')
    wire_size = len(sample_bytes)

    t0 = time.perf_counter()
    for _ in range(n):
        data = _outer_schema.dumps(payload).encode('utf-8')
        session.put(topic, data, encoding='application/json')
    done.wait(timeout=30)
    elapsed = time.perf_counter() - t0

    sub.undeclare()
    return elapsed, wire_size


def _run_zeared(
    session: zenoh.Session,
    n: int,
    msg_cls,
) -> tuple[float, int]:
    """zeared round-trip via Message.on_message + send."""
    done = threading.Event()
    count = [0]

    def on_msg(m):
        count[0] += 1
        if count[0] >= n:
            done.set()

    z.session = session
    sub = msg_cls.on_message(on_msg)
    time.sleep(0.1)

    payload = _payload_dict()
    sample_instance = msg_cls.load(payload)

    # Wire size: serialize one sample the same way send() does.
    effective = codec.effective_encoding(msg_cls.ENCODING, z.debug)
    data = msg_cls.dump(sample_instance)
    wire_size = len(codec.pack(data, effective))

    t0 = time.perf_counter()
    for _ in range(n):
        sample_instance.send()
    done.wait(timeout=30)
    elapsed = time.perf_counter() - t0

    sub.close()
    return elapsed, wire_size


def run(n: int = 5_000) -> None:
    session = _peer_session()
    try:
        # Warm-up to avoid first-declare overhead dominating small-N runs.
        _run_zenoh_marshmallow(session, 200)
        _run_zeared(session, 200, OuterJson)
        _run_zeared(session, 200, OuterMsgpack)
        _run_zeared(session, 200, OuterMsgpackNoCache)

        results: list[tuple[str, float, int]] = []

        label = 'zenoh + marshmallow (json)'
        t, wire = _run_zenoh_marshmallow(session, n)
        results.append((label, t, wire))

        label = 'zenoh + zeared (json, cached)'
        t, wire = _run_zeared(session, n, OuterJson)
        results.append((label, t, wire))

        label = 'zenoh + zeared (msgpack, cached)'
        t, wire = _run_zeared(session, n, OuterMsgpack)
        results.append((label, t, wire))

        label = 'zenoh + zeared (msgpack, PUBLISHER=False)'
        t, wire = _run_zeared(session, n, OuterMsgpackNoCache)
        results.append((label, t, wire))

        print()
        print(f'Schema: Outer(name, items[{_N_ITEMS}×Inner], tags[{_N_TAGS}])')
        print(f'N = {n:,}  |  publish → subscriber-received end-to-end')
        print('-' * 78)
        print(f'{"strategy":42s}  {"msgs/s":>12s}  {"µs/msg":>10s}  {"wire (B)":>10s}')
        print('-' * 78)
        for label, elapsed, wire in results:
            ops = n / elapsed
            per = elapsed * 1e6 / n
            print(f'{label:42s}  {ops:>12,.0f}  {per:>10.2f}  {wire:>10d}')
        print('-' * 78)

        baseline = results[0][1]
        print()
        print('Relative to zenoh + marshmallow (JSON) baseline:')
        for label, elapsed, _ in results:
            speed = baseline / elapsed
            print(f'  {label:42s}  {speed:>5.2f}x')
    finally:
        session.close()


if __name__ == '__main__':
    run()
