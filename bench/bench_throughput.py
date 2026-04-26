"""Duration-based end-to-end throughput benchmark.

Each strategy publishes for a fixed wall-clock duration, then waits for the
in-process subscriber to drain. Reports both rates (publisher-side and
end-to-end) plus effective bandwidth on the wire.

Four strategies, same nested schema as ``bench_wire.py``:

    1. Zenoh + marshmallow (raw Zenoh + JSON)
    2. Zenoh + zeared (JSON, cached publishers — default)
    3. Zenoh + zeared (msgpack, cached publishers — default)
    4. Zenoh + zeared (msgpack, PUBLISHER=False — no cache)

marshmallow is NOT a zeared dependency; install ad-hoc:

    uv pip install 'marshmallow>=3.26.1,<4.0'
    uv run python tests/bench_throughput.py          # 5s per strategy (default)
    uv run python tests/bench_throughput.py 10       # 10s per strategy

Recorded baseline (2026-04-24, zeared 0.0.8, 10s per strategy on the dev machine):

    strategy                                     sent   pub/s   e2e/s   MB/s   wire
    zenoh + marshmallow (json)                40,075   3,994   3,896   3.18    796
    zenoh + zeared (json, cached)             62,582   6,258   6,105   4.98    796  1.57x
    zenoh + zeared (msgpack, cached)          69,231   6,923   6,754   3.69    533  1.73x
    zenoh + zeared (msgpack, PUBLISHER=False) 66,027   6,601   6,471   3.52    533  1.65x

Publish rate vs end-to-end rate gap is under 3% for every strategy — the
in-process subscriber keeps up with the publisher, so these numbers reflect
steady-state throughput (not queue drain). No messages dropped.
"""
from __future__ import annotations

import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

import zenoh
from marshmallow import EXCLUDE, Schema
from marshmallow.fields import Float as MFloat
from marshmallow.fields import Integer, List as MList, Nested, String

import zeared as z
from zeared import _codec as codec


# ----------------------------------------------------------------------------
# Schemas (identical to bench_wire.py).
# ----------------------------------------------------------------------------

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


@z.zeared
class OuterJson(z.Message):
    TOPIC = 'bench/throughput/zeared/json'
    ENCODING = 'json'
    name: str = z.Str(required=True)
    items: list = z.T(Inner, many=True, required=True)
    tags: list = z.Str(many=True, missing=[])


@z.zeared
class OuterMsgpack(z.Message):
    TOPIC = 'bench/throughput/zeared/msgpack'
    ENCODING = 'msgpack'
    name: str = z.Str(required=True)
    items: list = z.T(Inner, many=True, required=True)
    tags: list = z.Str(many=True, missing=[])


@z.zeared
class OuterMsgpackNoCache(z.Message):
    TOPIC = 'bench/throughput/zeared/msgpack_nocache'
    ENCODING = 'msgpack'
    PUBLISHER = False
    name: str = z.Str(required=True)
    items: list = z.T(Inner, many=True, required=True)
    tags: list = z.Str(many=True, missing=[])


# ----------------------------------------------------------------------------
# Harness.
# ----------------------------------------------------------------------------

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


def _peer_session() -> zenoh.Session:
    c = zenoh.Config()
    c.insert_json5('mode', '"peer"')
    c.insert_json5('scouting/multicast/enabled', 'false')
    return zenoh.open(c)


def _run(
    label: str,
    session: zenoh.Session,
    duration_s: float,
    publish: Callable[[], None],
    subscribe: Callable[[Callable[[], None]], object],
    undeclare: Callable[[object], None],
    wire_bytes: int,
) -> Result:
    received = [0]
    done_event = threading.Event()

    def on_each():
        received[0] += 1

    sub = subscribe(on_each)
    time.sleep(0.15)   # let subscriber settle

    t_start = time.perf_counter()
    deadline = t_start + duration_s
    sent = 0
    while time.perf_counter() < deadline:
        publish()
        sent += 1
    t_pub_end = time.perf_counter()

    # Drain: wait until received count stops growing AND meets sent.
    last = -1
    stable_ticks = 0
    while received[0] < sent or stable_ticks < 3:
        if received[0] == last:
            stable_ticks += 1
        else:
            stable_ticks = 0
        last = received[0]
        time.sleep(0.05)
        if time.perf_counter() - t_pub_end > 15.0:
            break  # safety
    t_end = time.perf_counter()

    undeclare(sub)
    return Result(
        label=label,
        sent=sent,
        received=received[0],
        publish_secs=t_pub_end - t_start,
        total_secs=t_end - t_start,
        wire_bytes=wire_bytes,
    )


# ----------------------------------------------------------------------------
# Strategy wrappers.
# ----------------------------------------------------------------------------

def _strat_marshmallow(session, duration_s):
    topic = 'bench/throughput/marshmallow'
    payload = _payload_dict()
    raw = _outer_schema.dumps(payload).encode('utf-8')

    def publish():
        session.put(topic, raw, encoding='application/json')

    def subscribe(on_each):
        def handler(sample):
            _outer_schema.loads(bytes(sample.payload).decode('utf-8'))
            on_each()
        return session.declare_subscriber(topic, handler)

    def undeclare(sub):
        sub.undeclare()

    return _run(
        'zenoh + marshmallow (json)',
        session, duration_s,
        publish, subscribe, undeclare,
        wire_bytes=len(raw),
    )


def _strat_zeared(session, duration_s, msg_cls, label):
    z.session = session
    instance = msg_cls.load(_payload_dict())
    effective = codec.effective_encoding(msg_cls.ENCODING, z.debug)
    data = msg_cls.dump(instance)
    wire = len(codec.pack(data, effective))

    def publish():
        instance.send()

    def subscribe(on_each):
        return msg_cls.on_message(lambda m: on_each())

    def undeclare(sub):
        sub.close()

    return _run(label, session, duration_s, publish, subscribe, undeclare, wire)


def run(duration_s: float = 5.0) -> None:
    session = _peer_session()
    try:
        # Warm everything up so the first measured run isn't paying first-declare
        # costs disproportionately.
        _strat_marshmallow(session, 0.3)
        _strat_zeared(session, 0.3, OuterJson, 'warmup/json')
        _strat_zeared(session, 0.3, OuterMsgpack, 'warmup/msgpack')
        _strat_zeared(session, 0.3, OuterMsgpackNoCache, 'warmup/nocache')

        # Real runs.
        results = [
            _strat_marshmallow(session, duration_s),
            _strat_zeared(session, duration_s, OuterJson, 'zenoh + zeared (json, cached)'),
            _strat_zeared(session, duration_s, OuterMsgpack, 'zenoh + zeared (msgpack, cached)'),
            _strat_zeared(session, duration_s, OuterMsgpackNoCache,
                          'zenoh + zeared (msgpack, PUBLISHER=False)'),
        ]

        # Report.
        print()
        print(f'Schema: Outer(name, items[{_N_ITEMS}x Inner], tags[{_N_TAGS}])')
        print(f'Duration target: {duration_s:.1f}s per strategy (publish window; drain untimed)')
        print('-' * 98)
        hdr = f'{"strategy":42s}  {"sent":>8s}  {"pub/s":>9s}  {"e2e/s":>9s}  {"MB/s":>6s}  {"wire":>6s}'
        print(hdr)
        print('-' * 98)
        for r in results:
            drop = r.sent - r.received
            note = '' if drop == 0 else f' (-{drop} dropped)'
            print(
                f'{r.label:42s}  '
                f'{r.sent:>8,}  '
                f'{r.pub_rate:>9,.0f}  '
                f'{r.e2e_rate:>9,.0f}  '
                f'{r.mb_per_sec:>6.2f}  '
                f'{r.wire_bytes:>6d}{note}'
            )
        print('-' * 98)

        baseline = results[0].pub_rate
        print()
        print('Publish-rate vs zenoh + marshmallow (JSON):')
        for r in results:
            print(f'  {r.label:42s}  {r.pub_rate / baseline:>5.2f}x')
    finally:
        session.close()


if __name__ == '__main__':
    dur = 5.0 if len(sys.argv) < 2 else float(sys.argv[1])
    run(dur)
