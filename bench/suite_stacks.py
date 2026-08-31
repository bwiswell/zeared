"""Stack comparison: Pydantic over MQTT vs seared over Zenoh.

The two realistic stacks for a typed Python pub/sub system, and the suite
prospective users actually want. It exists to answer that question *honestly*,
which means refusing to answer it with one number.

The comparison spans two independent axes — codec (pydantic-core vs seared)
and transport (broker-mediated MQTT vs Zenoh). A single headline would
conflate them, and would conflate them in zeared's favour, because the rest
of this bench runs an in-process Zenoh peer with no broker while any MQTT
number pays a hop through a broker process. So the rows below hold one axis
fixed at a time:

* pydantic over MQTT — the stack being asked about.
* pydantic over Zenoh — same codec, different transport: the transport delta.
* seared over MQTT — same transport, different codec: the codec delta.
* zeared over Zenoh (router) — the native stack at a comparable topology.
* seared over Zenoh — same raw transport as the row above, so the remaining
  gap to the native row is zeared's own ``Message`` wrapper, not the codec.
* zeared over Zenoh (router) — the native stack at a comparable topology.
* zeared over Zenoh (peer) — what Zenoh's peer topology additionally buys.

Reading the rows pairwise gives a clean decomposition: pydantic-vs-seared on
one transport is the **codec** delta; one codec across MQTT and Zenoh is the
**transport** delta; raw-seared-vs-zeared on Zenoh is the **wrapper** cost.

One measurement caveat, stated here because it would otherwise flatter MQTT:
``paho``'s ``publish()`` hands off to a network thread and returns, so the
publish-side rate for the MQTT rows measures *client-side enqueue*, not
transmission. Zenoh's ``put`` does the work inline, so its two rates track
each other. **Compare these rows on end-to-end rate**, which is what the
subscriber actually observed in both cases.

Every row is JSON on the wire so the codec and transport deltas aren't
confounded by encoding; the msgpack default is measured in ``suite_wire`` and
appears here as one labelled reference row.

Skipped with a note when ``mosquitto`` isn't installed.
"""

from __future__ import annotations

import json
import threading
import time
from importlib.metadata import version as dist_version
from typing import TYPE_CHECKING, Any

import seared as s
from paho.mqtt import client as mqtt
from pydantic import BaseModel

import zeared as z

from . import _brokers
from .harness import DEFAULT_ITERATIONS, Inner, InnerPy, Run, payload, settle, versions, wire_size

if TYPE_CHECKING:
    import zenoh

SUITE = 'stacks'

#: Seconds to wait for the subscriber to receive everything published. QoS 0
#: is allowed to drop, so this must be able to expire without hanging the run.
_COLLECT_TIMEOUT_S = 15.0


# --------------------------------------------------------------------------
# Payload models — one shape, three codecs.
# --------------------------------------------------------------------------


class PInner(BaseModel):
    """Inner record — pydantic mirror of ``harness.Inner``."""

    x: int
    y: float
    label: str | None = None


class POuter(BaseModel):
    """Outer payload — pydantic mirror of the suites' message classes."""

    name: str
    items: list[PInner]
    tags: list[str] = []


@s.seared(accel=False)
class SInner(s.Seared):
    """Inner record for the raw-transport seared rows."""

    x: int = s.Int(required=True)
    y: float = s.Float(required=True)
    label: str | None = s.Str(default=None)


@s.seared(accel=False)
class SOuter(s.Seared):
    """Outer payload for the raw-transport seared rows — no ``Message`` wrapper."""

    name: str = s.Str(required=True)
    items: list[SInner] = s.T(SInner, many=True, required=True)
    tags: list[str] = s.Str(many=True, default_factory=list)


def _zeared_message(topic: str, encoding: str) -> type[z.Message]:
    """A native zeared ``Message`` on ``topic`` — the full wire path."""

    @z.zeared(accel=False)
    class Outer(z.Message):
        TOPIC = topic
        ENCODING = encoding
        name: str = z.Str(required=True)
        items: list[InnerPy] = z.T(InnerPy, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    return Outer


# --------------------------------------------------------------------------
# Codecs — each stack's own fastest path to and from bytes.
# --------------------------------------------------------------------------


def _pydantic_codec(data: dict[str, Any]) -> tuple[Any, Any, int]:
    """``(encode, decode, wire_bytes)`` for pydantic, using its compiled JSON path.

    ``encode`` runs per publish. Encoding once and republishing the same blob
    would measure the transport with the codec removed — which is precisely
    the axis this suite exists to isolate.
    """
    model = POuter(**data)

    def encode() -> bytes:
        return model.model_dump_json().encode('utf-8')

    return encode, POuter.model_validate_json, len(encode())


def _seared_codec(data: dict[str, Any]) -> tuple[Any, Any, int]:
    """``(encode, decode, wire_bytes)`` for seared over a raw transport."""
    instance = SOuter.load(data)

    def encode() -> bytes:
        return json.dumps(SOuter.dump(instance)).encode('utf-8')

    def decode(raw: bytes) -> SOuter:
        return SOuter.load(json.loads(raw.decode('utf-8')))

    return encode, decode, len(encode())


# --------------------------------------------------------------------------
# Transports.
# --------------------------------------------------------------------------


def _run_mqtt(  # noqa: PLR0913, PLR0917
    strategy: str, version: str, port: int, encode: Any, decode: Any, n: int, qos: int, wire: int
) -> Run:
    """Encode-and-publish ``n`` messages through mosquitto; decode every one."""
    topic = f'bench/stacks/qos{qos}'
    received = [0]
    done = threading.Event()

    def on_message(_c: Any, _u: Any, msg: mqtt.MQTTMessage) -> None:
        decode(msg.payload)
        received[0] += 1
        if received[0] >= n:
            done.set()

    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    sub.on_message = on_message
    sub.connect('127.0.0.1', port)
    sub.subscribe(topic, qos=qos)
    sub.loop_start()

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    pub.max_inflight_messages_set(65535)
    pub.max_queued_messages_set(0)
    pub.connect('127.0.0.1', port)
    pub.loop_start()
    settle()

    t0 = time.perf_counter()
    for _ in range(n):
        pub.publish(topic, encode(), qos=qos)
    t_pub = time.perf_counter()
    done.wait(timeout=_COLLECT_TIMEOUT_S)
    t_end = time.perf_counter()

    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()
    return Run(strategy, version, n, received[0], t_pub - t0, t_end - t0, wire)


def _run_zenoh_raw(  # noqa: PLR0913, PLR0917
    strategy: str, version: str, session: Any, sub_session: Any, encode: Any, decode: Any, n: int, wire: int
) -> Run:
    """Encode-and-publish ``n`` messages over raw Zenoh; decode every one."""
    topic = 'bench/stacks/raw'
    received = [0]
    done = threading.Event()

    def on_sample(sample: zenoh.Sample) -> None:
        decode(bytes(sample.payload))
        received[0] += 1
        if received[0] >= n:
            done.set()

    sub = sub_session.declare_subscriber(topic, on_sample)
    settle()

    t0 = time.perf_counter()
    for _ in range(n):
        session.put(topic, encode(), encoding='application/json')
    t_pub = time.perf_counter()
    done.wait(timeout=_COLLECT_TIMEOUT_S)
    t_end = time.perf_counter()

    sub.undeclare()
    return Run(strategy, version, n, received[0], t_pub - t0, t_end - t0, wire)


def _run_zeared(  # noqa: PLR0913, PLR0917
    strategy: str,
    version: str,
    msg_cls: type[z.Message],
    data: dict[str, Any],
    pub_session: Any,
    sub_session: Any,
    n: int,
) -> Run:
    """Publish ``n`` messages through zeared's native ``Message`` path."""
    instance = msg_cls.load(data)
    received = [0]
    done = threading.Event()

    def on_msg(_m: z.Message) -> None:
        received[0] += 1
        if received[0] >= n:
            done.set()

    sub = msg_cls.on_message(on_msg, session=sub_session)
    settle()

    t0 = time.perf_counter()
    for _ in range(n):
        instance.send(session=pub_session)
    t_pub = time.perf_counter()
    done.wait(timeout=_COLLECT_TIMEOUT_S)
    t_end = time.perf_counter()

    sub.close()
    return Run(strategy, version, n, received[0], t_pub - t0, t_end - t0, wire_size(msg_cls, data))


# --------------------------------------------------------------------------
# Suite.
# --------------------------------------------------------------------------


def run(session: zenoh.Session, n: int = DEFAULT_ITERATIONS) -> list[Run]:
    """Time each (codec, transport) combination over ``n`` messages."""
    if not _brokers.have_mosquitto():
        print('suite_stacks: skipped (mosquitto not installed)')
        return []

    data = payload()
    pyd_encode, pyd_decode, pyd_wire = _pydantic_codec(data)
    sea_encode, sea_decode, sea_wire = _seared_codec(data)
    pyd_version = f'pydantic {dist_version("pydantic")}'
    mqtt_version = f'paho-mqtt {dist_version("paho-mqtt")}'
    runs = []

    try:
        with _brokers.mosquitto() as port:
            for qos in (0, 1):
                runs.append(
                    _run_mqtt(
                        f'pydantic + MQTT (QoS {qos})',
                        f'{pyd_version}/{mqtt_version}',
                        port,
                        pyd_encode,
                        pyd_decode,
                        n,
                        qos,
                        pyd_wire,
                    )
                )
                runs.append(
                    _run_mqtt(
                        f'seared + MQTT (QoS {qos})',
                        f'seared {s.__version__}/{mqtt_version}',
                        port,
                        sea_encode,
                        sea_decode,
                        n,
                        qos,
                        sea_wire,
                    )
                )
    except (OSError, RuntimeError) as exc:
        print(f'suite_stacks: MQTT rows skipped ({exc})')

    with _brokers.zenoh_router() as endpoint, _brokers.router_clients(endpoint) as (pub_sess, sub_sess):
        runs.append(
            _run_zenoh_raw(
                'pydantic + Zenoh (router)', pyd_version, pub_sess, sub_sess, pyd_encode, pyd_decode, n, pyd_wire
            )
        )
        runs.append(
            _run_zenoh_raw(
                'seared + Zenoh (router, raw)',
                f'seared {s.__version__}',
                pub_sess,
                sub_sess,
                sea_encode,
                sea_decode,
                n,
                sea_wire,
            )
        )
        runs.append(
            _run_zeared(
                'zeared + Zenoh (router)',
                versions(),
                _zeared_message('bench/stacks/router', 'json'),
                data,
                pub_sess,
                sub_sess,
                n,
            )
        )

    runs.append(
        _run_zeared(
            'zeared + Zenoh (peer)',
            versions(),
            _zeared_message('bench/stacks/peer', 'json'),
            data,
            session,
            session,
            n,
        )
    )
    runs.append(
        _run_zeared(
            'zeared + Zenoh (peer, msgpack default)',
            versions(),
            _zeared_message('bench/stacks/peer_msgp', 'msgpack'),
            data,
            session,
            session,
            n,
        )
    )
    runs.extend(_accelerated_rows(data, session, n))
    return runs


def _accelerated_rows(data: dict[str, Any], session: Any, n: int) -> list[Run]:
    """The accelerated native rows, or nothing when ``rusted`` isn't engaged.

    Without these the suite answers only half the question: pure-Python seared
    loses the codec axis to pydantic-core by construction, and the accelerator
    is precisely what closes that gap.
    """
    try:
        import rusted  # noqa: F401
    except ImportError:
        return []

    rows = []
    for label, encoding in [('json', 'json'), ('msgpack default', 'msgpack')]:

        @z.zeared
        class Outer(z.Message):
            TOPIC = f'bench/stacks/accel_{encoding}'
            ENCODING = encoding
            name: str = z.Str(required=True)
            items: list[Inner] = z.T(Inner, many=True, required=True)
            tags: list[str] = z.Str(many=True, default_factory=list)

        if not Outer.__seared_accel__.accelerated:
            print(f'suite_stacks: accelerated rows skipped ({Outer.__seared_accel__.reason})')
            return []
        rows.append(
            _run_zeared(
                f'zeared + rusted + Zenoh (peer, {label})',
                versions(accelerated=True),
                Outer,
                data,
                session,
                session,
                n,
            )
        )
    return rows
