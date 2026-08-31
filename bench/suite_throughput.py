"""Throughput suite: sustained sync publish rate over a fixed time window.

The regression tracker. Where ``suite_wire`` answers "what does one message
cost", this answers "what rate does the sync path hold when pushed flat out",
which is the number that moves when publisher caching or the encode path
regresses.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import zeared as z

from .harness import DEFAULT_DURATION_S, InnerPy, payload, publish_window, versions, wire_size

if TYPE_CHECKING:
    import zenoh

    from .harness import Run

SUITE = 'throughput'


def _message_classes() -> list[tuple[str, type[z.Message]]]:
    """The zeared strategies under test, on topics unique to this suite."""

    @z.zeared(accel=False)
    class OuterJson(z.Message):
        TOPIC = 'bench/throughput/json'
        ENCODING = 'json'
        name: str = z.Str(required=True)
        items: list[InnerPy] = z.T(InnerPy, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    @z.zeared(accel=False)
    class OuterMsgpack(z.Message):
        TOPIC = 'bench/throughput/msgpack'
        ENCODING = 'msgpack'
        name: str = z.Str(required=True)
        items: list[InnerPy] = z.T(InnerPy, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    @z.zeared(accel=False)
    class OuterMsgpackNoCache(z.Message):
        TOPIC = 'bench/throughput/msgpack_nocache'
        ENCODING = 'msgpack'
        PUBLISHER = False
        name: str = z.Str(required=True)
        items: list[InnerPy] = z.T(InnerPy, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    return [
        ('sync (json, cached)', OuterJson),
        ('sync (msgpack, cached)', OuterMsgpack),
        ('sync (msgpack, PUBLISHER=False)', OuterMsgpackNoCache),
    ]


def run(session: zenoh.Session, duration_s: float = DEFAULT_DURATION_S) -> list[Run]:
    """Hold each strategy flat out for ``duration_s``, then drain and report."""
    from .suite_wire import zeared_strategy

    data = payload()
    runs = []

    try:
        from . import baseline
    except ImportError:
        baseline = None
    if baseline is not None:
        pub, sub, und = baseline.strategy(session, 'bench/throughput/marshmallow', data)
        runs.append(
            publish_window(baseline.STRATEGY, baseline.version(), duration_s, pub, sub, und, baseline.wire_bytes(data))
        )

    for label, msg_cls in _message_classes():
        pub, sub, und = zeared_strategy(session, msg_cls, data)
        runs.append(publish_window(label, versions(), duration_s, pub, sub, und, wire_size(msg_cls, data)))
    return runs
