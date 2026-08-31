"""Wire suite: bytes on the wire and fixed-N round-trip cost.

The quick smoke check after a wire-path change. Publishes an exact message
count so wire totals and per-message latency are exact, and reports the
encoding's size on the wire alongside them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import zeared as z

from .harness import DEFAULT_ITERATIONS, InnerPy, fixed_n, payload, versions, wire_size

if TYPE_CHECKING:
    import zenoh

    from .harness import Run

SUITE = 'wire'


def _message_classes() -> list[tuple[str, type[z.Message]]]:
    """The zeared strategies under test, built fresh so publisher caches don't persist."""

    @z.zeared(accel=False)
    class OuterJson(z.Message):
        TOPIC = 'bench/wire/json'
        ENCODING = 'json'
        name: str = z.Str(required=True)
        items: list[InnerPy] = z.T(InnerPy, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    @z.zeared(accel=False)
    class OuterMsgpack(z.Message):
        TOPIC = 'bench/wire/msgpack'
        ENCODING = 'msgpack'
        name: str = z.Str(required=True)
        items: list[InnerPy] = z.T(InnerPy, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    @z.zeared(accel=False)
    class OuterMsgpackNoCache(z.Message):
        TOPIC = 'bench/wire/msgpack_nocache'
        ENCODING = 'msgpack'
        PUBLISHER = False  # every send goes straight through session.put()
        name: str = z.Str(required=True)
        items: list[InnerPy] = z.T(InnerPy, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    return [
        ('zenoh + zeared (json, cached)', OuterJson),
        ('zenoh + zeared (msgpack, cached)', OuterMsgpack),
        ('zenoh + zeared (msgpack, PUBLISHER=False)', OuterMsgpackNoCache),
    ]


def zeared_strategy(
    session: zenoh.Session,
    msg_cls: type[z.Message],
    data: dict[str, Any],
) -> tuple[Any, Any, Any]:
    """Build the ``(publish, subscribe, undeclare)`` triple for one message class."""
    # Module-attribute interception (`_ZearedModule.__setattr__`) — the
    # documented way to set the default session; ty can't model it.
    z.session = session  # ty: ignore[invalid-assignment]
    instance = msg_cls.load(data)

    def publish() -> None:
        instance.send()

    def subscribe(on_each: Any) -> Any:
        return msg_cls.on_message(lambda _m: on_each())

    def undeclare(sub: Any) -> None:
        sub.close()

    return publish, subscribe, undeclare


def run(session: zenoh.Session, n: int = DEFAULT_ITERATIONS) -> list[Run]:
    """Time every strategy over ``n`` messages."""
    data = payload()
    runs = []

    # Imported here, not at module scope: the comparator lives behind the
    # repo-only `[bench]` extra, and its absence should cost one row, not the
    # whole suite.
    try:
        from . import baseline
    except ImportError:
        baseline = None
    if baseline is not None:
        pub, sub, und = baseline.strategy(session, 'bench/wire/marshmallow', data)
        runs.append(fixed_n(baseline.STRATEGY, baseline.version(), n, pub, sub, und, baseline.wire_bytes(data)))

    for label, msg_cls in _message_classes():
        pub, sub, und = zeared_strategy(session, msg_cls, data)
        runs.append(fixed_n(label, versions(), n, pub, sub, und, wire_size(msg_cls, data)))
    return runs
