"""zeared over seared + the ``rusted`` compiled accelerator core.

The same wire strategies as ``suite_wire``, built *without* ``accel=False`` so
seared's accelerator seam takes them. Everything else in the bench pins the
pure-Python path precisely so this suite is the only place compiled numbers
appear, under a name that says so.

Skipped with a note when ``rusted`` isn't installed. It is never a zeared
dependency — pending published wheels, the posture matches seared's: you
either have it on your system and it Just Works, or the suite sits out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import rusted

import zeared as z

from .harness import DEFAULT_ITERATIONS, Inner, fixed_n, payload, versions, wire_size

if TYPE_CHECKING:
    import zenoh

    from .harness import Run

SUITE = 'rusted'


def _message_classes() -> list[tuple[str, type[z.Message]]] | None:
    """The accelerated strategies, or ``None`` if the seam declined them.

    Importable is not the same as engaged: ``SEARED_ACCEL=off``, an ABI
    mismatch, or a single unsupported field would each leave these on the
    Python path. Timing that and labelling it "rusted" is exactly the
    mis-attribution this check exists to prevent.
    """

    @z.zeared
    class OuterJson(z.Message):
        TOPIC = 'bench/rusted/json'
        ENCODING = 'json'
        name: str = z.Str(required=True)
        items: list[Inner] = z.T(Inner, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    @z.zeared
    class OuterMsgpack(z.Message):
        TOPIC = 'bench/rusted/msgpack'
        ENCODING = 'msgpack'
        name: str = z.Str(required=True)
        items: list[Inner] = z.T(Inner, many=True, required=True)
        tags: list[str] = z.Str(many=True, default_factory=list)

    for cls in (OuterJson, OuterMsgpack):
        accel = cls.__seared_accel__
        if not accel.accelerated:
            print(f'suite_rusted: skipped ({accel.reason})')
            return None

    return [
        ('zenoh + zeared + rusted (json, cached)', OuterJson),
        ('zenoh + zeared + rusted (msgpack, cached)', OuterMsgpack),
    ]


def run(session: zenoh.Session, n: int = DEFAULT_ITERATIONS) -> list[Run]:
    """Time the accelerated strategies over ``n`` messages, or nothing if declined."""
    from .suite_wire import zeared_strategy

    classes = _message_classes()
    if classes is None:
        return []

    data: dict[str, Any] = payload()
    version = versions(accelerated=True)
    runs = []
    for label, msg_cls in classes:
        pub, sub, und = zeared_strategy(session, msg_cls, data)
        runs.append(fixed_n(label, version, n, pub, sub, und, wire_size(msg_cls, data)))
    return runs


__all__ = ['SUITE', 'run', 'rusted']
