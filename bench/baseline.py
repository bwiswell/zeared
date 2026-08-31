"""The raw-Zenoh + marshmallow comparator every suite measures against.

Kept in its own module so the ``marshmallow`` import lives in exactly one
place. It ships behind the repo-only ``[bench]`` extra, so suites import this
lazily and simply omit the baseline row when it isn't installed — the zeared
strategies are the point, the comparator is context.
"""

from __future__ import annotations

import json
from importlib.metadata import version as dist_version
from typing import TYPE_CHECKING, Any

from marshmallow import EXCLUDE, Schema
from marshmallow.fields import Float as MFloat
from marshmallow.fields import Integer, Nested, String
from marshmallow.fields import List as MList

if TYPE_CHECKING:
    from collections.abc import Callable

    import zenoh

STRATEGY = 'zenoh + marshmallow (json)'


class InnerSchema(Schema):
    """Inner record — marshmallow mirror of ``harness.Inner``."""

    class Meta:
        """Drop unknown keys, matching zeared's decode posture."""

        unknown = EXCLUDE

    x = Integer(required=True)
    y = MFloat(required=True)
    label = String(load_default=None)


class OuterSchema(Schema):
    """Outer payload — marshmallow mirror of the suites' message classes."""

    class Meta:
        """Drop unknown keys, matching zeared's decode posture."""

        unknown = EXCLUDE

    name = String(required=True)
    items = MList(Nested(InnerSchema()), required=True)
    tags = MList(String(), load_default=[])


_schema = OuterSchema()


def version() -> str:
    """Runtime version string for the comparator strategy."""
    return f'marshmallow {dist_version("marshmallow")}'


def wire_bytes(data: dict[str, Any]) -> int:
    """Bytes one marshmallow-encoded message occupies on the wire."""
    return len(_schema.dumps(data).encode('utf-8'))


def strategy(
    session: zenoh.Session,
    topic: str,
    data: dict[str, Any],
) -> tuple[Callable[[], None], Callable[[Callable[[], None]], Any], Callable[[Any], None]]:
    """Build the ``(publish, subscribe, undeclare)`` triple for the comparator.

    The subscriber decodes every sample, so the comparison stays
    apples-to-apples with zeared's decode-on-receive path.
    """

    def publish() -> None:
        session.put(topic, _schema.dumps(data).encode('utf-8'), encoding='application/json')

    def subscribe(on_each: Callable[[], None]) -> Any:
        def on_sample(sample: zenoh.Sample) -> None:
            _schema.load(json.loads(bytes(sample.payload).decode('utf-8')))
            on_each()

        return session.declare_subscriber(topic, on_sample)

    def undeclare(sub: Any) -> None:
        sub.undeclare()

    return publish, subscribe, undeclare
