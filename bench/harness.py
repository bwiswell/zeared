"""Shared bench harness: workload, Zenoh session, timing loops, result schema.

``Measurement`` / ``Report`` are ``@z.zeared`` classes on purpose — the bench
dogfoods the library it measures to produce its own artifact, exactly as
seared's bench does.

Where this deliberately differs from seared's harness: seared times one pair
of operations (``load`` / ``dump``) over a fixed iteration count, so its
``Case`` carries those two callables. zeared measures a wire path — bytes on
the wire, end-to-end delivery rate, sustained publish rate under a duration
window, and the cost of each sync/async delivery combination. Those don't
reduce to a load/dump pair, so measurements here are long-form: one row per
``(suite, strategy, metric)``.
"""

from __future__ import annotations

import platform
import threading
import time
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as dist_version
from typing import TYPE_CHECKING, Any

import zenoh

import zeared as z

if TYPE_CHECKING:
    from collections.abc import Callable

#: Iterations for the fixed-N wire suite. Matches the historical baseline so
#: numbers stay comparable across releases.
DEFAULT_ITERATIONS = 5_000

#: Publish-window length, in seconds, for the duration-based suites.
DEFAULT_DURATION_S = 5.0

#: Payload shape — one outer object with a list of records plus string tags.
#: Same shape as seared's bench, layered onto zeared's wire path.
N_ITEMS = 20
N_TAGS = 3

#: How long to let a freshly declared subscriber settle before timing.
_SETTLE_S = 0.15

#: Give up waiting for the subscriber to catch up after this long.
_DRAIN_TIMEOUT_S = 15.0

#: Consecutive idle polls that count as "the subscriber has caught up".
_DRAIN_STABLE_TICKS = 3

_DRAIN_POLL_S = 0.05


def payload() -> dict[str, Any]:
    """The representative nested payload every suite publishes."""
    return {
        'name': 'demo',
        'items': [{'x': i, 'y': i * 1.5, 'label': f'i{i}'} for i in range(N_ITEMS)],
        'tags': ['alpha', 'beta', 'gamma'],
    }


@z.zeared
class Inner(z.Zeared):
    """Inner record of the bench payload."""

    x: int = z.Int(required=True)
    y: float = z.Float(required=True)
    label: str | None = z.Str(default=None)


@z.zeared(accel=False)
class InnerPy(z.Zeared):
    """``Inner`` pinned to the pure-Python path.

    Used by every suite except ``suite_rusted``: an accelerator wheel that
    happens to be installed would otherwise silently retarget the baseline
    strategies and publish compiled numbers under zeared's own name.
    """

    x: int = z.Int(required=True)
    y: float = z.Float(required=True)
    label: str | None = z.Str(default=None)


def peer_session() -> zenoh.Session:
    """A scouting-free in-process peer session — the bench's transport."""
    c = zenoh.Config()
    c.insert_json5('mode', '"peer"')
    c.insert_json5('scouting/multicast/enabled', 'false')
    return zenoh.open(c)


def _dist(name: str) -> str:
    try:
        return dist_version(name)
    except PackageNotFoundError:
        return 'absent'


def versions(*, accelerated: bool = False) -> str:
    """Runtime library versions for a strategy, read from the installed dists."""
    # Read from installed metadata rather than the ``__version__``
    # constants: a stale constant would silently mislabel the artifact.
    parts = [f'zeared {_dist("zeared")}', f'seared {_dist("seared")}']
    if accelerated:
        parts.append(f'rusted {_dist("rusted")}')
    return '/'.join(parts)


def environment() -> tuple[str, str]:
    """(python version, platform string) for the report metadata."""
    return platform.python_version(), platform.platform()


@z.zeared
class Measurement(z.Zeared):
    """One (suite, strategy, metric) datum."""

    suite: str = z.Str(required=True)
    strategy: str = z.Str(required=True)
    version: str = z.Str(required=True)
    metric: str = z.Str(required=True)
    value: float = z.Float(required=True)
    unit: str = z.Str(required=True)


@z.zeared
class Report(z.Zeared):
    """The committed ``bench/results.json`` artifact."""

    timestamp: str = z.Str(required=True, doc='UTC ISO 8601')
    python: str = z.Str(required=True)
    platform: str = z.Str(required=True)
    zenoh: str = z.Str(required=True)
    iterations: int = z.Int(required=True, doc='fixed-N suites')
    duration_s: float = z.Float(required=True, doc='publish window, duration suites')
    measurements: list[Measurement] = z.T(Measurement, many=True, required=True)


@dataclass(frozen=True, slots=True)
class Run:
    """Raw counters from one timed publish window."""

    strategy: str
    version: str
    sent: int
    received: int
    publish_secs: float
    total_secs: float
    wire_bytes: int

    @property
    def pub_rate(self) -> float:
        """Publish-side messages per second."""
        return self.sent / self.publish_secs if self.publish_secs > 0 else 0.0

    @property
    def e2e_rate(self) -> float:
        """End-to-end (subscriber-received) messages per second."""
        return self.received / self.total_secs if self.total_secs > 0 else 0.0

    @property
    def mb_per_s(self) -> float:
        """Publish-side throughput in megabytes per second."""
        return self.pub_rate * self.wire_bytes / 1_000_000

    @property
    def drops(self) -> int:
        """Messages published but never delivered."""
        return self.sent - self.received

    def to_measurements(self, suite: str) -> list[Measurement]:
        """Expand into the long-form rows written to ``results.json``."""
        rows = [
            ('pub_rate', self.pub_rate, 'msgs/s'),
            ('e2e_rate', self.e2e_rate, 'msgs/s'),
            ('mb_per_s', self.mb_per_s, 'MB/s'),
            ('wire_bytes', float(self.wire_bytes), 'bytes'),
            ('drops', float(self.drops), 'msgs'),
        ]
        return [
            Measurement(
                suite=suite,
                strategy=self.strategy,
                version=self.version,
                metric=metric,
                value=value,
                unit=unit,
            )
            for metric, value, unit in rows
        ]


def settle() -> None:
    """Let a freshly declared subscriber come up before timing starts."""
    time.sleep(_SETTLE_S)


def drain(received: list[int], sent: int, t_pub_end: float) -> float:
    """Block until the subscriber catches up; return the wall clock at that point.

    Polls until the received count both meets ``sent`` and stops moving, so a
    slow consumer isn't credited with the publisher's rate. Bails out after
    ``_DRAIN_TIMEOUT_S`` so a genuinely dropped message can't hang the bench.
    """
    last = -1
    stable = 0
    while received[0] < sent or stable < _DRAIN_STABLE_TICKS:
        if received[0] == last:
            stable += 1
        else:
            stable = 0
        last = received[0]
        time.sleep(_DRAIN_POLL_S)
        if time.perf_counter() - t_pub_end > _DRAIN_TIMEOUT_S:
            break
    return time.perf_counter()


def publish_window(  # noqa: PLR0913, PLR0917
    strategy: str,
    version: str,
    duration_s: float,
    publish: Callable[[], None],
    subscribe: Callable[[Callable[[], None]], Any],
    undeclare: Callable[[Any], None],
    wire_bytes: int,
) -> Run:
    """Publish as fast as possible for ``duration_s``, then drain and report.

    The shared core of the duration-based suites: every strategy differs only
    in how it publishes, subscribes, and tears down.
    """
    received = [0]

    def on_each() -> None:
        received[0] += 1

    sub = subscribe(on_each)
    settle()

    t_start = time.perf_counter()
    deadline = t_start + duration_s
    sent = 0
    while time.perf_counter() < deadline:
        publish()
        sent += 1
    t_pub_end = time.perf_counter()

    t_end = drain(received, sent, t_pub_end)
    undeclare(sub)
    return Run(
        strategy=strategy,
        version=version,
        sent=sent,
        received=received[0],
        publish_secs=t_pub_end - t_start,
        total_secs=t_end - t_start,
        wire_bytes=wire_bytes,
    )


def fixed_n(  # noqa: PLR0913, PLR0917
    strategy: str,
    version: str,
    n: int,
    publish: Callable[[], None],
    subscribe: Callable[[Callable[[], None]], Any],
    undeclare: Callable[[Any], None],
    wire_bytes: int,
) -> Run:
    """Publish exactly ``n`` messages and wait for all of them.

    The fixed-iteration counterpart to :func:`publish_window`, used by the
    wire suite so message counts (and therefore wire totals) are exact.
    """
    received = [0]
    done = threading.Event()

    def on_each() -> None:
        received[0] += 1
        if received[0] >= n:
            done.set()

    sub = subscribe(on_each)
    settle()

    t_start = time.perf_counter()
    for _ in range(n):
        publish()
    t_pub_end = time.perf_counter()
    done.wait(timeout=_DRAIN_TIMEOUT_S)
    t_end = time.perf_counter()

    undeclare(sub)
    return Run(
        strategy=strategy,
        version=version,
        sent=n,
        received=received[0],
        publish_secs=t_pub_end - t_start,
        total_secs=t_end - t_start,
        wire_bytes=wire_bytes,
    )


def wire_size(msg_cls: type[z.Message], data: dict[str, Any]) -> int:
    """Bytes one message occupies on the wire, serialized exactly as ``send`` does."""
    from zeared import _codec as codec

    instance = msg_cls.load(data)
    encoding = codec.effective_encoding(msg_cls.ENCODING, z.debug)
    return len(codec.pack(msg_cls.dump(instance, format=encoding), encoding))
