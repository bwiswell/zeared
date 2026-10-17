"""Tests for ``zeared/_reconnect/_link.py`` — router-link detection and the
re-attach walk (0.4.0). Fakes stand in for the session and the subscribers, so
the transitions, the pairing, the skip and the cap are pinned without Zenoh;
``tests/test_hub_restart.py`` runs the real thing."""

from __future__ import annotations

import threading
import time

from zeared._managed_session._on_reconnect_mixin import _OnReconnectMixin
from zeared._reconnect._link import _check_link, _note_routers, _run_link_walk
from zeared.subscriber import _subscribers, _subscribers_lock


class _FakeInfo:
    def __init__(self) -> None:
        self.zids: list[str] = []
        self.broken = False

    def routers_zid(self) -> list[str]:
        if self.broken:
            msg = 'session closed'
            raise RuntimeError(msg)
        return list(self.zids)


class _FakeRaw:
    def __init__(self) -> None:
        self.info = _FakeInfo()


class _FakeManaged(_OnReconnectMixin):
    """Just the slots ``_link.py`` and the callback driver read."""

    def __init__(self, *, jitter: float = 0.0, concurrency: int = 4) -> None:
        self._lock = threading.RLock()
        self._on_reconnect_callbacks: list = []
        self._on_link_lost_callbacks: list = []
        self._on_link_restored_callbacks: list = []
        self._raw = _FakeRaw()
        self._link_state = 'up'
        self._routers: frozenset[str] = frozenset()
        self._resync_jitter = jitter
        self._resync_concurrency = concurrency
        self._probe_cancel = threading.Event()
        self.signals: list[str] = []
        self.on_link_lost(lambda _s: self.signals.append('lost'))
        self.on_link_restored(lambda _s: self.signals.append('restored'))

    def raw(self) -> _FakeRaw:
        return self._raw

    def routers(self, *zids: str) -> None:
        self._raw.info.zids = list(zids)


class _FakeSub:
    """A subscriber the walk would replay; records its calls and the concurrency it saw."""

    active = 0
    peak = 0
    lock = threading.Lock()

    def __init__(self, *, replays: bool = True, delay: float = 0.0, fails: bool = False) -> None:
        self.replays = replays
        self.delay = delay
        self.fails = fails
        self.calls = 0
        self._msg_cls = None

    def _wants_replay(self) -> bool:
        return self.replays

    def _replay_retained(self, raw: object) -> None:
        with _FakeSub.lock:
            _FakeSub.active += 1
            _FakeSub.peak = max(_FakeSub.peak, _FakeSub.active)
        try:
            self.calls += 1
            time.sleep(self.delay)
            if self.fails:
                msg = 'replay failed'
                raise RuntimeError(msg)
        finally:
            with _FakeSub.lock:
                _FakeSub.active -= 1


def _register(managed: _FakeManaged, subs: list[_FakeSub]) -> None:
    with _subscribers_lock:
        _subscribers[id(managed)] = set(subs)


def _unregister(managed: _FakeManaged) -> None:
    with _subscribers_lock:
        _subscribers.pop(id(managed), None)


class TestDetection:
    def test_a_session_that_never_had_a_router_never_transitions(self):
        m = _FakeManaged()
        _note_routers(m)
        _check_link(m)
        m.routers('r1')
        _check_link(m)  # first attach: a baseline, not a restore
        assert m.signals == []

    def test_loss_then_return(self):
        m = _FakeManaged()
        m.routers('r1')
        _note_routers(m)
        m.routers()
        _check_link(m)
        _check_link(m)  # still lost: no second signal
        assert m.signals == ['lost']
        m.routers('r1')
        _check_link(m)
        assert m.signals == ['lost', 'restored']
        assert m._link_state == 'up'

    def test_a_router_replaced_between_two_ticks_is_both(self):
        """A restart quicker than the probe interval: the zid changed, so it was lost and is back."""
        m = _FakeManaged()
        m.routers('r1')
        _note_routers(m)
        m.routers('r2')
        _check_link(m)
        assert m.signals == ['lost', 'restored']

    def test_a_partial_change_is_neither(self):
        m = _FakeManaged()
        m.routers('r1', 'r2')
        _note_routers(m)
        m.routers('r2', 'r3')
        _check_link(m)
        assert m.signals == []

    def test_unreadable_routers_change_nothing(self):
        m = _FakeManaged()
        m.routers('r1')
        _note_routers(m)
        m._raw.info.broken = True
        _check_link(m)
        assert m.signals == []
        assert m._routers == frozenset({'r1'})


class TestWalk:
    def setup_method(self):
        _FakeSub.active = 0
        _FakeSub.peak = 0

    def test_nothing_to_replay_skips_the_jitter(self):
        m = _FakeManaged(jitter=5.0)
        _register(m, [_FakeSub(replays=False)])
        try:
            start = time.monotonic()
            _run_link_walk(m)
            assert time.monotonic() - start < 0.5
        finally:
            _unregister(m)

    def test_every_replaying_subscriber_runs_once_within_the_cap(self):
        m = _FakeManaged(concurrency=2)
        subs = [_FakeSub(delay=0.1) for _ in range(6)]
        skipped = _FakeSub(replays=False)
        _register(m, [*subs, skipped])
        try:
            _run_link_walk(m)
        finally:
            _unregister(m)
        assert [s.calls for s in subs] == [1] * 6
        assert skipped.calls == 0
        assert _FakeSub.peak == 2

    def test_a_failed_replay_doesnt_stop_the_others(self):
        m = _FakeManaged(concurrency=1)
        ok = _FakeSub()
        _register(m, [_FakeSub(fails=True), ok])
        try:
            _run_link_walk(m)
        finally:
            _unregister(m)
        assert ok.calls == 1

    def test_a_teardown_during_the_jitter_abandons_the_walk(self):
        m = _FakeManaged(jitter=5.0)
        sub = _FakeSub()
        _register(m, [sub])
        m._probe_cancel.set()
        try:
            _run_link_walk(m)
        finally:
            _unregister(m)
        assert sub.calls == 0

    def test_restored_fires_after_the_walk(self):
        m = _FakeManaged()
        order: list[str] = []

        class Ordered(_FakeSub):
            def _replay_retained(self, raw: object) -> None:
                order.append('walk')

        m.on_link_restored(lambda _s: order.append('restored'))
        _register(m, [Ordered()])
        try:
            m.routers('r1')
            _note_routers(m)
            m.routers()
            _check_link(m)
            m.routers('r1')
            _check_link(m)
        finally:
            _unregister(m)
        assert order == ['walk', 'restored']


class TestRepublishOnRestore:
    """0.4.0: restoring the link republishes this session's retained values first."""

    def test_order_republish_walk_restored(self):
        from zeared.retention import _registry as retention_registry

        m = _FakeManaged()
        order: list[str] = []

        class FakeCache:
            def __init__(self, session: object) -> None:
                self._session = session
                self._cls = None

            def republish(self) -> int:
                order.append('republish')
                return 1

        class Ordered(_FakeSub):
            def _replay_retained(self, raw: object) -> None:
                order.append('walk')

        mine, other = (object, id(m)), (object, -1)
        retention_registry[mine] = FakeCache(m)
        retention_registry[other] = FakeCache(object())  # another session's cache: untouched
        m.on_link_restored(lambda _s: order.append('restored'))
        _register(m, [Ordered()])
        try:
            m.routers('r1')
            _note_routers(m)
            m.routers()
            _check_link(m)
            m.routers('r1')
            _check_link(m)
        finally:
            _unregister(m)
            retention_registry.pop(mine, None)
            retention_registry.pop(other, None)
        assert order == ['republish', 'walk', 'restored']
