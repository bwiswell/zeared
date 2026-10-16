"""Shared fixtures for zeared integration tests."""

from __future__ import annotations

import contextlib
import socket
import time
from typing import TYPE_CHECKING, cast

import pytest
import zenoh

import zeared as z

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from zeared._managed_session import ManagedSession

# Suppress Zenoh's default stderr noise during tests.
zenoh.init_log_from_env_or('error')


def _peer_session() -> zenoh.Session:
    c = zenoh.Config()
    c.insert_json5('mode', '"peer"')
    c.insert_json5('scouting/multicast/enabled', 'false')
    return zenoh.open(c)


@pytest.fixture
def session():
    """Isolated Zenoh peer session (no multicast)."""
    s = _peer_session()
    try:
        yield s
    finally:
        s.close()


@pytest.fixture
def session_pair():
    """Two independent Zenoh sessions that cannot see each other's traffic."""
    a = _peer_session()
    b = _peer_session()
    try:
        yield a, b
    finally:
        a.close()
        b.close()


@pytest.fixture
def connected_pair(unused_tcp_port_factory=None):
    """Two peer sessions wired via TCP so they discover each other."""
    import random

    port = random.randint(20000, 40000)
    endpoint = f'tcp/127.0.0.1:{port}'

    ca = zenoh.Config()
    ca.insert_json5('mode', '"peer"')
    ca.insert_json5('scouting/multicast/enabled', 'false')
    ca.insert_json5('listen/endpoints', f'["{endpoint}"]')
    a = zenoh.open(ca)

    cb = zenoh.Config()
    cb.insert_json5('mode', '"peer"')
    cb.insert_json5('scouting/multicast/enabled', 'false')
    cb.insert_json5('connect/endpoints', f'["{endpoint}"]')
    b = zenoh.open(cb)

    # Give the link a moment to come up.
    time.sleep(0.2)
    try:
        yield a, b
    finally:
        # Guard double-close — tests may explicitly close a session mid-run
        # (e.g. to fire an LWT).
        for s in (a, b):
            with contextlib.suppress(Exception):
                s.close()


@pytest.fixture(autouse=True)
def _reset_zeared_state():
    """Reset module-level session, debug flag, and caches between tests."""
    z.session._set_default(None)
    z.debug = False
    z.clear_publisher_cache()
    z.clear_retention_cache()
    z.clear_queryable_cache()
    z.clear_observer()
    z.clear_presence_state()
    yield
    z.session._set_default(None)
    z.debug = False
    z.clear_publisher_cache()
    z.clear_retention_cache()
    z.clear_queryable_cache()
    z.clear_observer()
    z.clear_presence_state()


def wait(seconds: float = 0.1) -> None:
    """Short sleep for async message delivery; centralised so tuning is cheap."""
    time.sleep(seconds)


# --- Endpoints and a restartable hub (0.3.10) --------------------------------


def _free_port() -> int:
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _endpoint() -> str:
    return f'tcp/127.0.0.1:{_free_port()}'


def routers(sess: zenoh.Session | ManagedSession) -> int:
    """How many routers ``sess`` is attached to: ``info.routers_zid()``, counted."""
    return len(list(sess.info.routers_zid()))


def wait_until(pred: Callable[[], bool], timeout: float, step: float = 0.05) -> float:
    """Poll ``pred`` until it's true and return the seconds it took; raise ``TimeoutError`` past ``timeout``."""
    start = time.monotonic()
    while True:
        if pred():
            return time.monotonic() - start
        if time.monotonic() - start > timeout:
            msg = f'condition not met within {timeout}s'
            raise TimeoutError(msg)
        time.sleep(step)


class RestartableHub:
    """An in-process hub on one fixed endpoint, closed and reopened to stand in for a restart.

    A client dialling the endpoint re-attaches to the reopened hub by itself, which is a new
    session with a new zid, just as a restarted hub process would be. ``on_boot`` runs straight
    after each open, on the hub's own session: in the hub topology the publisher is often the
    hub, so a boot publish there is what a restarted publisher does.
    """

    def __init__(self, endpoint: str | None = None) -> None:
        self.endpoint = endpoint or _endpoint()
        self.session: zenoh.Session | None = None

    def start(self, on_boot: Callable[[zenoh.Session], object] | None = None) -> zenoh.Session:
        if self.session is not None:
            msg = 'hub already running'
            raise RuntimeError(msg)
        self.session = z.hub(listen=[self.endpoint])
        if on_boot is not None:
            on_boot(self.session)
        return self.session

    def stop(self) -> None:
        sess, self.session = self.session, None
        if sess is not None:
            with contextlib.suppress(Exception):
                sess.close()

    def restart(self, on_boot: Callable[[zenoh.Session], object] | None = None) -> zenoh.Session:
        self.stop()
        return self.start(on_boot)


@pytest.fixture
def restartable_hub() -> Iterator[RestartableHub]:
    """A :class:`RestartableHub`, not yet started; closed on teardown."""
    hub = RestartableHub()
    try:
        yield hub
    finally:
        hub.stop()


def slow_redial_client(endpoint: str, *, redial_ms: int = 2000, probe_interval: float = 1.0) -> ManagedSession:
    """A managed client of ``endpoint`` whose router re-dial waits a fixed ``redial_ms``.

    At Zenoh's default re-dial a client can re-attach within a hub's own reopen (about 0.5 s),
    so whether a restarted hub's boot lands while the client is detached is down to timing.
    A fixed period makes it deterministic: the link drops at the hub's close, the reopen and
    its boot take about 0.5 s, and the client doesn't dial again until ``redial_ms`` after the
    drop. ``connect.retry`` is ``null`` in Zenoh's default config, so it is set as one object.
    """
    retry = {'period_init_ms': redial_ms, 'period_max_ms': redial_ms, 'period_increase_factor': 1}
    # ``client`` returns a ``ManagedSession`` whenever ``auto_reconnect=True``.
    return cast(
        'ManagedSession',
        z.client(
            config=z.SessionConfig(
                mode=z.Mode.CLIENT,
                router=endpoint,
                zenoh_overrides={'connect/retry': retry, 'scouting/multicast/enabled': False},
            ),
            auto_reconnect=True,
            probe_interval=probe_interval,
        ),
    )
