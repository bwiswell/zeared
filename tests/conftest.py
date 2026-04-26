"""Shared fixtures for zeared integration tests."""
from __future__ import annotations

import time

import pytest
import zenoh

import zeared as z


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
            try:
                s.close()
            except Exception:
                pass


@pytest.fixture(autouse=True)
def _reset_zeared_state():
    """Reset module-level session, debug flag, and caches between tests."""
    z.session._set_default(None)
    z.debug = False
    z.clear_publisher_cache()
    z.clear_retention_cache()
    z.clear_observer()
    z.clear_presence_state()
    yield
    z.session._set_default(None)
    z.debug = False
    z.clear_publisher_cache()
    z.clear_retention_cache()
    z.clear_observer()
    z.clear_presence_state()


def wait(seconds: float = 0.1) -> None:
    """Short sleep for async message delivery; centralised so tuning is cheap."""
    time.sleep(seconds)
