"""Process fixtures for the stack-comparison suite: an MQTT broker and a Zenoh router.

Both are started by the bench and torn down with it, so the suite needs no
external setup. Both fail closed: if the broker binary is missing or refuses
to come up, the caller skips its rows and the rest of the bench still runs.
"""

from __future__ import annotations

import contextlib
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import zeared as z

if TYPE_CHECKING:
    from collections.abc import Iterator

    from zeared._managed_session import SessionLike

#: Seconds allowed for a freshly spawned broker to accept connections.
_STARTUP_TIMEOUT_S = 5.0
_STARTUP_POLL_S = 0.05

#: Seconds to let a router's client sessions discover each other.
_MESH_SETTLE_S = 0.4


def have_mosquitto() -> bool:
    """Whether an MQTT broker binary is on PATH."""
    return shutil.which('mosquitto') is not None


def _free_port() -> int:
    """A loopback port free at this instant.

    Inherently racy — something else could claim it before the broker binds.
    Acceptable for a bench; the caller reports a startup failure as a skip.
    """
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, deadline: float) -> bool:
    """Poll until ``port`` accepts a connection, or ``deadline`` passes."""
    while time.perf_counter() < deadline:
        with contextlib.suppress(OSError), socket.create_connection(('127.0.0.1', port), timeout=0.2):
            return True
        time.sleep(_STARTUP_POLL_S)
    return False


@contextlib.contextmanager
def mosquitto() -> Iterator[int]:
    """Run a private ``mosquitto`` on a free loopback port; yield the port.

    Anonymous, non-persistent, loopback-only — a throwaway broker for the
    duration of the suite. Raises ``RuntimeError`` if it doesn't come up.
    """
    port = _free_port()
    tmp = Path(tempfile.mkdtemp(prefix='zeared-bench-'))
    cfg = tmp / 'mosquitto.conf'
    cfg.write_text(
        f'listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\nmax_inflight_messages 0\n',
        encoding='utf-8',
    )
    proc = subprocess.Popen(  # noqa: S603
        ['mosquitto', '-c', str(cfg)],  # noqa: S607
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not _wait_for_port(port, time.perf_counter() + _STARTUP_TIMEOUT_S):
            msg = f'mosquitto did not accept connections on {port} within {_STARTUP_TIMEOUT_S}s'
            raise RuntimeError(msg)
        yield port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        shutil.rmtree(tmp, ignore_errors=True)


@contextlib.contextmanager
def zenoh_router() -> Iterator[str]:
    """Run an in-process Zenoh router; yield its ``tcp/…`` endpoint.

    The broker-equivalent topology: client sessions connected here exchange
    messages through the router rather than peer-to-peer, so the MQTT rows
    and the Zenoh rows pay a comparable hop.
    """
    port = _free_port()
    endpoint = f'tcp/127.0.0.1:{port}'
    hub = z.hub(listen=[endpoint])
    try:
        if not _wait_for_port(port, time.perf_counter() + _STARTUP_TIMEOUT_S):
            msg = f'zenoh router did not listen on {endpoint}'
            raise RuntimeError(msg)
        yield endpoint
    finally:
        hub.close()


@contextlib.contextmanager
def router_clients(endpoint: str) -> Iterator[tuple[SessionLike, SessionLike]]:
    """Yield ``(publisher, subscriber)`` client sessions attached to ``endpoint``."""
    pub = z.client(router=endpoint)
    sub = z.client(router=endpoint)
    try:
        time.sleep(_MESH_SETTLE_S)
        yield pub, sub
    finally:
        for sess in (pub, sub):
            with contextlib.suppress(Exception):
                z.release(session=sess)
