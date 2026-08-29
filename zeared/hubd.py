"""``python -m zeared.hubd`` — a stateless Zenoh relay hub daemon.

Named ``hubd`` (the daemon), paired with the ``zeared.hub()`` factory it opens
— a submodule and a same-named function can't coexist on the ``zeared``
namespace, and the ``-d`` daemon suffix mirrors ``zenohd``.

Opens a router-mode session (see :func:`zeared.hub`) that relays pub/sub,
queries, and liveliness between nodes which can't reach each other directly
— e.g. two peers behind NAT, both outbound-only, each connecting *out* to
this hub. The hub holds no zeared message state; it routes opaque samples,
so it needs no ``rio-protocol`` / schemas. Deploy it as a systemd service on
a publicly-reachable host; point nodes at it with
``z.client(router='tcp/host:7447')``.

The relay runs in Zenoh's Rust core — the Python main thread only holds the
session open and waits for a shutdown signal.
"""
from __future__ import annotations

import argparse
import logging
import signal
import threading
from typing import Callable, Optional

import zenoh

from ._factories import hub as _open_hub


_log = logging.getLogger('zeared.hub')

_DEFAULT_LISTEN = 'tcp/0.0.0.0:7447'


def _install_signal_stop(stop: threading.Event) -> None:
    """Wire SIGINT/SIGTERM to ``stop.set``. No-op off the main thread
    (signal handlers can only be installed there — e.g. under pytest)."""
    def _handler(_signum, _frame):
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):  # not main thread / unsupported
            pass


def run(
    *,
    listen: Optional[list] = None,
    connect: Optional[list] = None,
    timestamping: bool = True,
    zenoh_config: Optional[zenoh.Config] = None,
    stop: Optional[threading.Event] = None,
    on_ready: Optional[Callable[[zenoh.Session], None]] = None,
) -> None:
    """Open the hub, then block until ``stop`` is set (or SIGINT/SIGTERM).

    Pass ``stop`` to drive shutdown yourself (tests, embedding); when omitted,
    a fresh event is created and SIGINT/SIGTERM are wired to it. ``on_ready``
    fires with the live session once it's up — handy for tests that need the
    endpoints/zid before connecting.
    """
    sess = _open_hub(
        listen=listen, connect=connect,
        timestamping=timestamping, zenoh_config=zenoh_config,
    )
    _log.info(
        'hub up: zid=%s listen=%s connect=%s',
        sess.zid(), listen or [_DEFAULT_LISTEN], connect or [],
    )
    if on_ready is not None:
        on_ready(sess)

    own_stop = stop is None
    if own_stop:
        stop = threading.Event()
        _install_signal_stop(stop)
    try:
        stop.wait()
    finally:
        _log.info('hub shutting down')
        try:
            sess.close()
        except Exception:  # noqa: BLE001
            _log.warning('hub session close failed', exc_info=True)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog='python -m zeared.hubd',
        description='Run a Zenoh relay hub (router) for NAT-gated nodes.',
    )
    parser.add_argument(
        '-l', '--listen', action='append', metavar='ENDPOINT',
        help=f'endpoint to bind (repeatable; default {_DEFAULT_LISTEN})',
    )
    parser.add_argument(
        '-c', '--connect', action='append', metavar='ENDPOINT',
        help='endpoint of another hub to link to (repeatable)',
    )
    parser.add_argument(
        '--config', metavar='FILE',
        help='JSON5 Zenoh config file (for TLS / access-control); '
             'listen/connect flags still layer on top',
    )
    parser.add_argument(
        '--no-timestamping', action='store_true',
        help='do not force HLC timestamping on (on by default)',
    )
    parser.add_argument(
        '--log-level', default='INFO',
        help='logging level (default INFO)',
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    zenoh_config = (
        zenoh.Config.from_file(args.config) if args.config else None
    )

    run(
        listen=args.listen or [_DEFAULT_LISTEN],
        connect=args.connect,
        timestamping=not args.no_timestamping,
        zenoh_config=zenoh_config,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
