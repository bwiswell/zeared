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
import contextlib
import logging
import signal
import sys
import threading
from typing import TYPE_CHECKING

import zenoh

from ._factories import _DEFAULT_HUB_LISTEN, _apply_defaults, _config_endpoints, _resolve_hub_listen
from ._factories import hub as _open_hub

if TYPE_CHECKING:
    import types
    from collections.abc import Callable

_log = logging.getLogger('zeared.hub')


def _install_signal_stop(stop: threading.Event) -> None:
    """Wire SIGINT/SIGTERM to ``stop.set``.

    No-op off the main thread (signal handlers can only be installed there — e.g. under
    pytest).
    """

    def _handler(_signum: int, _frame: types.FrameType | None) -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(ValueError, OSError):
            signal.signal(sig, _handler)


def run(  # noqa: PLR0913
    *,
    listen: list | None = None,
    connect: list | None = None,
    timestamping: bool = True,
    zenoh_config: zenoh.Config | None = None,
    stop: threading.Event | None = None,
    on_ready: Callable[[zenoh.Session], None] | None = None,
) -> None:
    """Open the hub, then block until ``stop`` is set (or SIGINT/SIGTERM).

    Pass ``stop`` to drive shutdown yourself (tests, embedding); when omitted,
    a fresh event is created and SIGINT/SIGTERM are wired to it. ``on_ready``
    fires with the live session once it's up — handy for tests that need the
    endpoints/zid before connecting.
    """
    # What the hub will bind, worked out before opening (opening writes any
    # explicit endpoints into ``zenoh_config``), so the log names the real ones.
    _, bound = _resolve_hub_listen(listen, None, zenoh_config)
    linked = connect or _config_endpoints(zenoh_config, 'connect/endpoints')
    sess = _open_hub(
        listen=listen,
        connect=connect,
        timestamping=timestamping,
        zenoh_config=zenoh_config,
    )
    _log.info('hub up: zid=%s listen=%s connect=%s', sess.zid(), bound, linked)
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


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for ``python -m zeared.hubd``."""
    parser = argparse.ArgumentParser(
        prog='python -m zeared.hubd',
        description='Run a Zenoh relay hub (router) for NAT-gated nodes.',
    )
    parser.add_argument(
        '-l',
        '--listen',
        action='append',
        metavar='ENDPOINT',
        help=(
            "endpoint to bind (repeatable; default: the --config file's listen endpoints, "
            f'else {_DEFAULT_HUB_LISTEN[0]})'
        ),
    )
    parser.add_argument(
        '-c',
        '--connect',
        action='append',
        metavar='ENDPOINT',
        help='endpoint of another hub to link to (repeatable)',
    )
    parser.add_argument(
        '--config',
        metavar='FILE',
        help=(
            'JSON5 Zenoh config file (for TLS / access-control). --listen / --connect override its '
            'endpoints when given; router mode, timestamping (unless --no-timestamping) and shared '
            'memory off are applied over it'
        ),
    )
    parser.add_argument(
        '--no-timestamping',
        action='store_true',
        help='do not force HLC timestamping on (on by default)',
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        help='logging level (default INFO)',
    )
    args = parser.parse_args(argv)

    # stdout, which systemd hands to journald: the house convention for daemons.
    # ``basicConfig``'s own default would be stderr.
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        stream=sys.stdout,
    )

    zenoh_config = None
    if args.config:
        zenoh_config = zenoh.Config.from_file(args.config)
        # The file brings TLS and access control; zeared's settings land over it, as over a
        # ``SessionConfig.zenoh_config_file``: router mode (this daemon *is* a router; a file
        # without ``mode`` would otherwise open a peer), timestamping unless --no-timestamping,
        # and shared memory off (a hub is where Zenoh's shared-memory leak bites first).
        _apply_defaults(zenoh_config, 'router', timestamping=not args.no_timestamping)

    run(
        listen=args.listen,
        connect=args.connect,
        timestamping=not args.no_timestamping,
        zenoh_config=zenoh_config,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
