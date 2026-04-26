"""Session-opening factories — ``peer`` / ``client`` / ``open`` plus the
shared retry / config-building / managed-wrap helpers.

Pulled out of ``__init__.py`` so the package init can stay a thin
re-export-and-glue module under the 300-line cap. Public names are
re-exported by ``__init__.py``.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Callable, Optional, Union

import zenoh

from ._managed_session import ManagedSession
from ._mode import Mode
from .config import SessionConfig


_log_connect = logging.getLogger('zeared.connect')

_MISSING = object()


def _open_with_retry(
    open_fn: Callable[[], zenoh.Session],
    *,
    retry: bool,
    initial_backoff: float,
    max_backoff: float,
    max_attempts: Optional[int],
    endpoint_label: str,
) -> zenoh.Session:
    """Call ``open_fn`` once or retry with exponential backoff.

    Logs at INFO for the first three retry attempts and at WARNING from
    the fourth onward. Sleeps via ``time.sleep`` — sync callers can
    interrupt via ``KeyboardInterrupt``.
    """
    if not retry:
        return open_fn()
    backoff = initial_backoff
    attempts = 0
    while True:
        try:
            sess = open_fn()
            if attempts > 0:
                _log_connect.info(
                    'connected to %s after %d retries', endpoint_label, attempts,
                )
            return sess
        except Exception as e:  # noqa: BLE001
            attempts += 1
            if max_attempts is not None and attempts >= max_attempts:
                raise
            level = logging.INFO if attempts <= 3 else logging.WARNING
            _log_connect.log(
                level,
                '%s connect failed (attempt %d): %s — retrying in %.1fs',
                endpoint_label, attempts, e, backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


def _build_config_for_peer(
    connect: Optional[list], listen: Optional[list],
    zenoh_config: Optional[zenoh.Config], *, timestamping: bool = True,
) -> zenoh.Config:
    # User let us build the config when ``zenoh_config is None`` — set
    # the mode + opt into HLC timestamping (RETAINED + DEDUPE need it).
    # ``timestamping=False`` opts back out.
    c = zenoh_config if zenoh_config is not None else zenoh.Config()
    if zenoh_config is None:
        c.insert_json5('mode', '"peer"')
        if timestamping:
            c.insert_json5('timestamping/enabled', 'true')
    if connect:
        c.insert_json5('connect/endpoints', json.dumps(connect))
    if listen:
        c.insert_json5('listen/endpoints', json.dumps(listen))
    return c


def _build_config_for_client(
    endpoints: list, zenoh_config: Optional[zenoh.Config],
    *, timestamping: bool = True,
) -> zenoh.Config:
    c = zenoh_config if zenoh_config is not None else zenoh.Config()
    if zenoh_config is None:
        c.insert_json5('mode', '"client"')
        if timestamping:
            c.insert_json5('timestamping/enabled', 'true')
    c.insert_json5('connect/endpoints', json.dumps(endpoints))
    return c


def _wrap_managed(
    raw, open_fn, label,
    initial_backoff, max_backoff, max_attempts, probe_interval,
) -> ManagedSession:
    from ._reconnect import start_probe
    sess = ManagedSession(
        raw, open_fn,
        endpoint_label=label,
        probe_interval=probe_interval,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
        max_attempts=max_attempts,
    )
    start_probe(sess)
    return sess


def _resolve_retry_knobs(
    config, retry, initial_backoff, max_backoff, max_attempts,
):
    """Layer explicit retry kwargs over a ``SessionConfig`` base; return
    ``(retry_b, initial_b, max_b, max_a)``. Shared by ``peer`` / ``client``."""
    if config is not None:
        retry_b = bool(config.retry)
        initial_b = float(config.initial_backoff)
        max_b = float(config.max_backoff)
        max_a = config.max_attempts
    else:
        retry_b, initial_b, max_b, max_a = False, 0.1, 30.0, None
    if retry is not _MISSING:
        retry_b = bool(retry)
    if initial_backoff is not _MISSING:
        initial_b = float(initial_backoff)
    if max_backoff is not _MISSING:
        max_b = float(max_backoff)
    if max_attempts is not _MISSING:
        max_a = max_attempts
    return retry_b, initial_b, max_b, max_a


def _finalise_session(
    raw, _open, label, *,
    auto_reconnect, retention_ttl, gc_interval, probe_interval,
    initial_b, max_b, max_a, factory_name,
):
    """Post-open: return raw or wrap as ManagedSession; reject
    ``retention_ttl`` on raw sessions. Shared by ``peer`` / ``client``."""
    if not auto_reconnect:
        if retention_ttl is not None:
            raise TypeError(
                f'{factory_name}(retention_ttl=...) requires auto_reconnect=True; '
                'raw zenoh sessions have nowhere to stash a per-session '
                'TTL fallback. Either set auto_reconnect=True or use '
                'class-level Cls.RETENTION_TTL.'
            )
        return raw
    managed = _wrap_managed(
        raw, _open, label, initial_b, max_b, max_a, probe_interval,
    )
    managed._gc_interval = gc_interval
    if retention_ttl is not None:
        managed._retention_ttl = retention_ttl
    return managed


def peer(
    *,
    connect: Optional[list] = None,
    listen: Optional[list] = None,
    config: Optional[SessionConfig] = None,
    zenoh_config: Optional[zenoh.Config] = None,
    retry: object = _MISSING,
    initial_backoff: object = _MISSING,
    max_backoff: object = _MISSING,
    max_attempts: object = _MISSING,
    auto_reconnect: bool = False,
    probe_interval: float = 10.0,
    timestamping: bool = True,
    gc_interval: float = 60.0,
    retention_ttl: Optional[float] = None,
) -> 'Union[zenoh.Session, ManagedSession]':
    """Open a Zenoh peer-mode session.

    Peer nodes discover each other via scouting (multicast) or explicit
    ``connect`` endpoints; no router required.

    Pass ``config=<Config>`` for a declarative base spec, then layer any
    explicit kwargs on top — kwargs win when both are supplied.
    ``zenoh_config=<zenoh.Config>`` layers raw Zenoh overrides on top.
    """
    base_connect = list(config.connect) or None if config is not None else None
    base_listen = list(config.listen) or None if config is not None else None
    retry_b, initial_b, max_b, max_a = _resolve_retry_knobs(
        config, retry, initial_backoff, max_backoff, max_attempts,
    )

    if connect is not None:
        base_connect = connect
    if listen is not None:
        base_listen = listen

    label = f'peer(connect={base_connect or []}, listen={base_listen or []})'

    def _open():
        cfg = _build_config_for_peer(
            base_connect, base_listen, zenoh_config,
            timestamping=timestamping,
        )
        return zenoh.open(cfg)

    raw = _open_with_retry(
        _open,
        retry=retry_b, initial_backoff=initial_b,
        max_backoff=max_b, max_attempts=max_a,
        endpoint_label=label,
    )
    return _finalise_session(
        raw, _open, label,
        auto_reconnect=auto_reconnect, retention_ttl=retention_ttl,
        gc_interval=gc_interval, probe_interval=probe_interval,
        initial_b=initial_b, max_b=max_b, max_a=max_a,
        factory_name='peer',
    )


def client(
    router: 'Optional[Union[str, list]]' = None,
    *,
    config: Optional[SessionConfig] = None,
    zenoh_config: Optional[zenoh.Config] = None,
    retry: object = _MISSING,
    initial_backoff: object = _MISSING,
    max_backoff: object = _MISSING,
    max_attempts: object = _MISSING,
    auto_reconnect: bool = False,
    probe_interval: float = 10.0,
    timestamping: bool = True,
    gc_interval: float = 60.0,
    retention_ttl: Optional[float] = None,
) -> 'Union[zenoh.Session, ManagedSession]':
    """Open a Zenoh client-mode session connected to one or more routers.

    Pass ``config=<Config>`` for a declarative base spec, then layer any
    explicit kwargs on top — kwargs win when both are supplied.
    ``zenoh_config=`` layers raw Zenoh overrides on top.
    """
    if config is not None:
        endpoints = list(config.connect)
        if config.router:
            endpoints = [config.router] + endpoints
    else:
        endpoints = []
    retry_b, initial_b, max_b, max_a = _resolve_retry_knobs(
        config, retry, initial_backoff, max_backoff, max_attempts,
    )

    if router is not None:
        endpoints = [router] if isinstance(router, str) else list(router)

    if not endpoints:
        raise TypeError(
            'client(): need either router=<endpoint(s)> or '
            'config=SessionConfig(... with connect/router)'
        )

    label = f'client(connect={endpoints})'

    def _open():
        cfg = _build_config_for_client(
            endpoints, zenoh_config, timestamping=timestamping,
        )
        return zenoh.open(cfg)

    raw = _open_with_retry(
        _open,
        retry=retry_b, initial_backoff=initial_b,
        max_backoff=max_b, max_attempts=max_a,
        endpoint_label=label,
    )
    return _finalise_session(
        raw, _open, label,
        auto_reconnect=auto_reconnect, retention_ttl=retention_ttl,
        gc_interval=gc_interval, probe_interval=probe_interval,
        initial_b=initial_b, max_b=max_b, max_a=max_a,
        factory_name='client',
    )


def open(cfg: SessionConfig) -> zenoh.Session:  # noqa: A001 — shadows builtin intentionally
    """Open a session from a :class:`SessionConfig`. Unified entry point.

    Dispatches to :func:`peer` or :func:`client` based on ``cfg.mode``.
    """
    if cfg.mode is Mode.PEER:
        return peer(config=cfg)
    if cfg.mode is Mode.CLIENT:
        return client(config=cfg)
    raise ValueError(f'SessionConfig.mode unrecognised: {cfg.mode!r}')
