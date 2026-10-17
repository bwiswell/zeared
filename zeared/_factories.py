"""Session-opening factories and their shared helpers.

Session-opening factories — ``peer`` / ``client`` / ``open`` plus the shared retry /
config-building / managed-wrap helpers.

Pulled out of ``__init__.py`` so the package init can stay a thin
re-export-and-glue module under the 300-line cap. Public names are
re-exported by ``__init__.py``.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any

import zenoh

from ._managed_session import ManagedSession
from ._mode import Mode

if TYPE_CHECKING:
    from collections.abc import Callable

    from .config import SessionConfig

_log_connect = logging.getLogger('zeared.connect')

_MISSING = object()

# Reconnect attempts logged at INFO before escalating to WARNING.
_QUIET_ATTEMPTS = 3

#: Zenoh's shared-memory transport — off in every config zeared builds. On (Zenoh's default), a
#: put, query or reply over 3 KB between two processes on one host travels through a shared-memory
#: pool, 16 MB a session, instead of over the link. The receiver locks the pages it maps and keeps
#: every closed session's segment mapped, so a long-lived hub with sessions coming and going climbs
#: toward ``RLIMIT_MEMLOCK`` (64 MB on a stock Linux box). Once there, opening the next segment
#: fails and Zenoh drops the message, logging it only at DEBUG: large messages simply stop
#: arriving. Only links within one host ever use it, and it is agreed per link, so a hub with it
#: off keeps it off every link through the hub.
_SHARED_MEMORY = 'transport/shared_memory/enabled'

#: Where a hub listens when nothing names its endpoints: no ``listen=``, no
#: ``SessionConfig.listen``, and a Zenoh config (if any) that leaves ``listen/endpoints`` unset.
_DEFAULT_HUB_LISTEN = ('tcp/0.0.0.0:7447',)


def _endpoints_set(c: zenoh.Config, key: str) -> bool:
    """Whether config ``c`` names its own endpoints at ``key`` (``listen/endpoints`` / ``connect/endpoints``).

    An unset key reads back as Zenoh's own default: a mode-keyed map for ``listen/endpoints``
    (``{"router": [...], "peer": [...]}``) and ``[]`` for ``connect/endpoints``. An explicit one
    reads back as a list. So "differs from a pristine config" is the test; an explicit ``[]``
    counts as unset, which is what it means.
    """
    return json.loads(c.get_json(key)) != json.loads(zenoh.Config().get_json(key))


def _config_endpoints(c: zenoh.Config | None, key: str) -> list:
    """The endpoints config ``c`` names at ``key``, or ``[]`` when it names none (or there is no config)."""
    if c is None or not _endpoints_set(c, key):
        return []
    value = json.loads(c.get_json(key))
    return list(value) if isinstance(value, list) else [value]


def _resolve_hub_listen(
    listen: list | None,
    config: SessionConfig | None,
    zenoh_config: zenoh.Config | None,
) -> tuple[list | None, list]:
    """Pick a hub's listen endpoints: ``(to write over the config, that the hub will bind)``.

    In order: ``listen=`` (when given and non-empty), then ``SessionConfig.listen``; else the
    endpoints the resolved Zenoh config already names, left in place (nothing to write); else
    ``tcp/0.0.0.0:7447``. ``zenoh_config`` is the *resolved* config: a raw one, or one built from
    ``zenoh_config_file`` / ``zenoh_overrides``, or ``None`` when zeared builds the whole config.

    Before 0.3.9 the last two steps didn't exist: every hub wrote ``listen/endpoints`` over its
    config, so a config file's own listeners (a TLS-only one, say) were never bound.
    """
    explicit = list(config.listen) or None if config is not None else None
    if listen is not None:
        explicit = list(listen)
    if explicit:
        return explicit, explicit
    own = _config_endpoints(zenoh_config, 'listen/endpoints')
    if own:
        return None, own
    default = list(_DEFAULT_HUB_LISTEN)
    return default, default


def _apply_defaults(c: zenoh.Config, mode: str, *, timestamping: bool) -> None:
    """Set zeared's own settings on a config it builds: the mode, HLC timestamping, shared memory off.

    Timestamping is on unless opted out (RETAINED + DEDUPE need the HLC).
    """
    c.insert_json5('mode', json.dumps(mode))
    if timestamping:
        c.insert_json5('timestamping/enabled', 'true')
    c.insert_json5(_SHARED_MEMORY, 'false')


def _open_with_retry(  # noqa: PLR0913
    open_fn: Callable[[], zenoh.Session],
    *,
    retry: bool,
    initial_backoff: float,
    max_backoff: float,
    max_attempts: int | None,
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
        except Exception as e:
            attempts += 1
            if max_attempts is not None and attempts >= max_attempts:
                raise
            level = logging.INFO if attempts <= _QUIET_ATTEMPTS else logging.WARNING
            _log_connect.log(
                level,
                '%s connect failed (attempt %d): %s — retrying in %.1fs',
                endpoint_label,
                attempts,
                e,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        else:
            if attempts > 0:
                _log_connect.info(
                    'connected to %s after %d retries',
                    endpoint_label,
                    attempts,
                )
            return sess


def _resolve_zenoh_config(
    config: SessionConfig | None,
    zenoh_config: zenoh.Config | None,
) -> zenoh.Config | None:
    """Build a ``zenoh.Config`` from a ``SessionConfig``'s raw-Zenoh fields.

    Returns ``None`` when there is nothing raw to apply, which leaves the
    ``_build_config_for_*`` helpers on their normal "zeared builds the
    whole config" branch.

    An explicit ``zenoh_config=`` kwarg always wins — it is the lower-level
    escape hatch and a caller who reaches for it has said what they want.

    Precedence within the built config: ``zenoh_config_file`` first, then
    zeared's own settings — ``mode``, timestamping, shared memory off (which
    the ``_build_config_for_*`` helpers skip once they are handed a config,
    so they have to be set here) — then ``zenoh_overrides``. Overrides land
    last so the field name is honest: they are how a session opts back in to
    shared memory.

    This is what makes a security posture — mTLS, access control, scouting
    off — expressible through the declarative ``SessionConfig`` path
    instead of only through a hand-built ``zenoh.Config``.
    """
    if zenoh_config is not None:
        return zenoh_config
    if config is None:
        return None
    has_file = bool(config.zenoh_config_file)
    has_overrides = bool(config.zenoh_overrides)
    if not has_file and not has_overrides:
        return None

    c = zenoh.Config.from_file(config.zenoh_config_file) if has_file else zenoh.Config()
    # ``mode`` is a required SessionConfig field, so it is always known; the
    # rest mirrors the factories' own defaults.
    _apply_defaults(c, config.mode.value, timestamping=True)
    for key, value in config.zenoh_overrides.items():
        c.insert_json5(str(key), json.dumps(value))
    return c


def _build_config_for_peer(
    connect: list | None,
    listen: list | None,
    zenoh_config: zenoh.Config | None,
    *,
    timestamping: bool = True,
) -> zenoh.Config:
    # User let us build the config when ``zenoh_config is None`` — zeared's
    # own settings apply (``timestamping=False`` opts out of the HLC). A
    # caller's own ``zenoh_config`` is taken as given.
    c = zenoh_config if zenoh_config is not None else zenoh.Config()
    if zenoh_config is None:
        _apply_defaults(c, 'peer', timestamping=timestamping)
    if connect:
        c.insert_json5('connect/endpoints', json.dumps(connect))
    if listen:
        c.insert_json5('listen/endpoints', json.dumps(listen))
    return c


def _build_config_for_client(
    endpoints: list | None,
    zenoh_config: zenoh.Config | None,
    *,
    timestamping: bool = True,
) -> zenoh.Config:
    # Endpoints are written over the config only when given; otherwise the ones the
    # config already names stand (``client()`` checks that there are some).
    c = zenoh_config if zenoh_config is not None else zenoh.Config()
    if zenoh_config is None:
        _apply_defaults(c, 'client', timestamping=timestamping)
    if endpoints:
        c.insert_json5('connect/endpoints', json.dumps(endpoints))
    return c


def _build_config_for_router(
    listen: list | None,
    connect: list | None,
    zenoh_config: zenoh.Config | None,
    *,
    timestamping: bool = True,
) -> zenoh.Config:
    # A hub is a router that relays between nodes that can't reach each other
    # directly (e.g. both NAT-gated, outbound-only). It routes pub/sub,
    # queries, and liveliness in-process — no ``zenohd`` binary required.
    # Endpoints are written over the config only when given: ``listen=None``
    # keeps the config's own (``_resolve_hub_listen`` decides).
    c = zenoh_config if zenoh_config is not None else zenoh.Config()
    if zenoh_config is None:
        _apply_defaults(c, 'router', timestamping=timestamping)
    if listen is not None:
        c.insert_json5('listen/endpoints', json.dumps(listen))
    if connect:
        c.insert_json5('connect/endpoints', json.dumps(connect))
    return c


def _wrap_managed(  # noqa: PLR0913, PLR0917
    raw: zenoh.Session,
    open_fn: Callable[[], zenoh.Session],
    label: str,
    initial_backoff: float,
    max_backoff: float,
    max_attempts: int | None,
    probe_interval: float,
) -> ManagedSession:
    from ._reconnect import start_probe

    sess = ManagedSession(
        raw,
        open_fn,
        endpoint_label=label,
        probe_interval=probe_interval,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
        max_attempts=max_attempts,
    )
    start_probe(sess)
    return sess


def _resolve_retry_knobs(
    config: SessionConfig | None,
    # Each may be the `_MISSING` sentinel or a real value; the body narrows
    # by identity. `Any` is a placeholder: typing these properly needs a
    # dedicated sentinel type (`Union[T, _MissingType]`) so the declared
    # signature stops erasing the real parameter types. Deferred — it is a
    # public-signature change with no runtime effect.
    retry: Any,
    initial_backoff: Any,
    max_backoff: Any,
    max_attempts: Any,
) -> tuple[bool, float, float, int | None]:
    """Layer explicit retry kwargs over a ``SessionConfig`` base; return ``(retry_b, initial_b, max_b, max_a)``.

    Shared by ``peer`` / ``client``.
    """
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


def _resolve_session_knobs(
    config: SessionConfig | None,
    # ``_MISSING`` or a real value, as in ``_resolve_retry_knobs``.
    auto_reconnect: Any,
    probe_interval: Any,
    resync_jitter: Any,
    resync_concurrency: Any,
) -> tuple[bool, float, float, int]:
    """Layer explicit session kwargs over a ``SessionConfig`` base (0.4.0).

    Returns ``(auto_reconnect, probe_interval, resync_jitter, resync_concurrency)``.
    An explicit keyword wins; an absent one defers to ``config``, then to the
    defaults (``False``, ``10.0``, ``2.0``, ``4``). Shared by ``peer`` / ``client``.
    """
    if config is not None:
        out = [bool(config.auto_reconnect), config.probe_interval, config.resync_jitter, config.resync_concurrency]
    else:
        out = [False, 10.0, 2.0, 4]
    for i, explicit in enumerate((auto_reconnect, probe_interval, resync_jitter, resync_concurrency)):
        if explicit is not _MISSING:
            out[i] = explicit
    ar, pi, rj, rc = out
    return bool(ar), pi, float(rj), int(rc)


def _finalise_session(  # noqa: PLR0913
    raw: zenoh.Session,
    _open: Callable[[], zenoh.Session],
    label: str,
    *,
    auto_reconnect: bool,
    retention_ttl: float | None,
    probe_interval: float,
    initial_b: float,
    max_b: float,
    max_a: int | None,
    factory_name: str,
    resync_jitter: float = 2.0,
    resync_concurrency: int = 4,
) -> zenoh.Session | ManagedSession:
    """Post-open: return raw or wrap as ManagedSession; reject ``retention_ttl`` on raw sessions.

    Shared by ``peer`` / ``client``.
    """
    if not auto_reconnect:
        if retention_ttl is not None:
            msg = (
                f'{factory_name}(retention_ttl=...) requires auto_reconnect=True; '
                'raw zenoh sessions have nowhere to stash a per-session '
                'TTL fallback. Either set auto_reconnect=True or use '
                'class-level Cls.RETENTION_TTL.'
            )
            raise TypeError(msg)
        return raw
    managed = _wrap_managed(
        raw,
        _open,
        label,
        initial_b,
        max_b,
        max_a,
        probe_interval,
    )
    if retention_ttl is not None:
        managed._retention_ttl = retention_ttl  # noqa: SLF001
    managed._resync_jitter = resync_jitter  # noqa: SLF001
    managed._resync_concurrency = resync_concurrency  # noqa: SLF001
    return managed


def peer(  # noqa: PLR0913
    *,
    connect: list | None = None,
    listen: list | None = None,
    config: SessionConfig | None = None,
    zenoh_config: zenoh.Config | None = None,
    retry: object = _MISSING,
    initial_backoff: object = _MISSING,
    max_backoff: object = _MISSING,
    max_attempts: object = _MISSING,
    auto_reconnect: object = _MISSING,
    probe_interval: object = _MISSING,
    timestamping: bool = True,
    retention_ttl: float | None = None,
    resync_jitter: object = _MISSING,
    resync_concurrency: object = _MISSING,
) -> zenoh.Session | ManagedSession:
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
        config,
        retry,
        initial_backoff,
        max_backoff,
        max_attempts,
    )
    auto_reconnect, probe_interval, resync_jitter, resync_concurrency = _resolve_session_knobs(
        config,
        auto_reconnect,
        probe_interval,
        resync_jitter,
        resync_concurrency,
    )

    if connect is not None:
        base_connect = connect
    if listen is not None:
        base_listen = listen

    zenoh_config = _resolve_zenoh_config(config, zenoh_config)

    label = f'peer(connect={base_connect or []}, listen={base_listen or []})'

    def _open() -> zenoh.Session:
        cfg = _build_config_for_peer(
            base_connect,
            base_listen,
            zenoh_config,
            timestamping=timestamping,
        )
        return zenoh.open(cfg)

    raw = _open_with_retry(
        _open,
        retry=retry_b,
        initial_backoff=initial_b,
        max_backoff=max_b,
        max_attempts=max_a,
        endpoint_label=label,
    )
    return _finalise_session(
        raw,
        _open,
        label,
        auto_reconnect=auto_reconnect,
        retention_ttl=retention_ttl,
        probe_interval=probe_interval,
        initial_b=initial_b,
        max_b=max_b,
        max_a=max_a,
        factory_name='peer',
        resync_jitter=resync_jitter,
        resync_concurrency=resync_concurrency,
    )


def client(  # noqa: PLR0913
    router: str | list | None = None,
    *,
    config: SessionConfig | None = None,
    zenoh_config: zenoh.Config | None = None,
    retry: object = _MISSING,
    initial_backoff: object = _MISSING,
    max_backoff: object = _MISSING,
    max_attempts: object = _MISSING,
    auto_reconnect: object = _MISSING,
    probe_interval: object = _MISSING,
    timestamping: bool = True,
    retention_ttl: float | None = None,
    resync_jitter: object = _MISSING,
    resync_concurrency: object = _MISSING,
) -> zenoh.Session | ManagedSession:
    """Open a Zenoh client-mode session connected to one or more routers.

    Pass ``config=<Config>`` for a declarative base spec, then layer any
    explicit kwargs on top — kwargs win when both are supplied.
    ``zenoh_config=`` layers raw Zenoh overrides on top.
    """
    if config is not None:
        endpoints = list(config.connect)
        if config.router:
            endpoints = [config.router, *endpoints]
    else:
        endpoints = []
    retry_b, initial_b, max_b, max_a = _resolve_retry_knobs(
        config,
        retry,
        initial_backoff,
        max_backoff,
        max_attempts,
    )
    auto_reconnect, probe_interval, resync_jitter, resync_concurrency = _resolve_session_knobs(
        config,
        auto_reconnect,
        probe_interval,
        resync_jitter,
        resync_concurrency,
    )

    if router is not None:
        endpoints = [router] if isinstance(router, str) else list(router)

    zenoh_config = _resolve_zenoh_config(config, zenoh_config)

    # No endpoint fields: the connect endpoints the Zenoh config already names
    # (a raw ``zenoh_config``, a ``zenoh_config_file`` or ``zenoh_overrides``)
    # stand, as every other mode's do. Only with none anywhere is it an error.
    effective = endpoints or _config_endpoints(zenoh_config, 'connect/endpoints')
    if not effective:
        msg = (
            'client(): need router=<endpoint(s)>, config=SessionConfig(... with connect/router), '
            'or connect endpoints in zenoh_config / zenoh_config_file / zenoh_overrides'
        )
        raise TypeError(msg)

    label = f'client(connect={effective})'

    def _open() -> zenoh.Session:
        cfg = _build_config_for_client(
            endpoints or None,
            zenoh_config,
            timestamping=timestamping,
        )
        return zenoh.open(cfg)

    raw = _open_with_retry(
        _open,
        retry=retry_b,
        initial_backoff=initial_b,
        max_backoff=max_b,
        max_attempts=max_a,
        endpoint_label=label,
    )
    return _finalise_session(
        raw,
        _open,
        label,
        auto_reconnect=auto_reconnect,
        retention_ttl=retention_ttl,
        probe_interval=probe_interval,
        initial_b=initial_b,
        max_b=max_b,
        max_a=max_a,
        factory_name='client',
        resync_jitter=resync_jitter,
        resync_concurrency=resync_concurrency,
    )


def hub(  # noqa: PLR0913
    *,
    listen: list | None = None,
    connect: list | None = None,
    config: SessionConfig | None = None,
    zenoh_config: zenoh.Config | None = None,
    retry: object = _MISSING,
    initial_backoff: object = _MISSING,
    max_backoff: object = _MISSING,
    max_attempts: object = _MISSING,
    timestamping: bool = True,
) -> zenoh.Session:
    """Open a Zenoh router-mode session — a relay hub.

    A hub lets nodes that can't reach each other directly still communicate:
    each connects **outbound** to the hub (e.g. two peers behind NAT, both
    outbound-only), and the hub routes pub/sub, queries, and liveliness
    between them — everything zeared needs (retention/queryables and
    presence/LWT included). This is the ``zenohd`` role, run in-process; no
    external binary. The routing runs in Zenoh's Rust core, so throughput
    matches a standalone router.

    ``listen`` is the set of endpoints the hub binds. Without it (or
    ``config.listen``), the hub binds the endpoints its Zenoh config already
    names (from ``zenoh_config=``, ``zenoh_config_file`` or
    ``zenoh_overrides``), and ``['tcp/0.0.0.0:7447']`` only when nothing names
    any. ``connect`` optionally links this hub to other hubs for a multi-hub
    mesh. Point nodes at it with ``z.client(router='tcp/host:7447')``
    — client mode routes everything through the hub and never attempts the
    direct peer links that would fail under NAT.

    Returns a raw :class:`zenoh.Session`: a hub is a listener with no
    zeared-owned resources to supervise, so there is no ``ManagedSession``
    wrapper. Secure a public hub with TLS / access-control via
    ``zenoh_config=`` (or the daemon's ``--config`` file). A raw
    ``zenoh_config=`` is taken as given, Zenoh's own defaults included — set
    ``transport/shared_memory/enabled`` false in it (the daemon does that for
    its file).
    """
    base_connect = list(config.connect) or None if config is not None else None
    retry_b, initial_b, max_b, max_a = _resolve_retry_knobs(
        config,
        retry,
        initial_backoff,
        max_backoff,
        max_attempts,
    )
    if connect is not None:
        base_connect = connect

    zenoh_config = _resolve_zenoh_config(config, zenoh_config)
    write_listen, effective_listen = _resolve_hub_listen(listen, config, zenoh_config)
    effective_connect = base_connect or _config_endpoints(zenoh_config, 'connect/endpoints')

    label = f'hub(listen={effective_listen}, connect={effective_connect})'

    def _open() -> zenoh.Session:
        cfg = _build_config_for_router(
            write_listen,
            base_connect,
            zenoh_config,
            timestamping=timestamping,
        )
        return zenoh.open(cfg)

    return _open_with_retry(
        _open,
        retry=retry_b,
        initial_backoff=initial_b,
        max_backoff=max_b,
        max_attempts=max_a,
        endpoint_label=label,
    )


def open(cfg: SessionConfig) -> zenoh.Session | ManagedSession:  # noqa: A001 — shadows builtin intentionally
    """Open a session from a :class:`SessionConfig`. Unified entry point.

    Dispatches to :func:`peer` / :func:`client` / :func:`hub` based on
    ``cfg.mode``. Returns a ``ManagedSession`` when ``cfg.auto_reconnect``
    is set (0.4.0), a raw ``zenoh.Session`` otherwise. A router (hub) is
    what others reconnect to, so ``auto_reconnect`` there raises
    ``ValueError``.
    """
    if cfg.mode is Mode.PEER:
        return peer(config=cfg)
    if cfg.mode is Mode.CLIENT:
        return client(config=cfg)
    if cfg.mode is Mode.ROUTER:
        if cfg.auto_reconnect:
            msg = 'SessionConfig.auto_reconnect applies to peer and client modes, not router (a hub)'
            raise ValueError(msg)
        return hub(config=cfg)
    msg = f'SessionConfig.mode unrecognised: {cfg.mode!r}'
    raise ValueError(msg)
