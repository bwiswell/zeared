"""Declarative connection spec for a zeared session.

Factories ``z.peer(...)``, ``z.client(...)``, and ``z.open(cfg)`` accept
either explicit kwargs or a :class:`SessionConfig` instance via
``config=``. The object form lets consumers share / log / diff connection
specs without threading a bag of kwargs through every layer.

Named ``SessionConfig`` (rather than ``Config``) to avoid colliding with
``zenoh.Config`` — they cohabit when users want raw Zenoh overrides via the
``zenoh_config=`` factory kwarg.
"""

from __future__ import annotations

import dataclasses
import os

import seared as s

from ._mode import Mode

_ENV_PREFIX_DEFAULT = 'ZEARED_SESSION_'


def _split_csv(value: str) -> list[str]:
    """Split a comma-separated env value into a stripped list."""
    return [item.strip() for item in value.split(',') if item.strip()]


def _parse_bool(value: str) -> bool:
    """Parse a permissive bool from an env-string value."""
    v = value.strip().lower()
    if v in ('true', '1', 'yes', 'on'):
        return True
    if v in ('false', '0', 'no', 'off', ''):
        return False
    msg = f'cannot parse {value!r} as bool'
    raise ValueError(msg)


@s.seared
class SessionConfig(s.Seared):
    """Connection spec. Pass to :func:`zeared.open` or to a factory.

    ``mode`` is required; ``connect`` / ``listen`` are endpoint lists.
    ``router`` is a convenience shortcut for ``mode='client'`` — it
    becomes the sole ``connect`` endpoint when set and ``connect`` is
    empty.

    Raw Zenoh settings zeared doesn't model — TLS/mTLS, access control,
    scouting — come from ``zenoh_config_file`` (a path to a JSON5 Zenoh
    config) and ``zenoh_overrides`` (JSON5 fragments keyed by config path,
    e.g. ``{'scouting/multicast/enabled': False}``). Without these the
    declarative path can only express what the typed fields cover, which
    is why a security posture had to be set by bypassing ``SessionConfig``
    entirely and calling a factory with ``zenoh_config=``.

    Precedence when building the underlying ``zenoh.Config``: the file
    first, then zeared's own ``mode`` / timestamping, then
    ``zenoh_overrides``. Overrides genuinely override — a ``mode`` set
    there beats :attr:`mode`.

    Retry knobs:
      - ``retry=False`` (default): ``zenoh.open`` is called once; failures
        propagate immediately.
      - ``retry=True``: ``zenoh.open`` is retried with exponential
        backoff — ``initial_backoff`` doubling up to ``max_backoff``.
        Stop after ``max_attempts`` if set, otherwise retry forever.

    Builders (all return a new ``SessionConfig``, originals unchanged):
      - :meth:`replace` — generic field update.
      - :meth:`with_retry` — retry knobs in one call.
      - :meth:`with_connect` / :meth:`with_listen` — append endpoints.
      - :meth:`set_router` — replace the client-mode router shortcut.
    """

    # fmt: off
    # Column-aligned deliberately — mirrors docs/config.md.
    mode:            Mode       = s.Enum(enum=Mode, required=True)
    router:          str | None = s.Str(default=None)      # client shortcut
    connect:         list       = s.Str(many=True, default_factory=list)
    listen:          list       = s.Str(many=True, default_factory=list)
    retry:           bool       = s.Bool(default=False)
    initial_backoff: float      = s.Float(default=0.1)
    max_backoff:     float      = s.Float(default=30.0)
    max_attempts:    int | None = s.Int(default=None)
    zenoh_config_file: str | None = s.Str(default=None)   # JSON5 Zenoh config path
    zenoh_overrides:   dict       = s.Dict(default_factory=dict)
    # fmt: on

    # -- builders -----------------------------------------------------

    def replace(self, **changes: object) -> SessionConfig:
        """Return a copy with the given fields overridden.

        Accepts any field name on ``SessionConfig``; unknown keys raise
        ``TypeError`` (from ``dataclasses.replace``).
        """
        return dataclasses.replace(self, **changes)

    def with_retry(
        self,
        retry: bool = True,  # noqa: FBT001, FBT002
        *,
        initial_backoff: float | None = None,
        max_backoff: float | None = None,
        max_attempts: int | None = None,
    ) -> SessionConfig:
        """Return a copy with retry enabled (or disabled) and any supplied backoff knobs overridden.

        Knobs left as ``None`` keep their current values.
        """
        changes: dict = {'retry': retry}
        if initial_backoff is not None:
            changes['initial_backoff'] = initial_backoff
        if max_backoff is not None:
            changes['max_backoff'] = max_backoff
        if max_attempts is not None:
            changes['max_attempts'] = max_attempts
        return dataclasses.replace(self, **changes)

    def with_connect(self, *endpoints: str) -> SessionConfig:
        """Return a copy with the given endpoints APPENDED to ``connect``."""
        return dataclasses.replace(
            self,
            connect=[*self.connect, *endpoints],
        )

    def with_listen(self, *endpoints: str) -> SessionConfig:
        """Return a copy with the given endpoints APPENDED to ``listen``."""
        return dataclasses.replace(
            self,
            listen=[*self.listen, *endpoints],
        )

    @classmethod
    def from_env(cls, prefix: str = _ENV_PREFIX_DEFAULT) -> SessionConfig:
        """Build a ``SessionConfig`` from environment variables.

        Env keys are read as ``{prefix}{FIELD_NAME}``, uppercase with
        underscores (the default ``prefix='ZEARED_SESSION_'`` yields
        ``ZEARED_SESSION_MODE``, ``ZEARED_SESSION_RETRY``, etc.).

        | Env var | Field | Notes |
        |---------|-------|-------|
        | ``MODE`` | ``mode`` | required; ``'peer'`` / ``'client'`` |
        | ``ROUTER`` | ``router`` | optional; single endpoint |
        | ``CONNECT`` | ``connect`` | optional; comma-separated endpoint list |
        | ``LISTEN`` | ``listen`` | optional; comma-separated endpoint list |
        | ``RETRY`` | ``retry`` | optional; ``'true'``/``'false'`` (case-insensitive) |
        | ``INITIAL_BACKOFF`` | ``initial_backoff`` | optional; float seconds |
        | ``MAX_BACKOFF`` | ``max_backoff`` | optional; float seconds |
        | ``MAX_ATTEMPTS`` | ``max_attempts`` | optional; int |
        | ``ZENOH_CONFIG_FILE`` | ``zenoh_config_file`` | optional; path to a JSON5 Zenoh config |

        ``zenoh_overrides`` is deliberately absent from the env surface —
        a nested JSON5 mapping doesn't survive a flat env string legibly.
        Put it in the TOML / JSON the config is loaded from, or point
        ``ZENOH_CONFIG_FILE`` at a file.

        Missing optional fields fall back to seared defaults. Missing
        required ``MODE`` raises ``ValidationError`` via ``cls.load``.
        """
        data: dict = {}
        for env_key, field_name, coercer in (
            ('MODE', 'mode', str),
            ('ROUTER', 'router', str),
            ('CONNECT', 'connect', _split_csv),
            ('LISTEN', 'listen', _split_csv),
            ('RETRY', 'retry', _parse_bool),
            ('INITIAL_BACKOFF', 'initial_backoff', float),
            ('MAX_BACKOFF', 'max_backoff', float),
            ('MAX_ATTEMPTS', 'max_attempts', int),
            ('ZENOH_CONFIG_FILE', 'zenoh_config_file', str),
        ):
            v = os.environ.get(f'{prefix}{env_key}')
            if v is None or v == '':
                continue
            data[field_name] = coercer(v)
        return cls.load(data)

    # NOTE: ``SessionConfig.from_yaml`` / ``to_yaml`` / ``from_toml`` /
    # ``to_toml`` / ``from_csv`` / ``to_csv`` / ``from_json`` / ``to_json``
    # are auto-attached by seared 0.1.10's decorator. The bespoke
    # ``from_yaml`` that lived here in 0.0.15 has been removed in 0.0.16
    # — seared's auto-attached version produces an identical result with
    # the canonical install hint pointing at ``seared[yaml]``.

    def set_router(self, router: str) -> SessionConfig:
        """Return a copy with ``router`` set (replaces any existing value).

        Named ``set_router`` rather than ``with_router`` for builder-verb
        consistency: ``with_*`` appends (``with_connect``, ``with_listen``);
        ``set_*`` replaces. ``router`` is a scalar field, so only replace
        makes sense.
        """
        return dataclasses.replace(self, router=router)
