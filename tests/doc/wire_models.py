"""Fixture Message classes for the zeared doc layer."""

from __future__ import annotations

import zeared as z


@z.zeared
class _CmdBase(z.Message):
    """Private base — should be excluded from the doc set."""

    source: str = z.Str(required=True)


@z.zeared
class PingRequest(z.Zeared):
    """Ping request payload (embedded, not published on a topic)."""

    deep: bool = z.Bool(default=False)


@z.zeared
class Ping(_CmdBase):
    """:class:`Ping` a reader (a queryable)."""

    TOPIC = 'rio/command/ping/{source}/{reader_id}'
    SCHEMA = '2'
    RETAINED = False
    DEDUPE = True
    REQUEST = PingRequest

    reader_id: int = z.Int(required=True)
    note: str | None = z.Str(default=None, doc='free-text note')


@z.zeared
class SetMode(_CmdBase):
    """Put a device into a named mode (a queryable that changes state)."""

    TOPIC = 'device/command/mode/{source}/{device_id}'
    EFFECT = 'write'  # declared by name, to exercise normalisation (0.3.12)

    device_id: int = z.Int(required=True)
    mode: str = z.Str(required=True)
