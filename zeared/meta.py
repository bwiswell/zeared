from __future__ import annotations

import datetime
import enum
import logging
import re
from typing import TYPE_CHECKING

import seared as s

from . import _codec as codec

if TYPE_CHECKING:
    import zenoh


_log = logging.getLogger('zeared.meta')


class Origin(enum.StrEnum):
    """Provenance of a dispatched sample — how it reached this subscriber.

    Determined entirely by the local delivery path (never read from the
    wire): the live ``zenoh.Subscriber`` callback, a retained-fetch
    ``session.get`` reply, or the presence observer's will synthesis.
    """

    LIVE = 'live'  # delivered by the live zenoh subscriber
    REPLAY = 'replay'  # from a retention cache (subscribe-time or post-reconnect)
    WILL = 'will'  # synthesised by the presence observer


@s.seared
class ZenohMeta(s.Seared):
    """Per-message Zenoh metadata surfaced to 2-arg subscribers.

    Kept intentionally narrow: string/bytes primitives so downstream code never
    imports Zenoh types. Advanced users who need the raw ``zenoh.Sample`` should
    drop to the underlying Zenoh API.

    ``captures`` holds the template-slot values extracted from the incoming
    key expression. It's always populated when the message class has declared
    templates (empty dict if no slots matched / no templates on the class).

    ``schema`` carries the publisher's class-level ``SCHEMA`` value when set
    (msgpack-decoded from ``sample.attachment``), or ``None`` when the
    publisher didn't stamp a schema (or the attachment didn't decode).

    ``issued_at`` is parsed from ``sample.timestamp`` (Zenoh HLC) when
    timestamping is enabled. ``None`` when no timestamp on the sample
    (synthesised wills, raw publishes pre-0.0.13, etc.).

    ``origin_zid`` is the Zenoh session id of whoever stamped the sample's
    HLC — normally the publisher. It is **attribution, not
    authentication**: ``Session.put`` / ``Publisher.put`` both accept a
    caller-supplied ``timestamp=``, so a hostile publisher can claim any
    zid. Use it for logging, forensics, and catching a misconfigured node;
    do not use it to authorize anything. Real origin enforcement belongs
    at the router (Zenoh access-control keyed on a TLS certificate).
    ``None`` when the sample carries no timestamp.

    ``origin`` is the sample's provenance — ``Origin.LIVE`` for a real
    publish through the live subscriber, ``Origin.REPLAY`` for a
    retained-fetch delivery (subscribe-time or post-reconnect), and
    ``Origin.WILL`` for a presence-synthesised will. Set by the dispatch
    layer; ``from_sample`` defaults it to ``LIVE``.
    """

    # fmt: off
    # Column-aligned deliberately — mirrors docs/meta.md and reads as a
    # wire-contract table. The formatter would collapse it.
    key_expr:    str                      = s.Str(required=True)
    timestamp:   str | None               = s.Str(default=None)       # raw HLC string
    issued_at:   datetime.datetime | None = s.DateTime(default=None)  # parsed UTC
    encoding:    str | None               = s.Str(default=None)
    source_info: str | None               = s.Str(default=None)
    origin_zid:  str | None               = s.Str(default=None)      # HLC id half
    attachment:  bytes | None             = s.Bytes(default=None)
    schema:      str | None               = s.Str(default=None)
    captures:    dict                     = s.Dict(default_factory=dict)
    origin:      Origin                   = s.Enum(enum=Origin, default=Origin.LIVE)
    # fmt: on


# Zenoh HLC sample timestamp shape: ``<ntp64>/<id>``. The numeric half is
# a 64-bit NTP fixed-point value (high 32 bits = seconds since 1970, low
# 32 = fraction); ``zenoh.Timestamp.__str__`` renders it in **decimal**.
#
# The digits are accepted in either base because this regex only sees the
# string fallback path: a real ``zenoh.Timestamp`` is read through
# ``get_time()`` / ``get_id()`` below. Which base a 16-digit run is meant
# to be is genuinely ambiguous, so ``_ntp64_seconds`` disambiguates on the
# epoch it yields rather than guessing from the text.
_HLC_RE = re.compile(r'^([0-9a-fA-F]+)/')

# Any decode landing outside [2000-01-01, 2100-01-01) is the wrong base.
_EPOCH_PLAUSIBLE_MIN = 946684800
_EPOCH_PLAUSIBLE_MAX = 4102444800


def _ntp64_seconds(raw: str) -> float | None:
    """Decode the numeric half of an HLC string into epoch seconds.

    Tries decimal first (what Zenoh actually emits), then hex, and keeps
    whichever yields a plausible wall-clock time.
    """
    for base in (10, 16):
        try:
            ntp64 = int(raw, base)
        except ValueError:
            continue
        seconds = ntp64 >> 32
        if _EPOCH_PLAUSIBLE_MIN <= seconds <= _EPOCH_PLAUSIBLE_MAX:
            # Low 32 bits are an NTP fractional-second field.
            return seconds + (ntp64 & 0xFFFFFFFF) / (1 << 32)
    return None


def _parse_hlc_string(s_repr: str) -> datetime.datetime | None:
    """Parse the ``<ntp64>/<id>`` string form into a UTC ``datetime``."""
    m = _HLC_RE.match(s_repr)
    if not m:
        return None
    epoch = _ntp64_seconds(m.group(1))
    if epoch is None:
        return None
    try:
        return datetime.datetime.fromtimestamp(epoch, tz=datetime.UTC)
    except ValueError, OSError, OverflowError:
        return None


def _parse_hlc(ts: object) -> datetime.datetime | None:
    """Parse a Zenoh HLC sample timestamp into a UTC ``datetime``.

    Prefers ``zenoh.Timestamp.get_time()``, which returns an aware UTC
    ``datetime`` directly. Falls back to parsing the string form for
    hand-built values and older bindings. Permissive — ``None`` on any
    failure, so a malformed timestamp never breaks dispatch.
    """
    if ts is None:
        return None
    get_time = getattr(ts, 'get_time', None)
    if get_time is not None:
        try:
            return get_time()
        except Exception:  # noqa: BLE001
            return None
    return _parse_hlc_string(str(ts))


def _parse_origin_zid(ts: object) -> str | None:
    """Extract the publishing session's zid from a Zenoh HLC timestamp.

    The HLC's id half is the zid of whichever session stamped the sample —
    normally the publisher. Prefers ``zenoh.Timestamp.get_id()``; falls
    back to the string suffix. ``None`` when absent or unparseable.

    **Attribution, not authentication** — see :attr:`ZenohMeta.origin_zid`.
    """
    if ts is None:
        return None
    get_id = getattr(ts, 'get_id', None)
    if get_id is not None:
        try:
            return str(get_id())
        except Exception:  # noqa: BLE001
            return None
    _, sep, tail = str(ts).partition('/')
    return tail or None if sep else None


def _parse_attachment_schema(attachment: bytes | None) -> str | None:
    """Extract the ``schema`` field from a Zenoh attachment payload.

    Returns the schema string when present, ``None`` when the attachment
    is absent / undecodable / lacks the field. Defensive — never raises.
    """
    if not attachment:
        return None
    try:
        att_dict = codec.unpack(attachment, 'msgpack')
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(att_dict, dict):
        return None
    schema = att_dict.get('schema')
    return schema if isinstance(schema, str) else None


def build_attachment_schema(schema: str | None) -> bytes | None:
    """Pack a ``schema`` value into the msgpack attachment shape.

    Inverse of :func:`_parse_attachment_schema` — returns the wire bytes a
    subscriber's schema check reads, or ``None`` when ``schema`` is unset.
    Used to stamp the attachment on a synthesised will sample so the
    presence path passes the same schema check as a live publish (the
    ``Message._schema_attachment_bytes`` classmethod caches the equivalent
    for the publish path).
    """
    if schema is None:
        return None
    return codec.pack({'schema': schema}, 'msgpack')


def from_sample(sample: zenoh.Sample) -> ZenohMeta:
    """Build a ``ZenohMeta`` from a Zenoh ``Sample``.

    ``captures`` starts empty — the subscriber fills it in from the matched
    template before invoking the user callback. ``schema`` is parsed from
    the attachment if present; ``issued_at`` is parsed from the sample's
    HLC timestamp when timestamping is enabled.
    """
    ts = sample.timestamp
    ts_str = str(ts) if ts is not None else None
    issued_at = _parse_hlc(ts)

    enc = sample.encoding
    enc_str = str(enc) if enc is not None else None

    src = sample.source_info
    src_str = str(src) if src is not None else None

    # Zenoh leaves ``source_info`` unset on an ordinary publish (it is only
    # populated when a caller passes ``source_info=``), so the HLC's id
    # half is the only origin signal actually present on the wire.
    origin_zid = _parse_origin_zid(ts)

    attach = sample.attachment
    attach_bytes = bytes(attach) if attach is not None else None

    schema = _parse_attachment_schema(attach_bytes)

    return ZenohMeta(
        key_expr=str(sample.key_expr),
        timestamp=ts_str,
        issued_at=issued_at,
        encoding=enc_str,
        source_info=src_str,
        origin_zid=origin_zid,
        attachment=attach_bytes,
        schema=schema,
        captures={},
    )
