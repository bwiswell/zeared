from __future__ import annotations

import datetime
import logging
import re
from typing import TYPE_CHECKING, Optional

import seared as s

from . import _codec as codec

if TYPE_CHECKING:
    import zenoh


_log = logging.getLogger('zeared.meta')


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
    """
    key_expr:    str                          = s.Str(required=True)
    timestamp:   Optional[str]                = s.Str()          # raw HLC string
    issued_at:   Optional[datetime.datetime]  = s.DateTime()     # parsed UTC
    encoding:    Optional[str]                = s.Str()
    source_info: Optional[str]                = s.Str()
    attachment:  Optional[bytes]              = s.Bytes()
    schema:      Optional[str]                = s.Str()
    captures:    dict                         = s.Dict(missing={})


# Zenoh HLC sample timestamp shape: ``<8-hex-seconds><8-hex-frac>/<id>``.
# Format documented in Zenoh's protocol spec; parser is permissive — falls
# back to None on any error.
_HLC_RE = re.compile(r'^([0-9a-fA-F]{16})/')


def _parse_hlc(ts) -> Optional[datetime.datetime]:
    """Parse a Zenoh HLC sample timestamp into a UTC ``datetime``.

    Zenoh HLC is a 64-bit NTP-style fixed-point timestamp followed by a
    node id; high 32 bits are seconds since 1970, low 32 bits are
    fractional. Falls back to ``None`` on any parse failure.
    """
    if ts is None:
        return None
    s_repr = str(ts)
    m = _HLC_RE.match(s_repr)
    if not m:
        return None
    try:
        ntp64 = int(m.group(1), 16)
        seconds = ntp64 >> 32
        fraction = ntp64 & 0xFFFFFFFF
        # Convert NTP fractional 32-bit field to seconds.
        frac_seconds = fraction / (1 << 32)
        return datetime.datetime.fromtimestamp(
            seconds + frac_seconds, tz=datetime.timezone.utc,
        )
    except (ValueError, OSError, OverflowError):
        return None


def _parse_attachment_schema(attachment: Optional[bytes]) -> Optional[str]:
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


def from_sample(sample: 'zenoh.Sample') -> ZenohMeta:
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

    attach = sample.attachment
    attach_bytes = bytes(attach) if attach is not None else None

    schema = _parse_attachment_schema(attach_bytes)

    return ZenohMeta(
        key_expr=str(sample.key_expr),
        timestamp=ts_str,
        issued_at=issued_at,
        encoding=enc_str,
        source_info=src_str,
        attachment=attach_bytes,
        schema=schema,
        captures={},
    )
