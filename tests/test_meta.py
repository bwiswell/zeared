from __future__ import annotations

import datetime

from zeared.meta import ZenohMeta, _parse_attachment_schema, _parse_hlc


class TestZenohMeta:
    def test_minimal(self):
        m = ZenohMeta(key_expr='robot/1/telemetry')
        assert m.key_expr == 'robot/1/telemetry'
        assert m.timestamp is None
        assert m.captures == {}
        assert m.schema is None
        assert m.issued_at is None

    def test_full(self):
        m = ZenohMeta(
            key_expr='robot/1/telemetry',
            timestamp='2026-01-01T00:00:00Z',
            encoding='application/msgpack',
            source_info='zid-abc',
            attachment=b'extra',
            schema='1.0',
            issued_at=datetime.datetime(
                2026, 1, 1, tzinfo=datetime.timezone.utc,
            ),
        )
        d = ZenohMeta.dump(m)
        assert d['encoding'] == 'application/msgpack'
        assert d['source_info'] == 'zid-abc'
        assert d['schema'] == '1.0'
        # Bytes are base64-encoded via seared's Bytes field.
        assert isinstance(d['attachment'], str)


class TestParseHLC:
    """Pin: ``_parse_hlc`` decodes Zenoh's NTP-style HLC sample timestamp
    into a UTC ``datetime``. Falls back to ``None`` defensively on any
    parse failure so a malformed/missing timestamp doesn't break dispatch."""

    def test_returns_none_on_none(self):
        assert _parse_hlc(None) is None

    def test_returns_datetime_for_well_formed_hlc(self):
        # Construct a known HLC by hand. Top 32 bits = seconds since
        # 1970; bottom 32 bits = NTP fractional seconds. Pick a known
        # second value, zero fraction, arbitrary node id.
        seconds = 1735689600  # 2025-01-01T00:00:00 UTC
        ntp64 = (seconds << 32) | 0
        hlc_str = f'{ntp64:016x}/abcdef'
        result = _parse_hlc(hlc_str)
        assert isinstance(result, datetime.datetime)
        assert result.tzinfo is not None
        # Within a fractional second of expected.
        expected = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        assert abs((result - expected).total_seconds()) < 1.0

    def test_returns_none_on_garbled(self):
        assert _parse_hlc('not-an-hlc') is None
        assert _parse_hlc('') is None


class TestParseAttachmentSchema:
    """Pin: ``_parse_attachment_schema`` extracts the ``schema`` field
    from a Zenoh attachment payload (msgpack-encoded dict). Defensive on
    every error path."""

    def test_returns_none_on_empty(self):
        assert _parse_attachment_schema(None) is None
        assert _parse_attachment_schema(b'') is None

    def test_returns_schema_when_present(self):
        from zeared import _codec as codec
        att = codec.pack({'schema': '1.0'}, 'msgpack')
        assert _parse_attachment_schema(att) == '1.0'

    def test_returns_none_when_field_absent(self):
        from zeared import _codec as codec
        att = codec.pack({'other': 'x'}, 'msgpack')
        assert _parse_attachment_schema(att) is None

    def test_returns_none_on_garbled_payload(self):
        assert _parse_attachment_schema(b'\xff\xff\xff') is None

    def test_returns_none_on_non_dict(self):
        from zeared import _codec as codec
        att = codec.pack(['not-a-dict'], 'msgpack')
        assert _parse_attachment_schema(att) is None
