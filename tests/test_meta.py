from __future__ import annotations

import contextlib
import datetime
import random

import pytest
from conftest import wait

import zeared as z
from zeared.meta import Origin, ZenohMeta, _parse_attachment_schema, _parse_hlc, _parse_origin_zid


class TestZenohMeta:
    def test_minimal(self):
        m = ZenohMeta(key_expr='robot/1/telemetry')
        assert m.key_expr == 'robot/1/telemetry'
        assert m.timestamp is None
        assert m.captures == {}
        assert m.schema is None
        assert m.issued_at is None
        assert m.origin is Origin.LIVE

    def test_full(self):
        m = ZenohMeta(
            key_expr='robot/1/telemetry',
            timestamp='2026-01-01T00:00:00Z',
            encoding='application/msgpack',
            source_info='zid-abc',
            attachment=b'extra',
            schema='1.0',
            issued_at=datetime.datetime(
                2026,
                1,
                1,
                tzinfo=datetime.UTC,
            ),
        )
        d = ZenohMeta.dump(m)
        assert d['encoding'] == 'application/msgpack'
        assert d['source_info'] == 'zid-abc'
        assert d['schema'] == '1.0'
        # Bytes are base64-encoded via seared's Bytes field.
        assert isinstance(d['attachment'], str)


class TestOrigin:
    """Pin: ``Origin`` is the provenance enum on ``meta.origin`` —
    string-valued, defaulting to ``LIVE``, round-tripping through
    seared as its string value."""

    def test_exported_from_package_root(self):
        import zeared as z

        assert z.Origin is Origin

    def test_values(self):
        assert Origin.LIVE == 'live'
        assert Origin.REPLAY == 'replay'
        assert Origin.WILL == 'will'

    def test_from_sample_shape_defaults_live(self):
        # ``from_sample`` never sets origin — the dispatch layer does.
        m = ZenohMeta(key_expr='a/b')
        assert m.origin is Origin.LIVE

    def test_round_trip_as_string_value(self):
        m = ZenohMeta(key_expr='a/b', origin=Origin.WILL)
        d = ZenohMeta.dump(m)
        assert d['origin'] == 'will'
        m2 = ZenohMeta.load(d)
        assert m2.origin is Origin.WILL


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
        expected = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
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


@pytest.fixture
def timestamped_pair():
    """Two linked peers opened through zeared's own factories.

    conftest's raw-zenoh pair fixture calls ``zenoh.open`` directly and
    never sets ``timestamping/enabled``, so its samples carry no HLC at
    all — fine for the tests that use it, useless for anything reading the
    timestamp. ``z.peer`` opts into timestamping by default (RETAINED +
    DEDUPE need it), which is what a real deployment looks like.
    """
    ep = f'tcp/127.0.0.1:{random.randint(20000, 40000)}'
    pub = z.peer(listen=[ep])
    sub = z.peer(connect=[ep])
    wait(0.3)
    try:
        yield pub, sub
    finally:
        for sess in (pub, sub):
            with contextlib.suppress(Exception):
                z.release(session=sess)


class TestOriginZid:
    """``origin_zid`` — the HLC's id half, surfaced as attribution.

    Zenoh leaves ``sample.source_info`` unset on an ordinary publish, so
    the timestamp's id half is the only origin signal actually on the
    wire. It is not authentication: ``put(timestamp=...)`` lets a
    publisher claim any zid.
    """

    def test_parses_from_string_form(self):
        assert _parse_origin_zid('7681717563362965824/abc123') == 'abc123'

    def test_none_on_none_and_garbled(self):
        assert _parse_origin_zid(None) is None
        assert _parse_origin_zid('no-separator') is None

    def test_prefers_timestamp_accessor(self):
        class FakeTs:
            def get_id(self):
                return 'from-accessor'

            def __str__(self):
                return 'from-string/other'

        assert _parse_origin_zid(FakeTs()) == 'from-accessor'

    def test_matches_publisher_zid_on_a_real_sample(self, timestamped_pair):
        """The end-to-end claim: the id half is the publishing session."""
        pub, sub = timestamped_pair

        @z.zeared
        class Ping(z.Message):
            TOPIC = 'meta/originzid'
            v: int = z.Int(required=True)

        seen: list = []
        Ping.on_message(lambda m, meta: seen.append(meta), session=sub)
        wait(0.3)
        Ping(v=1).send(session=pub)
        wait(0.5)

        assert seen, 'no sample delivered'
        assert seen[0].origin_zid == str(pub.zid())


class TestIssuedAtOnRealSamples:
    """Regression: ``issued_at`` was ``None`` on every real sample.

    ``_HLC_RE`` required 16 hex digits, but ``zenoh.Timestamp.__str__``
    renders the NTP64 value in **decimal** (19 digits for current dates),
    so the regex never matched and the advertised field silently stayed
    ``None``. The unit test that covered ``_parse_hlc`` built its input
    with ``f'{ntp64:016x}'`` — a form Zenoh does not emit — which is
    exactly why it survived.
    """

    def test_decimal_ntp64_string_parses(self):
        seconds = 1735689600  # 2025-01-01T00:00:00 UTC
        ntp64 = seconds << 32
        result = _parse_hlc(f'{ntp64:d}/abc')
        assert result is not None
        assert abs((result - datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)).total_seconds()) < 1.0

    def test_issued_at_populated_on_a_real_sample(self, timestamped_pair):
        pub, sub = timestamped_pair

        @z.zeared
        class Ping(z.Message):
            TOPIC = 'meta/issuedat'
            v: int = z.Int(required=True)

        seen: list = []
        Ping.on_message(lambda m, meta: seen.append(meta), session=sub)
        wait(0.3)
        before = datetime.datetime.now(datetime.UTC)
        Ping(v=1).send(session=pub)
        wait(0.5)

        assert seen, 'no sample delivered'
        issued = seen[0].issued_at
        assert issued is not None, 'issued_at is None on a real sample'
        assert abs((issued - before).total_seconds()) < 60.0
