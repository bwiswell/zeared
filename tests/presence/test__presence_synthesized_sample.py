"""Tests for ``zeared/presence/_presence_synthesized_sample.py`` — the
``_SynthesizedSample`` shim used to fire wills locally through the
normal subscriber dispatch path."""
from __future__ import annotations

import zenoh

from zeared.presence._presence_synthesized_sample import _SynthesizedSample


class TestSynthesizedSample:
    def test_construction_fills_attributes(self):
        s = _SynthesizedSample(
            key_expr='peer/alice/status',
            payload=b'offline',
            encoding_mime='application/msgpack',
            source_zid='zid123',
        )
        assert s.key_expr == 'peer/alice/status'
        assert s.payload == b'offline'
        assert s.kind is zenoh.SampleKind.PUT
        assert s.encoding == 'application/msgpack'
        assert s.timestamp is None
        assert s.source_info == 'zid123'
        assert s.attachment is None

    def test_uses_slots(self):
        # Pin: ``__slots__`` is set; attribute writes outside the slot
        # set fail.
        s = _SynthesizedSample('k', b'p', 'mime', 'zid')
        try:
            s.unknown_attr = 'oops'
        except AttributeError:
            pass
        else:
            raise AssertionError(
                '_SynthesizedSample should reject unknown attr writes'
            )
