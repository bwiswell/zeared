from __future__ import annotations

import pytest

from zeared import _codec as codec


class TestPackUnpack:
    @pytest.mark.parametrize('encoding', ['msgpack', 'json'])
    def test_round_trip_dict(self, encoding):
        data = {'a': 1, 'b': 'hello', 'c': [1.5, 2.5], 'd': {'nested': True}}
        raw = codec.pack(data, encoding)
        assert isinstance(raw, bytes)
        assert codec.unpack(raw, encoding) == data

    def test_msgpack_smaller_than_json_for_typical_payload(self):
        data = {'id': 42, 'x': 1.23456789, 'y': 9.87654321, 'label': 'hi'}
        assert len(codec.pack(data, 'msgpack')) < len(codec.pack(data, 'json'))

    def test_unknown_encoding_raises(self):
        with pytest.raises(ValueError, match='unknown encoding'):
            codec.pack({}, 'xml')  # type: ignore[arg-type]
        with pytest.raises(ValueError, match='unknown encoding'):
            codec.unpack(b'', 'xml')  # type: ignore[arg-type]


class TestEffectiveEncoding:
    def test_debug_forces_json(self):
        assert codec.effective_encoding('msgpack', debug=True) == 'json'
        assert codec.effective_encoding('json', debug=True) == 'json'

    def test_debug_off_honours_class_encoding(self):
        assert codec.effective_encoding('msgpack', debug=False) == 'msgpack'
        assert codec.effective_encoding('json', debug=False) == 'json'


class TestMimeTypes:
    def test_msgpack_mime(self):
        assert codec.MIME['msgpack'] == 'application/msgpack'

    def test_json_mime(self):
        assert codec.MIME['json'] == 'application/json'
