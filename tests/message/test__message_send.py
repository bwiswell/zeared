"""Tests for ``zeared/message/_message_send.py`` — the
``_MessageSendMixin`` (sync ``send`` + ``send_batch``).

Folds in the ``format=`` carrier-hint threading coverage that
previously lived in ``test_format_threading.py`` — format threading
is implemented inside ``send`` (and the matching ``_decode``).
"""
from __future__ import annotations

import json

import msgpack

import zeared as z
from zeared.message._message_send import _MessageSendMixin

from conftest import wait


# ---------------------------------------------------------------------------
# Smoke: mixin surface.
# ---------------------------------------------------------------------------


class TestMixinSurface:
    def test_class_importable(self):
        assert _MessageSendMixin is not None

    def test_no_instance_state(self):
        assert _MessageSendMixin.__slots__ == ()

    def test_methods_present(self):
        assert hasattr(_MessageSendMixin, 'send')
        assert hasattr(_MessageSendMixin, 'send_batch')


# ---------------------------------------------------------------------------
# format= threading (folded from test_format_threading.py).
# ---------------------------------------------------------------------------


@z.zeared
class _MsgpackBlob(z.Message):
    TOPIC = 'fmt/mp/{n}'
    ENCODING = 'msgpack'
    n: int = z.Int(required=True)
    payload: bytes = z.Bytes(required=True)


@z.zeared
class _JsonBlob(z.Message):
    TOPIC = 'fmt/json/{n}'
    ENCODING = 'json'
    n: int = z.Int(required=True)
    payload: bytes = z.Bytes(required=True)


class TestMsgpackNativeBytes:
    """Pin: ``ENCODING='msgpack'`` produces native bytes on the wire."""

    def test_payload_msgpack_decodes_to_native_bytes(self, session):
        captured: list[bytes] = []

        def capture(sample):
            captured.append(bytes(sample.payload))

        sub = session.declare_subscriber('fmt/mp/**', capture)
        try:
            z.session = session
            _MsgpackBlob(n=1, payload=b'\x00\x01\x02hello').send()
            wait(0.2)
        finally:
            sub.undeclare()

        assert len(captured) == 1
        decoded = msgpack.unpackb(captured[0], raw=False)
        assert isinstance(decoded, dict)
        assert isinstance(decoded['payload'], bytes), (
            f'expected native bytes under format=msgpack, '
            f'got {type(decoded["payload"]).__name__} (would mean '
            f'format= threading regressed)'
        )
        assert decoded['payload'] == b'\x00\x01\x02hello'


class TestJsonBase64Path:
    """Pin: ``ENCODING='json'`` preserves the base64-string wire form."""

    def test_payload_json_decodes_to_base64_string(self, session):
        captured: list[bytes] = []

        def capture(sample):
            captured.append(bytes(sample.payload))

        sub = session.declare_subscriber('fmt/json/**', capture)
        try:
            z.session = session
            _JsonBlob(n=1, payload=b'hello').send()
            wait(0.2)
        finally:
            sub.undeclare()

        assert len(captured) == 1
        decoded = json.loads(captured[0])
        assert isinstance(decoded['payload'], str)
        assert decoded['payload'] == 'aGVsbG8='


class TestRoundTripUnaffected:
    """Pin: subscriber-side decode threads the same format hint."""

    def test_msgpack_round_trip(self, session):
        received: list[bytes] = []
        z.session = session
        sub = _MsgpackBlob.on_message(lambda m: received.append(m.payload))
        wait()
        original = bytes(range(256))
        _MsgpackBlob(n=1, payload=original).send()
        wait()
        sub.close()
        assert received == [original]

    def test_json_round_trip(self, session):
        received: list[bytes] = []
        z.session = session
        sub = _JsonBlob.on_message(lambda m: received.append(m.payload))
        wait()
        _JsonBlob(n=1, payload=b'\xff\xfe\xfd').send()
        wait()
        sub.close()
        assert received == [b'\xff\xfe\xfd']
