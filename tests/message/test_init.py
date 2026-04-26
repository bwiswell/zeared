"""Smoke tests for ``zeared/message/__init__.py`` — the namespace
re-exports for the message Pattern B subdir."""
from __future__ import annotations

import zeared as z
from zeared.message import Encoding, Message


class TestReExports:
    def test_message_class(self):
        assert Message is not None
        assert isinstance(Message, type)

    def test_encoding_type_alias(self):
        # ``Encoding = Literal['msgpack', 'json']`` — type alias exists.
        assert Encoding is not None

    def test_message_re_exported_at_package_root(self):
        assert z.Message is Message
