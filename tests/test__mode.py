"""Tests for ``zeared/_mode.py`` — the ``Mode`` enum used by
``SessionConfig`` and the ``open()`` dispatcher."""
from __future__ import annotations

from enum import Enum

import zeared as z
from zeared._mode import Mode


class TestModeEnum:
    def test_mode_is_an_enum(self):
        assert issubclass(Mode, Enum)

    def test_two_members(self):
        members = list(Mode)
        assert len(members) == 2

    def test_peer_member(self):
        assert hasattr(Mode, 'PEER')
        assert isinstance(Mode.PEER, Mode)

    def test_client_member(self):
        assert hasattr(Mode, 'CLIENT')
        assert isinstance(Mode.CLIENT, Mode)


class TestPublicReExport:
    def test_mode_re_exported_from_zeared(self):
        assert z.Mode is Mode

    def test_peer_via_z(self):
        assert z.Mode.PEER is Mode.PEER
