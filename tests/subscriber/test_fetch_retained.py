"""Tests for ``Message.fetch_retained`` — the one-shot typed snapshot of a
class's current retained set (the durable reconcile path)."""

from __future__ import annotations

import asyncio

import pytest
from conftest import wait

import zeared as z


class TestFetchRetained:
    def test_returns_current_retained_set(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Unit(z.Message):
            TOPIC = 'fr/set/{unit_id}'
            RETAINED = True
            unit_id: int = z.Int(required=True)
            zone: str = z.Str(required=True)

        Unit(unit_id=1, zone='dock').send(session=session_a)
        Unit(unit_id=2, zone='bay').send(session=session_a)
        wait(0.2)

        # A peer with no prior subscription fetches the whole set on demand.
        got = Unit.fetch_retained(session=session_b)
        by_id = {m.unit_id: m.zone for m in got}
        assert by_id == {1: 'dock', 2: 'bay'}

    def test_reflects_unretain_removal(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Unit(z.Message):
            TOPIC = 'fr/removed/{unit_id}'
            RETAINED = True
            unit_id: int = z.Int(required=True)
            zone: str = z.Str(required=True)

        Unit(unit_id=1, zone='dock').send(session=session_a)
        Unit(unit_id=2, zone='bay').send(session=session_a)
        wait(0.2)
        Unit.unretain(unit_id=1, session=session_a)
        wait(0.2)

        got = Unit.fetch_retained(session=session_b)
        assert {m.unit_id for m in got} == {2}

    def test_empty_when_nothing_retained(self, connected_pair):
        _session_a, session_b = connected_pair

        @z.zeared
        class Unit(z.Message):
            TOPIC = 'fr/empty/{unit_id}'
            RETAINED = True
            unit_id: int = z.Int(required=True)
            zone: str = z.Str(required=True)

        assert Unit.fetch_retained(session=session_b) == []

    def test_requires_retained(self, session):
        @z.zeared
        class Live(z.Message):
            TOPIC = 'fr/live/{id}'
            id: int = z.Int(required=True)

        with pytest.raises(z.TopicError, match='requires RETAINED = True'):
            Live.fetch_retained(session=session)

    def test_uses_module_default_session(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Unit(z.Message):
            TOPIC = 'fr/default/{unit_id}'
            RETAINED = True
            unit_id: int = z.Int(required=True)
            zone: str = z.Str(required=True)

        Unit(unit_id=5, zone='dock').send(session=session_a)
        wait(0.2)

        z.session = session_b
        got = Unit.fetch_retained()
        assert {m.unit_id for m in got} == {5}


def test_afetch_retained(connected_pair):
    session_a, session_b = connected_pair

    @z.zeared
    class Unit(z.Message):
        TOPIC = 'fr/async/{unit_id}'
        RETAINED = True
        unit_id: int = z.Int(required=True)
        zone: str = z.Str(required=True)

    Unit(unit_id=9, zone='dock').send(session=session_a)
    wait(0.2)

    async def main():
        return await z.afetch_retained(Unit, session=session_b)

    got = asyncio.run(main())
    assert {m.unit_id for m in got} == {9}
