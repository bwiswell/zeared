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


def _peer_session(*, default_timeout_ms: int | None = None):
    """A peer session; ``default_timeout_ms`` lowers Zenoh's default query timeout (10 s)."""
    import zenoh

    c = zenoh.Config()
    c.insert_json5('mode', '"peer"')
    c.insert_json5('scouting/multicast/enabled', 'false')
    if default_timeout_ms is not None:
        c.insert_json5('queries_default_timeout', str(default_timeout_ms))
    return zenoh.open(c)


@z.zeared
class _Bay(z.Message):
    TOPIC = 'fr/err/{bay}'
    RETAINED = True
    bay: str = z.Str(required=True)
    level: int = z.Int(default=0)


class TestErrorReplies:
    """``on_error`` hears error replies, as the docstring always said (0.4.4); the set comes back as far as it got."""

    def test_a_servers_error_reply(self, session):
        _Bay(bay='a', level=1).send(session=session)
        wait(0.2)
        refusing = session.declare_queryable('fr/err/**', lambda query: query.reply_err(b'nope'))
        wait(0.2)
        heard: list[tuple[Exception, bytes]] = []
        got = _Bay.fetch_retained(session=session, on_error=lambda exc, raw: heard.append((exc, raw)))
        refusing.undeclare()
        assert [(m.bay, m.level) for m in got] == [('a', 1)]
        assert len(heard) == 1
        exc, raw = heard[0]
        assert isinstance(exc, z.RetainedFetchError)
        assert exc.__cause__ is None
        assert raw == b'nope'
        assert str(exc) == "_Bay: error reply to retained fetch 'fr/err/*': 'nope'"

    def test_a_queryable_that_doesnt_answer_in_time(self):
        # Zenoh's own error reply, payload b'Timeout'. Before 0.4.4 it was dropped,
        # and this partial set looked complete.
        session = _peer_session(default_timeout_ms=300)
        try:
            _Bay(bay='b', level=2).send(session=session)
            wait(0.2)
            silent = session.declare_queryable('fr/err/**')  # a channel nobody drains
            wait(0.2)
            heard: list[bytes] = []
            got = _Bay.fetch_retained(session=session, on_error=lambda exc, raw: heard.append(raw))
            silent.undeclare()
        finally:
            session.close()
        assert [(m.bay, m.level) for m in got] == [('b', 2)]
        assert heard == [b'Timeout']

    def test_afetch_retained_routes_them_too(self, session):
        _Bay(bay='c').send(session=session)
        wait(0.2)
        refusing = session.declare_queryable('fr/err/**', lambda query: query.reply_err(b'nope'))
        wait(0.2)
        heard: list[bytes] = []

        async def main():
            return await z.afetch_retained(_Bay, session=session, on_error=lambda exc, raw: heard.append(raw))

        got = asyncio.run(main())
        refusing.undeclare()
        assert [m.bay for m in got] == ['c']
        assert heard == [b'nope']
