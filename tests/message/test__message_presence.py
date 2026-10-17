"""Tests for ``zeared/message/_message_presence.py`` — ``declare_presence`` /
``adeclare_presence`` and the removed ``register_will`` (0.4.0).

End-to-end presence coverage lives in ``tests/presence/test_presence.py``.
This file pins the surface: dual dispatch, the ``LIVELINESS`` guard, slot
rendering, and the pointer left where ``register_will`` was.
"""

from __future__ import annotations

import asyncio

import pytest

import zeared as z
from zeared.message._message_presence import _MessagePresenceMixin


@z.zeared
class Beacon(z.Message):
    TOPIC = 'beacon/{site}/{unit}'
    LIVELINESS = True
    site: str = z.Str(required=True)
    unit: int = z.Int(required=True)
    state: str = z.Str(default='up')


class TestMixinSurface:
    def test_no_instance_state(self):
        assert _MessagePresenceMixin.__slots__ == ()

    def test_names_present(self):
        assert hasattr(z.Message, 'declare_presence')
        assert hasattr(z.Message, 'adeclare_presence')


class TestDualDispatch:
    def test_instance_holds_its_own_key(self, session):
        token = Beacon(site='n', unit=1).declare_presence(session=session)
        assert isinstance(token, z.PresenceToken)
        assert token.key == 'beacon/n/1'

    def test_class_holds_the_key_its_slots_name(self, session):
        assert Beacon.declare_presence(site='n', unit=2, session=session).key == 'beacon/n/2'

    def test_class_form_needs_every_slot(self, session):
        with pytest.raises(z.TopicError, match="missing slot 'unit'"):
            Beacon.declare_presence(site='n', session=session)

    def test_instance_form_refuses_slot_kwargs(self, session):
        with pytest.raises(TypeError, match='slot values come from the instance'):
            Beacon(site='n', unit=1).declare_presence(unit=9, session=session)

    def test_a_split_value_raises(self, session):
        with pytest.raises(z.TopicError, match='contains "/"'):
            Beacon.declare_presence(site='a/b', unit=1, session=session)

    def test_same_key_returns_the_same_token(self, session):
        first = Beacon.declare_presence(site='n', unit=3, session=session)
        assert Beacon(site='n', unit=3).declare_presence(session=session) is first

    def test_async_counterpart(self, session):
        async def main():
            return await Beacon.adeclare_presence(site='n', unit=4, session=session)

        assert asyncio.run(main()).key == 'beacon/n/4'


class TestLivelinessGuard:
    def test_rejects_non_liveliness_class(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'plain/{n}'
            n: int = z.Int(required=True)

        with pytest.raises(z.TopicError, match='LIVELINESS = True'):
            Plain(n=1).declare_presence(session=session)
        with pytest.raises(z.TopicError, match='LIVELINESS = True'):
            Plain.declare_presence(n=1, session=session)


class TestRegisterWillIsGone:
    """0.4.0: reading ``register_will`` raises a pointer, and ``hasattr`` says it's gone."""

    @pytest.mark.parametrize('name', ['register_will', 'aregister_will'])
    def test_pointer(self, name):
        assert not hasattr(Beacon, name)
        assert not hasattr(Beacon(site='n', unit=1), name)
        with pytest.raises(AttributeError, match=f'{name} was removed in 0.4.0') as info:
            getattr(Beacon(site='n', unit=1), name)
        assert 'declare_presence()' in str(info.value)
