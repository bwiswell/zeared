"""``_MessagePresenceMixin`` — ``declare_presence`` / ``adeclare_presence`` (0.4.0).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
Both names are descriptors, like ``unretain`` and ``concrete_topic``, so they
dispatch on instance-vs-class access:

- ``msg.declare_presence(*, session=None, topic=None)`` — the instance's own key;
- ``Cls.declare_presence(*, session=None, topic=None, **slots)`` — the key the
  slot values name. A token carries no payload, so no instance is needed.

``register_will`` / ``aregister_will`` were removed in 0.4.0: reading either
raises ``AttributeError`` with a pointer, so ``hasattr`` reports them gone.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from ..errors import TopicError
from ._message_render import _render_instance, _render_slots

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import zenoh

    from ..presence import PresenceToken
    from .message import Message


def _declare(key: str, session: zenoh.Session | None) -> PresenceToken:
    import zeared as z

    from ..presence import get_presence

    return get_presence(z.session.resolve(session)).declare(key)


def _require_liveliness(cls: type[Message], caller: str) -> None:
    if not getattr(cls, 'LIVELINESS', False):
        msg = f'{cls.__name__}.{caller}: class must set LIVELINESS = True'
        raise TopicError(msg)


def _key_for(instance: Message | None, owner: type[Message], topic: str | None, slots: dict[str, Any]) -> str:
    """The concrete key to hold: the instance's own, or the one ``slots`` name (every slot required)."""
    if instance is not None:
        return _render_instance(instance, topic)[1]
    return _render_slots(owner, slots, topic=topic, caller=f'{owner.__name__}.declare_presence')


class _DeclarePresenceDescriptor:
    """Dispatch ``declare_presence`` on instance-vs-class access.

    Declares a Zenoh liveliness token at the concrete key and returns its
    :class:`~zeared.PresenceToken`. Requires ``LIVELINESS = True``. The key
    renders through the canonical topic render ``send`` uses, so every slot
    must be supplied, and a ``/`` in a single-segment value raises
    ``TopicError``. A key the session already holds returns the same token.
    """

    def __get__(self, instance: Message | None, owner: type[Message]) -> Callable[..., PresenceToken]:
        def declare_presence(
            *,
            session: zenoh.Session | None = None,
            topic: str | None = None,
            **slots: Any,
        ) -> PresenceToken:
            _require_liveliness(owner, 'declare_presence')
            if instance is not None and slots:
                msg = f'{owner.__name__}.declare_presence: slot values come from the instance; got {sorted(slots)}'
                raise TypeError(msg)
            return _declare(_key_for(instance, owner, topic, slots), session)

        declare_presence.__qualname__ = f'{owner.__qualname__}.declare_presence'
        return declare_presence


class _ADeclarePresenceDescriptor:
    """Async counterpart of ``declare_presence``: the declare runs on a worker thread."""

    def __get__(self, instance: Message | None, owner: type[Message]) -> Callable[..., Awaitable[PresenceToken]]:
        sync = _DeclarePresenceDescriptor().__get__(instance, owner)

        async def adeclare_presence(
            *,
            session: zenoh.Session | None = None,
            topic: str | None = None,
            **slots: Any,
        ) -> PresenceToken:
            return await asyncio.to_thread(lambda: sync(session=session, topic=topic, **slots))

        adeclare_presence.__qualname__ = f'{owner.__qualname__}.adeclare_presence'
        return adeclare_presence


class _Removed:
    """A class attribute removed in 0.4.0: reading it raises ``AttributeError`` with a pointer."""

    def __init__(self, name: str, pointer: str) -> None:
        self._name = name
        self._pointer = pointer

    def __get__(self, instance: object, owner: type) -> Any:
        msg = f'{self._name} was removed in 0.4.0: {self._pointer} See "Upgrading to 0.4.0" in the README.'
        raise AttributeError(msg)


_WILL_POINTER = "a liveliness token can't carry a payload. Use declare_presence(), which returns a PresenceToken."


class _MessagePresenceMixin:
    """Presence surface on :class:`Message`."""

    __slots__ = ()

    declare_presence = _DeclarePresenceDescriptor()
    adeclare_presence = _ADeclarePresenceDescriptor()
    register_will = _Removed('register_will', _WILL_POINTER)
    aregister_will = _Removed('aregister_will', _WILL_POINTER)
