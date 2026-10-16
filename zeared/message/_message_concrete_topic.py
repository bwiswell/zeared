"""``_ConcreteTopicDescriptor`` — instance-vs-class dispatch for ``Message.concrete_topic``.

Sibling helper inside the ``message`` Pattern B subdir, shaped like
``_message_unretain.py``: ``concrete_topic`` is a descriptor on the class
(not a method), so it intercepts both ``msg.concrete_topic()`` and
``Cls.concrete_topic(**slots)``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ._message_render import _render_instance, _render_slots

if TYPE_CHECKING:
    from collections.abc import Callable

    from .message import Message


class _ConcreteTopicDescriptor:
    """Dispatch ``concrete_topic`` on instance-vs-class access.

    - ``msg.concrete_topic(*, topic=None)`` — the concrete key
      ``msg.send(topic=topic)`` publishes on: the canonical template (or a
      declared ``topic=``), rendered from the instance's slot fields.
    - ``Cls.concrete_topic(*, topic=None, **slots)`` — the concrete key those
      slot values name, without building an instance. It's for operations
      whose input *is* an identity, such as naming a row to delete. Every
      slot must be supplied; a missing or ``None`` slot raises
      ``TopicError`` rather than widening to a wildcard. Keyword arguments
      that aren't slots of the template are ignored, so a full field dict
      can be passed as-is.

    Both render through the canonical topic render shared with ``send``,
    ``unretain`` and the query getters, so the key always matches what
    those paths use. A ``TopicError`` is raised for an undeclared or
    subscribe-only ``topic=``.
    """

    def __get__(self, instance: Message | None, owner: type[Message]) -> Callable[..., str]:
        if instance is None:

            def concrete_topic(*, topic: str | None = None, **slots: Any) -> str:
                return _render_slots(
                    owner,
                    slots,
                    topic=topic,
                    caller=f'{owner.__name__}.concrete_topic',
                )

            concrete_topic.__qualname__ = f'{owner.__qualname__}.concrete_topic'
            return concrete_topic

        def concrete_topic(*, topic: str | None = None) -> str:
            return _render_instance(instance, topic)[1]

        concrete_topic.__qualname__ = f'{owner.__qualname__}.concrete_topic'
        return concrete_topic
