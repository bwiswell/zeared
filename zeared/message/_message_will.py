"""``_MessageWillMixin`` — Last-Will-and-Testament registration
(``register_will`` + ``aregister_will``).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from .. import _codec as codec
from ..errors import TopicError

if TYPE_CHECKING:
    import zenoh


class _MessageWillMixin:
    """LWT registration surface on :class:`Message`."""
    __slots__ = ()

    def register_will(
        self,
        *,
        session: Optional['zenoh.Session'] = None,
        topic: Optional[str] = None,
    ) -> None:
        """Register this message as the LWT for its target topic.

        Requires ``LIVELINESS = True`` on the class. On the resolved
        session's liveliness-token disappearance (graceful close OR crash),
        any subscriber on a ``LIVELINESS = True`` class whose templates
        match the target will receive this payload as a synthesised
        sample through the normal decode path.

        Re-registering overwrites the previous will for the same
        (class, concrete topic) — slug is deterministic.
        """
        import zeared as z

        from ..presence import _WillEnvelope, get_presence

        if not type(self).LIVELINESS:
            raise TopicError(
                f'{type(self).__name__}.register_will: class must set '
                f'LIVELINESS = True'
            )

        sess = z.session.resolve(session)
        encoding = codec.effective_encoding(self.ENCODING, z.debug)
        # Thread ``format=`` into seared's dump so binary fields use
        # native bytes under msgpack — same logic as ``send``.
        data = type(self).dump(self, format=encoding)
        template = type(self)._templates().resolve_publish_topic(topic)
        concrete_topic = template.render(data)
        payload_dict = {
            k: v for k, v in data.items() if k not in template.field_names
        }
        raw = codec.pack(payload_dict, encoding)

        envelope = _WillEnvelope(
            source_zid=str(sess.zid()),
            target_key_expr=concrete_topic,
            encoding=encoding,
            payload=raw,
        )
        get_presence(sess).register_will(type(self).__qualname__, envelope)

    async def aregister_will(
        self,
        *,
        session: Optional['zenoh.Session'] = None,
        topic: Optional[str] = None,
    ) -> None:
        """Async counterpart of :meth:`register_will`."""
        import asyncio
        await asyncio.to_thread(
            self.register_will, session=session, topic=topic,
        )
