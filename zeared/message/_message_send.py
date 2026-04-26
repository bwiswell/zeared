"""``_MessageSendMixin`` — sync publish methods (``send`` + ``send_batch``).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Optional

from .. import _codec as codec

if TYPE_CHECKING:
    import zenoh

    from .message import Message


class _MessageSendMixin:
    """Sync publish surface for :class:`Message`."""
    __slots__ = ()

    def send(
        self,
        *,
        session: Optional['zenoh.Session'] = None,
        topic: Optional[str] = None,
        retain: Optional[bool] = None,
    ) -> None:
        """Serialize and publish this message on the resolved session.

        ``topic=`` optionally picks a non-canonical declared template (one of
        ``TOPIC`` or ``EXTRA_TOPICS``). Arbitrary strings are rejected.

        ``retain=`` overrides the class-level ``RETAINED`` default for this
        call only. ``retain=True`` on a ``RETAINED = False`` class raises
        ``TopicError``. ``retain=False`` on a ``RETAINED = True`` class
        publishes live without touching the retention cache.

        When a ``z.batch()`` block is active on this thread, the serialized
        bytes are buffered and flushed at the outermost ``__exit__`` instead
        of hitting Zenoh immediately.
        """
        import zeared as z  # local import: avoids circular import at module load

        from ..batch import current_buffer
        from ..publisher import get_cache
        from ..retention import effective_retain, get_retention_cache

        sess = z.session.resolve(session)
        encoding = codec.effective_encoding(self.ENCODING, z.debug)
        retain_flag = effective_retain(type(self), retain)

        # Thread the wire encoding into seared's dump as a ``format=``
        # hint. Under ``format='msgpack'`` ``Bytes`` and ``NDArray``
        # fields emit native bytes (no base64 overhead) for the msgpack
        # packer to consume directly. JSON path is unchanged.
        data = type(self).dump(self, format=encoding)
        template = type(self)._templates().resolve_publish_topic(topic)
        concrete_topic = template.render(data)

        # Strip template fields from the payload — they live in the key.
        payload_dict = {
            k: v for k, v in data.items() if k not in template.field_names
        }
        raw = codec.pack(payload_dict, encoding)

        retain_mode = 'retain' if retain_flag else 'none'
        attachment = type(self)._schema_attachment_bytes()

        buffer = current_buffer()
        if buffer is not None:
            buffer.append((
                type(self), sess, concrete_topic, raw, encoding,
                retain_mode, attachment,
            ))
            return

        if retain_flag:
            get_retention_cache(type(self), sess).store(concrete_topic, raw, encoding)
        get_cache(type(self), sess).put(
            concrete_topic, raw, encoding, attachment=attachment,
        )

    @classmethod
    def send_batch(
        cls,
        items: Iterable['Message'],
        *,
        session: Optional['zenoh.Session'] = None,
        topic: Optional[str] = None,
        retain: Optional[bool] = None,
    ) -> None:
        """Publish a homogeneous batch. Wraps the loop in ``z.batch()`` and
        propagates ``session=`` / ``topic=`` / ``retain=`` to every message."""
        from ..batch import batch as _batch_cm

        with _batch_cm():
            for m in items:
                if not isinstance(m, cls):
                    raise TypeError(
                        f'{cls.__name__}.send_batch: expected {cls.__name__} '
                        f'instance, got {type(m).__name__}'
                    )
                m.send(session=session, topic=topic, retain=retain)
