"""``_MessageSendMixin`` — sync publish methods (``send`` + ``send_batch``).

Mixin — contributes no instance state. ``Message`` composes this via MRO.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .. import _codec as codec
from ._message_render import _render_instance, _slot_wire_keys

if TYPE_CHECKING:
    from collections.abc import Iterable

    import zenoh

    from .message import Message


def _new_timestamp(session: object) -> zenoh.Timestamp | None:
    """A fresh timestamp from ``session``'s clock, or ``None`` to let Zenoh stamp the put."""
    from .._managed_session import resolve_raw

    try:
        return resolve_raw(session).new_timestamp()  # ty: ignore[invalid-argument-type]
    except Exception:  # noqa: BLE001
        return None


class _MessageSendMixin:
    """Sync publish surface for :class:`Message`."""

    __slots__ = ()

    def send(
        self: Message,
        *,
        session: zenoh.Session | None = None,
        topic: str | None = None,
        retain: bool | None = None,
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
        # The key renders from the instance's own slot values, through the
        # canonical render shared with unretain / query / concrete_topic —
        # not from ``data``, whose keys are wire keys and whose values are
        # carrier-specific (native bytes under msgpack).
        template, concrete_topic = _render_instance(self, topic)

        # Strip template fields from the payload — they live in the key.
        slot_keys = _slot_wire_keys(type(self), template)
        payload_dict = {k: v for k, v in data.items() if k not in slot_keys}
        raw = codec.pack(payload_dict, encoding)

        retain_mode = 'retain' if retain_flag else 'none'
        attachment = type(self)._schema_attachment_bytes()  # noqa: SLF001

        buffer = current_buffer()
        if buffer is not None:
            buffer.append(
                (
                    type(self),
                    sess,
                    concrete_topic,
                    raw,
                    encoding,
                    retain_mode,
                    attachment,
                )
            )
            return

        # A retained send stamps its put explicitly and keeps the timestamp
        # with the cached value, for the republish on link restore (0.4.0).
        timestamp = _new_timestamp(sess) if retain_flag else None
        if retain_flag:
            get_retention_cache(type(self), sess).store(
                concrete_topic,
                raw,
                encoding,
                attachment=attachment,
                timestamp=timestamp,
            )
        get_cache(type(self), sess).put(
            concrete_topic,
            raw,
            encoding,
            attachment=attachment,
            timestamp=timestamp,
        )

    @classmethod
    def send_batch(
        cls,
        items: Iterable[Message],
        *,
        session: zenoh.Session | None = None,
        topic: str | None = None,
        retain: bool | None = None,
    ) -> None:
        """Publish a homogeneous batch.

        Wraps the loop in ``z.batch()`` and propagates ``session=`` / ``topic=`` /
        ``retain=`` to every message.
        """
        from ..batch import batch as _batch_cm

        with _batch_cm():
            for m in items:
                if not isinstance(m, cls):
                    msg = f'{cls.__name__}.send_batch: expected {cls.__name__} instance, got {type(m).__name__}'
                    raise TypeError(msg)
                m.send(session=session, topic=topic, retain=retain)
