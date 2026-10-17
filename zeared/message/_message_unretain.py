"""``_UnretainDescriptor`` and ``_unretain_impl`` — instance-vs-class dispatch for ``Message.unretain``.

Sibling helper inside the ``message`` Pattern B subdir. Lives outside
the mixin set because ``unretain`` is implemented as a descriptor on
the class (not a method); it intercepts both ``msg.unretain()`` and
``Cls.unretain(**key_fields)``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from .._managed_session import SessionLike
    from .message import Message


class _UnretainDescriptor:
    """Dispatch ``unretain`` on instance-vs-class access.

    - ``msg.unretain(*, session=None, topic=None)`` — drops the cache entry
      for the concrete topic derived from ``self``'s template field values
      and emits a Zenoh DELETE sample on that key.
    - ``Cls.unretain(*, session=None, topic=None, **key_fields)`` — same,
      but the concrete topic is built from explicit kwargs.

    Both forms require ``RETAINED = True`` on the class.
    """

    def __get__(self, instance: Message | None, owner: type[Message]) -> Callable[..., None]:
        if instance is None:

            def unretain(
                *,
                session: SessionLike | None = None,
                topic: str | None = None,
                **key_fields: Any,
            ) -> None:
                _unretain_impl(owner, key_fields, session=session, topic=topic)

            unretain.__qualname__ = f'{owner.__qualname__}.unretain'
            return unretain

        def unretain(
            *,
            session: SessionLike | None = None,
            topic: str | None = None,
        ) -> None:
            key_fields = {name: getattr(instance, name) for name in owner._templates().field_names}  # noqa: SLF001
            _unretain_impl(owner, key_fields, session=session, topic=topic)

        unretain.__qualname__ = f'{owner.__qualname__}.unretain'
        return unretain


def _unretain_impl(
    cls: type[Message],
    key_fields: dict,
    *,
    session: SessionLike | None,
    topic: str | None,
) -> None:
    """Shared implementation for ``msg.unretain()`` and ``Cls.unretain(**)``.

    Buffers into an active batch if one is live on the current context;
    otherwise drops the retention-cache entry and emits a DELETE sample.
    """
    import zeared as z

    from ..batch import current_buffer
    from ..errors import TopicError
    from ..publisher import send_error
    from ..retention import get_retention_cache
    from ._message_render import _render_publish_key

    if not getattr(cls, 'RETAINED', False):
        msg = f'{cls.__name__}.unretain: class does not have RETAINED = True'
        raise TopicError(msg)

    sess = z.session.resolve(session)
    template = cls._templates().resolve_publish_topic(topic)
    try:
        # Canonical render: an ``Enum`` / ``DateTime`` slot value renders as
        # its serialized form, so the DELETE lands on the key ``send`` used.
        # A ``/`` in a single-segment value raises (0.4.0): the tombstone
        # would otherwise land on a key no template matches.
        concrete_topic = _render_publish_key(cls, template, key_fields)
    except TopicError:
        raise
    except KeyError as e:
        msg = f'{cls.__name__}.unretain: missing key field {e.args[0]!r}'
        raise TopicError(msg) from e

    buffer = current_buffer()
    if buffer is not None:
        # Encoding is unread on the tombstone path (``_flush`` short-
        # circuits on retain_mode) — carry the class's own so the slot
        # holds a valid ``Encoding`` rather than a sentinel ''.
        buffer.append(
            (
                cls,
                sess,
                concrete_topic,
                b'',
                cls.ENCODING,
                'tombstone',
                None,
            )
        )
        return

    get_retention_cache(cls, sess).delete(concrete_topic)
    try:
        sess.delete(concrete_topic)
    except Exception as e:
        msg = f'{cls.__name__}: session.delete failed on {concrete_topic!r}: {e}'
        raise send_error(sess, msg, e) from e
