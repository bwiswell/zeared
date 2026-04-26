"""``_UnretainDescriptor`` and ``_unretain_impl`` — instance-vs-class
dispatch for ``Message.unretain``.

Sibling helper inside the ``message`` Pattern B subdir. Lives outside
the mixin set because ``unretain`` is implemented as a descriptor on
the class (not a method); it intercepts both ``msg.unretain()`` and
``Cls.unretain(**key_fields)``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import zenoh


class _UnretainDescriptor:
    """Dispatches ``unretain`` instance-vs-class at access:

    - ``msg.unretain(*, session=None, topic=None)`` — drops the cache entry
      for the concrete topic derived from ``self``'s template field values
      and emits a Zenoh DELETE sample on that key.
    - ``Cls.unretain(*, session=None, topic=None, **key_fields)`` — same,
      but the concrete topic is built from explicit kwargs.

    Both forms require ``RETAINED = True`` on the class.
    """

    def __get__(self, instance, owner):
        if instance is None:
            def unretain(
                *,
                session=None,
                topic=None,
                **key_fields,
            ) -> None:
                _unretain_impl(owner, key_fields, session=session, topic=topic)
            unretain.__qualname__ = f'{owner.__qualname__}.unretain'
            return unretain

        def unretain(
            *,
            session=None,
            topic=None,
        ) -> None:
            key_fields = {
                name: getattr(instance, name)
                for name in owner._templates().field_names
            }
            _unretain_impl(owner, key_fields, session=session, topic=topic)
        unretain.__qualname__ = f'{owner.__qualname__}.unretain'
        return unretain


def _unretain_impl(
    cls,
    key_fields: dict,
    *,
    session: Optional['zenoh.Session'],
    topic: Optional[str],
) -> None:
    """Shared implementation for ``msg.unretain()`` and ``Cls.unretain(**)``.

    Buffers into an active batch if one is live on the current context;
    otherwise drops the retention-cache entry and emits a DELETE sample.
    """
    import zeared as z

    from ..batch import current_buffer
    from ..errors import TopicError, ZearedError
    from ..retention import get_retention_cache

    if not getattr(cls, 'RETAINED', False):
        raise TopicError(
            f'{cls.__name__}.unretain: class does not have RETAINED = True'
        )

    sess = z.session.resolve(session)
    template = cls._templates().resolve_publish_topic(topic)
    try:
        concrete_topic = template.render(key_fields)
    except TopicError:
        raise
    except KeyError as e:
        raise TopicError(
            f'{cls.__name__}.unretain: missing key field {e.args[0]!r}'
        ) from e

    buffer = current_buffer()
    if buffer is not None:
        # Encoding unused for tombstones — keep '' to satisfy the tuple shape.
        buffer.append((
            cls, sess, concrete_topic, b'', '', 'tombstone', None,
        ))
        return

    get_retention_cache(cls, sess).delete(concrete_topic)
    try:
        sess.delete(concrete_topic)
    except Exception as e:  # noqa: BLE001
        raise ZearedError(
            f'{cls.__name__}: session.delete failed on {concrete_topic!r}: {e}'
        ) from e
