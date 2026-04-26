from __future__ import annotations

from contextlib import AbstractContextManager
from contextvars import ContextVar
from typing import TYPE_CHECKING, List, Optional, Tuple

from . import publisher as _pub
from . import retention as _ret

if TYPE_CHECKING:
    import zenoh


# A pending send:
#   (Message subclass, session, concrete_topic, raw_bytes, encoding,
#    retain_mode, attachment_bytes)
#
# ``retain_mode`` is one of:
#   'none'      — plain publish; put via _PublisherCache
#   'retain'    — publish AND store in _RetentionCache
#   'tombstone' — emit DELETE sample AND drop cache entry; raw_bytes unused
#
# ``attachment_bytes`` is the per-class schema-attachment payload (or
# ``None`` for classes without ``SCHEMA``, and always ``None`` for
# tombstones — DELETE samples don't carry attachments).
BufferedSend = Tuple[
    type, 'zenoh.Session', str, bytes, str, str, Optional[bytes],
]


# ContextVar-backed stack of batch buffers. Using a context variable (rather
# than threading.local) so that asyncio tasks get per-task isolation naturally
# — each task inherits a copy of the context on creation, and mutations don't
# leak back to the parent. In plain-sync threads the behaviour is unchanged:
# each new thread starts with an empty stack.
_buffer_stack: ContextVar[Optional[List[List[BufferedSend]]]] = ContextVar(
    'zeared.batch_stack',
    default=None,
)


def _get_stack() -> List[List[BufferedSend]]:
    """Return the current context's stack, treating ``None`` as empty.

    NEVER mutates the ContextVar — that's reserved for the
    ``_BatchContext`` enter / exit which uses ``set(...) + reset(token)``
    to scope mutations correctly across thread / task boundaries.
    Mutating a shared list across contexts (the pre-0.1.0 bug) leaks
    appends across asyncio tasks; flat-nested ``async with z.abatch():``
    blocks would visibly cross-contaminate.
    """
    stack = _buffer_stack.get()
    return stack if stack is not None else []


def current_buffer() -> Optional[List[BufferedSend]]:
    """Return the innermost active batch buffer, or ``None``.

    Flat-nesting semantics: there is at most one live buffer per context
    (thread *or* asyncio task). ``Message.send`` appends here when non-None.
    """
    stack = _buffer_stack.get()
    if stack is None or not stack:
        return None
    return stack[-1]


def _flush(buffer: List[BufferedSend]) -> None:
    """Drain buffer → publisher / retention caches, dispatched by retain_mode."""
    for cls, sess, topic, raw, encoding, retain_mode, attachment in buffer:
        if retain_mode == 'tombstone':
            _ret.get_retention_cache(cls, sess).delete(topic)
            try:
                sess.delete(topic)
            except Exception as e:  # noqa: BLE001
                from .errors import ZearedError
                raise ZearedError(
                    f'{cls.__name__}: session.delete failed on {topic!r}: {e}'
                ) from e
            continue
        if retain_mode == 'retain':
            _ret.get_retention_cache(cls, sess).store(topic, raw, encoding)
        _pub.get_cache(cls, sess).put(
            topic, raw, encoding, attachment=attachment,
        )
    buffer.clear()


class _BatchHandle:
    """Returned from ``with z.batch() as b:`` — exposes ``flush()``."""

    __slots__ = ('_buffer',)

    def __init__(self, buffer: List[BufferedSend]):
        self._buffer = buffer

    def flush(self) -> None:
        """Drain pending sends to Zenoh now. Leaves the block still active."""
        _flush(self._buffer)


class _BatchContext(AbstractContextManager):
    """Flat-nested batch context manager.

    The outermost ``with z.batch():`` owns the buffer; inner blocks are
    no-ops that return a handle over the same buffer. An exception escaping
    the outer block discards the buffer without flushing (users can call
    ``b.flush()`` mid-block to drain explicitly).
    """

    __slots__ = ('_owns', '_buffer', '_token')

    def __init__(self) -> None:
        self._owns = False
        self._buffer: Optional[List[BufferedSend]] = None
        self._token = None

    def __enter__(self) -> _BatchHandle:
        stack = _get_stack()
        if not stack:
            self._owns = True
            self._buffer = []
            # Build a NEW list (not a mutation of the inherited one) so
            # asyncio tasks that inherited a snapshot of the parent's
            # context don't share this stack via reference. ``reset``
            # restores the prior value on exit.
            self._token = _buffer_stack.set([self._buffer])
        else:
            self._buffer = stack[-1]
        return _BatchHandle(self._buffer)

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self._owns:
            return
        # Restore the pre-enter ContextVar value (typically ``None`` /
        # the default), atomically dropping our owned stack from this
        # context. Crucially, this does NOT mutate any list shared with
        # parent or sibling contexts.
        if self._token is not None:
            _buffer_stack.reset(self._token)
        if exc_type is not None:
            return  # exception → discard
        _flush(self._buffer)


def batch() -> _BatchContext:
    """Collect ``send()`` calls into a single flush.

    Usage::

        with z.batch() as b:
            a.send()
            b.send()
            # b.flush()   # optional: drain now, continue collecting

    Flat nesting: an inner ``with z.batch():`` shares the outer buffer and
    flushes only when the outer block exits. An exception escaping the
    outermost block discards pending sends. The buffer is scoped to the
    current context (thread or asyncio task) via ``contextvars``.
    """
    return _BatchContext()
