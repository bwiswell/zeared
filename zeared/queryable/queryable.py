"""Zeared queryable handle — the ``Queryable`` class.

Primary file of the ``queryable`` Pattern B subdir. Wraps N underlying
``zenoh.Queryable`` instances (one per declared template on the message
class) whose handler answers peer ``session.get()`` requests with
computed message instances. The compute-serving sibling of the
cache-serving retention queryable, and the request/response sibling of
``Subscriber``.

Per-query dispatch lives in ``_query_dispatch.py``; the ``QueryContext``
handed to handlers lives in ``_query_context.py``; the module-level
registry used by ``z.release(session=)`` and the reconnect machinery
lives in ``_queryable_registry.py``.
"""
from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Callable, Generic, Optional, Type, TypeVar

from .._managed_session import resolve_raw
from ..errors import QueryableError, TopicError
from ._query_dispatch import _build_query_dispatch
from ._queryable_registry import _deregister_queryable, _register_queryable

if TYPE_CHECKING:
    import zenoh

    from ..message import Message


# Type parameter for generic-parameterised ``Queryable[Cls]``. Bound to
# ``Message`` so callers can't parameterise with a non-message type.
M = TypeVar('M', bound='Message')


class Queryable(Generic[M]):
    """Zeared queryable handle.

    Wraps N underlying ``zenoh.Queryable`` instances — one per declared
    topic template on the message class. Each incoming query is handed to
    the user handler as a :class:`QueryContext`; the handler returns
    message instance(s) to reply with (or replies explicitly via the
    context).

    Close via ``.close()`` or as a context manager. Close is idempotent.

    Generic in the message class for IDE ergonomics —
    ``Cls.on_query(handler)`` returns ``Queryable[Cls]``. The type
    parameter is type-only; runtime behaviour is identical with or without
    it.
    """

    __slots__ = (
        '_zenoh_queryables', '_session', '_closed',
        # Redeclaration state — populated by `_declare`, used on reconnect.
        '_msg_cls', '_handler', '_on_error', '_auto_reconnect',
        '_is_async', '_loop',
    )

    def __init__(self, zenoh_queryables: tuple, session=None):
        self._zenoh_queryables = zenoh_queryables
        self._session = session
        self._closed = False
        self._msg_cls = None
        self._handler = None
        self._on_error = None
        self._auto_reconnect = True
        self._is_async = False
        self._loop = None

    @classmethod
    def _declare(
        cls,
        msg_cls: Type['Message'],
        session: 'zenoh.Session',
        handler: Callable,
        on_error: Optional[Callable[[Exception, bytes], None]],
        auto_reconnect: bool = True,
    ) -> 'Queryable':
        # RETAINED classes already own a cache-serving queryable over the
        # same template wildcard; a second compute-serving queryable would
        # answer the same get with competing replies. One or the other.
        if getattr(msg_cls, 'RETAINED', False):
            raise TopicError(
                f'{msg_cls.__name__}: on_query is not allowed on a RETAINED '
                f'class — retention already serves a queryable over the same '
                f'topic. Use one or the other.'
            )

        tpls = msg_cls._templates()

        # Async handler: capture the loop running at on_query time (mirrors
        # the on_message async-callback contract). Fail loud if none.
        is_async = inspect.iscoroutinefunction(handler)
        loop = None
        if is_async:
            import asyncio
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError as e:
                raise QueryableError(
                    'async handler passed to on_query, but no running event '
                    'loop at declare time; call from within an async context '
                    'or use a sync handler'
                ) from e

        dispatch = _build_query_dispatch(
            msg_cls, handler, on_error, is_async=is_async, loop=loop,
        )

        # Internal declaration — route through the raw to avoid the
        # user-facing declare_* RuntimeWarning; zeared rebuilds these
        # handles itself across reconnects via ``_redeclare``.
        raw = resolve_raw(session)
        zenoh_queryables: list = []
        try:
            for tpl in tpls.all:
                zenoh_queryables.append(
                    raw.declare_queryable(tpl.wildcard, dispatch),
                )
        except Exception as e:  # noqa: BLE001
            for q in zenoh_queryables:
                try:
                    q.undeclare()
                except Exception:  # noqa: BLE001
                    pass
            raise QueryableError(
                f'{msg_cls.__name__}: failed to declare queryable: {e}'
            ) from e

        handle = cls(tuple(zenoh_queryables), session=session)
        handle._msg_cls = msg_cls
        handle._handler = handler
        handle._on_error = on_error
        handle._auto_reconnect = auto_reconnect
        handle._is_async = is_async
        handle._loop = loop
        _register_queryable(session, handle)
        return handle

    def _redeclare(self, new_raw_session, managed_session) -> None:
        """Rebuild the underlying ``zenoh.Queryable`` set against
        ``new_raw_session``. Called by the reconnect machinery.

        Queryables hold no replayed state — just the handler closure — so
        this simply re-declares each template wildcard against the fresh
        raw. The dispatch closure is rebuilt from the retained handler so
        it captures the (possibly still-current) loop for async handlers.
        """
        if self._closed or self._msg_cls is None:
            return
        msg_cls = self._msg_cls
        tpls = msg_cls._templates()
        dispatch = _build_query_dispatch(
            msg_cls, self._handler, self._on_error,
            is_async=self._is_async, loop=self._loop,
        )
        new_queryables: list = []
        try:
            for tpl in tpls.all:
                new_queryables.append(
                    new_raw_session.declare_queryable(tpl.wildcard, dispatch),
                )
        except Exception as e:  # noqa: BLE001
            for q in new_queryables:
                try:
                    q.undeclare()
                except Exception:  # noqa: BLE001
                    pass
            raise QueryableError(
                f'{msg_cls.__name__}: redeclare after reconnect failed: {e}'
            ) from e
        self._zenoh_queryables = tuple(new_queryables)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for q in self._zenoh_queryables:
            try:
                q.undeclare()
            except Exception:  # noqa: BLE001
                pass
        _deregister_queryable(self._session, self)

    def __enter__(self) -> 'Queryable':
        return self

    def __exit__(self, *exc) -> None:
        self.close()


__all__ = ['Queryable']
