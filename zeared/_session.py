from __future__ import annotations

import threading
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

from .errors import NoSessionError

if TYPE_CHECKING:
    import zenoh

    from ._managed_session import SessionLike


class _SessionScope(AbstractContextManager):
    """Thread-local scoped override returned by ``_SessionHandle.__call__``."""

    def __init__(self, handle: _SessionHandle, session: zenoh.Session) -> None:
        self._handle = handle
        self._session = session

    def __enter__(self) -> zenoh.Session:
        self._handle._push(self._session)  # noqa: SLF001
        return self._session

    def __exit__(self, *exc: object) -> None:
        self._handle._pop()  # noqa: SLF001


class _SessionHandle:
    """Dual-role ``zeared.session`` attribute.

    - ``zeared.session = sess`` → sets module-level default (intercepted by the
      module's ``__setattr__``).
    - ``zeared.session.current`` → read the current resolved default (without
      consulting the thread-local scope stack).
    - ``with zeared.session(other): ...`` → push *other* onto the thread-local
      stack for the duration of the block; callers that don't pass an explicit
      ``session=`` kwarg will see *other* for the duration.
    """

    def __init__(self) -> None:
        self._default: zenoh.Session | None = None
        self._local = threading.local()

    def _set_default(self, session: zenoh.Session | None) -> None:
        self._default = session

    def _push(self, session: zenoh.Session) -> None:
        stack = getattr(self._local, 'stack', None)
        if stack is None:
            stack = []
            self._local.stack = stack
        stack.append(session)

    def _pop(self) -> None:
        self._local.stack.pop()

    @property
    def current(self) -> zenoh.Session | None:
        stack = getattr(self._local, 'stack', None)
        if stack:
            return stack[-1]
        return self._default

    def resolve(self, explicit: SessionLike | None) -> SessionLike:
        """Session resolution: explicit kwarg → scope stack → module default → raise."""
        if explicit is not None:
            return explicit
        current = self.current
        if current is None:
            msg = (
                'no zeared session available — set zeared.session = <session>, '
                'pass session=..., or enter a `with zeared.session(sess):` block'
            )
            raise NoSessionError(msg)
        return current

    def __call__(self, session: zenoh.Session) -> _SessionScope:
        return _SessionScope(self, session)

    def __repr__(self) -> str:
        return f'<zeared.session handle, default={self._default!r}>'
