from __future__ import annotations

import threading
from contextlib import AbstractContextManager
from typing import Optional, TYPE_CHECKING

from .errors import NoSessionError

if TYPE_CHECKING:
    import zenoh


class _SessionScope(AbstractContextManager):
    """Thread-local scoped override returned by ``_SessionHandle.__call__``."""

    def __init__(self, handle: '_SessionHandle', session: 'zenoh.Session'):
        self._handle = handle
        self._session = session

    def __enter__(self) -> 'zenoh.Session':
        self._handle._push(self._session)
        return self._session

    def __exit__(self, *exc) -> None:
        self._handle._pop()


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
        self._default: Optional['zenoh.Session'] = None
        self._local = threading.local()

    def _set_default(self, session: Optional['zenoh.Session']) -> None:
        self._default = session

    def _push(self, session: 'zenoh.Session') -> None:
        stack = getattr(self._local, 'stack', None)
        if stack is None:
            stack = []
            self._local.stack = stack
        stack.append(session)

    def _pop(self) -> None:
        self._local.stack.pop()

    @property
    def current(self) -> Optional['zenoh.Session']:
        stack = getattr(self._local, 'stack', None)
        if stack:
            return stack[-1]
        return self._default

    def resolve(self, explicit: Optional['zenoh.Session']) -> 'zenoh.Session':
        """Session resolution: explicit kwarg → scope stack → module default → raise."""
        if explicit is not None:
            return explicit
        current = self.current
        if current is None:
            raise NoSessionError(
                'no zeared session available — set zeared.session = <session>, '
                'pass session=..., or enter a `with zeared.session(sess):` block'
            )
        return current

    def __call__(self, session: 'zenoh.Session') -> _SessionScope:
        return _SessionScope(self, session)

    def __repr__(self) -> str:
        return f'<zeared.session handle, default={self._default!r}>'
