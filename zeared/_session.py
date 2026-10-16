from __future__ import annotations

from contextlib import AbstractContextManager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

from .errors import NoSessionError

if TYPE_CHECKING:
    import zenoh

    from ._managed_session import SessionLike


# ContextVar-backed scope stack, for the same reason ``batch``'s buffer
# stack is one: ``asyncio.to_thread`` copies the context into its worker, so
# a ``with zeared.session(other):`` scope reaches ``asend`` / ``aquery`` /
# ``afetch_retained`` (which resolve on that worker), and each asyncio task
# gets its own copy, so tasks interleaving on one loop don't see each
# other's scopes. Plain threads are unchanged on standard builds: each new
# thread starts with an empty context. (On a free-threaded build,
# ``sys.flags.thread_inherit_context`` defaults to true and a new thread
# inherits the scope instead.) The stack is an immutable tuple, so a push
# is a ``set`` and a pop is a ``reset`` of its token.
_scope_stack: ContextVar[tuple[zenoh.Session, ...]] = ContextVar(
    'zeared.session_scope',
    default=(),
)


class _SessionScope(AbstractContextManager):
    """Context-local scoped override returned by ``_SessionHandle.__call__``.

    Entering pushes the session onto the current context's scope stack;
    exiting restores the stack as it was. Exiting in a different context
    from the one the scope was entered in raises ``ValueError`` (from
    ``ContextVar.reset``), for example a scope held across a ``yield`` in an
    async generator that another task resumes.
    """

    def __init__(self, session: zenoh.Session) -> None:
        self._session = session
        # One token per active entry, so re-entering the same scope object
        # (``s = zeared.session(x); with s: with s: ...``) unwinds correctly.
        self._tokens: list[Token[tuple[zenoh.Session, ...]]] = []

    def __enter__(self) -> zenoh.Session:
        self._tokens.append(_scope_stack.set((*_scope_stack.get(), self._session)))
        return self._session

    def __exit__(self, *exc: object) -> None:
        # Reset before forgetting the token: if the reset raises (wrong
        # context), the scope can still be exited from the right one.
        _scope_stack.reset(self._tokens[-1])
        self._tokens.pop()


class _SessionHandle:
    """Dual-role ``zeared.session`` attribute.

    - ``zeared.session = sess`` → sets module-level default (intercepted by the
      module's ``__setattr__``).
    - ``zeared.session.current`` → the session a call without ``session=``
      resolves to right now: the innermost scope, else the default.
    - ``with zeared.session(other): ...`` → push *other* onto the
      context-local scope stack for the duration of the block; callers that
      don't pass an explicit ``session=`` kwarg will see *other* for the
      duration. Context-local means per thread *and* per asyncio task, and
      it follows ``asyncio.to_thread`` into the worker, so the async
      wrappers (``asend``, ``aquery``, ...) honour the scope too.
    """

    def __init__(self) -> None:
        self._default: zenoh.Session | None = None

    def _set_default(self, session: zenoh.Session | None) -> None:
        self._default = session

    @property
    def current(self) -> zenoh.Session | None:
        stack = _scope_stack.get()
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
        return _SessionScope(session)

    def __repr__(self) -> str:
        return f'<zeared.session handle, default={self._default!r}>'
