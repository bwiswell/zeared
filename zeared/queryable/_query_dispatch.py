"""Queryable dispatch — builds the per-query handler closure that the
underlying ``zenoh.Queryable`` invokes.

Handles both handler forms:

- **Return form**: ``handler(ctx) -> Message | Iterable[Message] | None``.
  Each returned instance is replied automatically; ``None`` replies
  nothing (the handler may have called ``ctx.reply`` itself).
- **Explicit form**: the handler calls ``ctx.reply`` / ``ctx.reply_err`` /
  ``ctx.reply_del`` directly (for multi-reply, streaming, or error
  cases) and returns ``None``.

Async ``handler`` coroutine functions are scheduled on the loop captured
at ``on_query`` time; the ``QueryContext`` (and thus the ``zenoh.Query``)
is held until the coroutine resolves, then its return value is replied.

Sibling helper inside the ``queryable`` Pattern B subdir — mirrors
``subscriber/_subscriber_dispatch.py``.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Optional

from ..errors import QueryableError
from ._query_context import QueryContext

if TYPE_CHECKING:
    from ..message import Message
    import asyncio

    import zenoh


_log = logging.getLogger('zeared.queryable')


def _reply_result(ctx: QueryContext, result, msg_cls: type,
                  on_error: Optional[Callable]) -> None:
    """Reply with a handler's return value: a single ``Message``, an
    iterable of them, or ``None`` (no-op)."""
    from ..message import Message

    if result is None:
        return
    items = (result,) if isinstance(result, Message) else result
    try:
        iterator = iter(items)
    except TypeError:
        _log.warning(
            '%s: on_query handler returned %r; expected a Message, an '
            'iterable of Messages, or None', msg_cls.__name__, type(result),
        )
        return
    for item in iterator:
        try:
            ctx.reply(item)
        except Exception as exc:  # noqa: BLE001
            wrapped = QueryableError(
                f'{msg_cls.__name__}: query reply failed: {exc}'
            )
            wrapped.__cause__ = exc
            if on_error is not None:
                on_error(wrapped, b'')
            else:
                _log.warning(
                    '%s: query reply failed: %s', msg_cls.__name__, exc,
                )


def _build_query_dispatch(
    msg_cls: 'type[Message]',
    handler: Callable,
    on_error: Optional[Callable[[Exception, bytes], None]],
    *,
    is_async: bool,
    loop: 'Optional[asyncio.AbstractEventLoop]',
) -> Callable[['zenoh.Query'], None]:
    """Build the per-query dispatch closure for a ``Queryable``.

    The returned closure is what the underlying ``zenoh.Queryable`` calls
    for every incoming query. It builds a :class:`QueryContext`, invokes
    ``handler``, and replies with the return value (return form) — errors
    at every step routed through ``on_error`` / ``_log``, never propagated
    to Zenoh.
    """

    def dispatch(query: 'zenoh.Query') -> None:
        try:
            ctx = QueryContext(query, msg_cls)
        except Exception as exc:  # noqa: BLE001
            wrapped = QueryableError(
                f'{msg_cls.__name__}: failed to build query context: {exc}'
            )
            wrapped.__cause__ = exc
            if on_error is not None:
                on_error(wrapped, b'')
            else:
                _log.warning(
                    '%s: failed to build query context: %s',
                    msg_cls.__name__, exc,
                )
            return

        if is_async:
            _dispatch_async(ctx, query)
            return

        try:
            result = handler(ctx)
        except Exception as exc:  # noqa: BLE001
            _handle_handler_error(query, msg_cls, on_error, exc)
            return
        _reply_result(ctx, result, msg_cls, on_error)

    def _dispatch_async(ctx: QueryContext, query: 'zenoh.Query') -> None:
        import asyncio

        if loop is None or loop.is_closed():
            _log.warning(
                '%s: async on_query handler has no running loop; dropping '
                'query', msg_cls.__name__,
            )
            return
        coro = handler(ctx)
        future = asyncio.run_coroutine_threadsafe(coro, loop)

        def _on_done(fut) -> None:
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                _handle_handler_error(query, msg_cls, on_error, exc)
                return
            _reply_result(ctx, result, msg_cls, on_error)

        # Holds ``ctx`` (and the live ``zenoh.Query``) until the coroutine
        # resolves, so a late ``reply`` is still valid.
        future.add_done_callback(_on_done)

    return dispatch


def _handle_handler_error(
    query: 'zenoh.Query', msg_cls: type,
    on_error: Optional[Callable], exc: Exception,
) -> None:
    """Route a handler exception through ``on_error`` / logging and send a
    best-effort error reply so the getter isn't left hanging."""
    wrapped = QueryableError(
        f'{msg_cls.__name__}: on_query handler raised: {exc}'
    )
    wrapped.__cause__ = exc
    if on_error is not None:
        on_error(wrapped, b'')
    else:
        _log.exception('%s: on_query handler raised', msg_cls.__name__)
    try:
        query.reply_err(str(exc).encode('utf-8'))
    except Exception:  # noqa: BLE001
        pass
