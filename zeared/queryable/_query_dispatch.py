"""Queryable dispatch — builds the per-query handler closure that the underlying ``zenoh.Queryable`` invokes.

Handles three handler forms:

- **Return form**: ``handler(ctx) -> Message | Iterable[Message] | None``.
  Each returned instance is replied automatically; ``None`` replies
  nothing (the handler may have called ``ctx.reply`` itself). A
  *generator* handler is the streaming shape of this form — items are
  replied as they are yielded, so the handler never materialises the full
  result set.
- **Explicit form**: the handler calls ``ctx.reply`` / ``ctx.reply_err`` /
  ``ctx.reply_del`` directly (for multi-reply, streaming, or error
  cases) and returns ``None``.
- **Async forms**: an ``async def`` handler is scheduled on the loop
  captured at ``on_query`` time; the ``QueryContext`` (and thus the
  ``zenoh.Query``) is held until the coroutine resolves, then its return
  value is replied. An ``async def`` **generator** is the async sibling of
  the sync generator: it is drained with ``async for`` and each yielded
  instance is replied as it arrives.

Errors never propagate into Zenoh's callback. A handler that raises —
including a generator that raises part-way through a stream, after some
replies have already gone out — routes through ``on_error`` / logging and
sends a best-effort ``reply_err`` so the getter isn't left believing it
received a complete answer.

Sibling helper inside the ``queryable`` Pattern B subdir — mirrors
``subscriber/_subscriber_dispatch.py``.
"""

from __future__ import annotations

import contextlib
import inspect
import logging
from typing import TYPE_CHECKING, Any

from ..errors import QueryableError
from ._query_context import QueryContext

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable
    from concurrent.futures import Future

    import zenoh

    from ..message import Message


_log = logging.getLogger('zeared.queryable')


def _reply_one(ctx: QueryContext, item: Any, msg_cls: type[Message], on_error: Callable | None) -> None:
    """Reply with one instance, routing a reply failure without aborting the stream.

    Shared by the sync and async streaming paths so both classify a failed
    ``ctx.reply`` the same way: as a *reply* failure (this item is lost,
    the rest of the stream continues), distinct from the *handler* failure
    raised by the generator itself.
    """
    try:
        ctx.reply(item)
    except Exception as exc:  # noqa: BLE001
        wrapped = QueryableError(f'{msg_cls.__name__}: query reply failed: {exc}')
        wrapped.__cause__ = exc
        if on_error is not None:
            on_error(wrapped, b'')
        else:
            _log.warning(
                '%s: query reply failed: %s',
                msg_cls.__name__,
                exc,
            )


def _reply_result(
    query: zenoh.Query,
    ctx: QueryContext,
    result: Any,
    msg_cls: type[Message],
    on_error: Callable | None,
) -> None:
    """Reply with a handler's return value: a single ``Message``, an iterable of them, or ``None`` (no-op)."""
    from ..message import Message

    if result is None:
        return
    items = (result,) if isinstance(result, Message) else result
    try:
        iterator = iter(items)
    except TypeError:
        _log.warning(
            '%s: on_query handler returned %r; expected a Message, an iterable of Messages, or None',
            msg_cls.__name__,
            type(result),
        )
        return
    # The outer guard catches a *generator* handler raising as the iterator
    # is advanced — which happens here, after ``handler(ctx)`` already
    # returned cleanly, so ``dispatch``'s own try/except is long past. Left
    # unguarded it escapes into Zenoh's callback: on_error never fires and
    # the getter silently receives a truncated stream as if it were whole.
    try:
        for item in iterator:
            _reply_one(ctx, item, msg_cls, on_error)
    except Exception as exc:  # noqa: BLE001
        _handle_handler_error(query, msg_cls, on_error, exc)


async def _drain_async_gen(
    ctx: QueryContext,
    agen: Any,
    msg_cls: type[Message],
    on_error: Callable | None,
) -> None:
    """Drain an ``async def`` generator handler, replying each item as it is yielded.

    The async sibling of the generator branch in ``_reply_result``:
    O(1) in the handler, replies stream out as they are produced. Returns
    ``None`` so the ``_on_done`` callback's ``_reply_result`` is a no-op —
    every reply has already gone out by then.

    A raise from the generator propagates out of this coroutine into the
    future, where ``_on_done`` routes it as a handler error.
    """
    async for item in agen:
        _reply_one(ctx, item, msg_cls, on_error)


def _build_query_dispatch(
    msg_cls: type[Message],
    handler: Callable,
    on_error: Callable[[Exception, bytes], None] | None,
    *,
    is_async: bool,
    loop: asyncio.AbstractEventLoop | None,
) -> Callable[[zenoh.Query], None]:
    """Build the per-query dispatch closure for a ``Queryable``.

    The returned closure is what the underlying ``zenoh.Queryable`` calls
    for every incoming query. It builds a :class:`QueryContext`, invokes
    ``handler``, and replies with the return value (return form) — errors
    at every step routed through ``on_error`` / ``_log``, never propagated
    to Zenoh.

    ``is_async`` covers both awaitable forms (they share the captured-loop
    requirement); which one this handler is gets resolved once here rather
    than per query.
    """
    is_async_gen = inspect.isasyncgenfunction(handler)

    def dispatch(query: zenoh.Query) -> None:
        try:
            ctx = QueryContext(query, msg_cls)
        except Exception as exc:  # noqa: BLE001
            wrapped = QueryableError(f'{msg_cls.__name__}: failed to build query context: {exc}')
            wrapped.__cause__ = exc
            if on_error is not None:
                on_error(wrapped, b'')
            else:
                _log.warning(
                    '%s: failed to build query context: %s',
                    msg_cls.__name__,
                    exc,
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
        _reply_result(query, ctx, result, msg_cls, on_error)

    def _dispatch_async(ctx: QueryContext, query: zenoh.Query) -> None:
        import asyncio

        if loop is None or loop.is_closed():
            _log.warning(
                '%s: async on_query handler has no running loop; dropping query',
                msg_cls.__name__,
            )
            return
        # An async generator function is not a coroutine function, so
        # ``handler(ctx)`` yields an async_generator — not awaitable, and
        # not iterable either. Wrap it in a drain coroutine rather than
        # handing it to ``run_coroutine_threadsafe`` raw.
        coro = _drain_async_gen(ctx, handler(ctx), msg_cls, on_error) if is_async_gen else handler(ctx)
        future = asyncio.run_coroutine_threadsafe(coro, loop)

        def _on_done(fut: Future) -> None:
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                _handle_handler_error(query, msg_cls, on_error, exc)
                return
            # ``None`` for the drained-generator path — every reply is
            # already out — so this is a no-op there.
            _reply_result(query, ctx, result, msg_cls, on_error)

        # Holds ``ctx`` (and the live ``zenoh.Query``) until the coroutine
        # resolves, so a late ``reply`` is still valid.
        future.add_done_callback(_on_done)

    return dispatch


def _handle_handler_error(
    query: zenoh.Query,
    msg_cls: type,
    on_error: Callable | None,
    exc: Exception,
) -> None:
    """Route a handler exception and send a best-effort error reply.

    Route a handler exception through ``on_error`` / logging and send a best-effort
    error reply so the getter isn't left hanging.
    """
    wrapped = QueryableError(f'{msg_cls.__name__}: on_query handler raised: {exc}')
    wrapped.__cause__ = exc
    if on_error is not None:
        on_error(wrapped, b'')
    else:
        _log.exception('%s: on_query handler raised', msg_cls.__name__)
    with contextlib.suppress(Exception):
        query.reply_err(str(exc).encode('utf-8'))
