"""Retained-fetch helper for the subscriber dispatch path.

Retained-fetch helper — issue ``session.get(wildcard)`` per declared template and route
reply samples through the subscriber's dispatch path.

Sibling helper inside the ``subscriber`` Pattern B subdir.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar

from ..errors import RetainedFetchError
from ..meta import Origin

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from .._managed_session import SessionLike
    from ..message import Message


_M = TypeVar('_M', bound='Message')

_log = logging.getLogger('zeared.subscriber')


def _fetch_retained(
    session: SessionLike,
    templates: Iterable[Any],
    dispatch: Callable,
    msg_cls: type[Message],
    on_error: Callable | None,
) -> None:
    """Replay cached values into the dispatch path as REPLAY samples.

    Issue ``session.get(wildcard)`` per declared template and route each reply sample
    through the subscriber's dispatch path with ``origin=Origin.REPLAY`` — these are
    cached values delivered after the fact, not publishes the subscriber witnessed.

    Failures to issue the get() are logged (no useful recovery) — the live
    subscriber is still active and will deliver future messages.
    """
    for tpl in templates:
        try:
            replies = session.get(tpl.wildcard)
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                '%s: retained-fetch get() on %s failed: %s',
                msg_cls.__name__,
                tpl.wildcard,
                exc,
            )
            continue
        for reply in replies:
            ok = reply.ok if hasattr(reply, 'ok') else None
            if ok is None:
                continue  # error reply; skip
            try:
                dispatch(ok, origin=Origin.REPLAY)
            except Exception as exc:  # noqa: BLE001
                raw = bytes(getattr(ok, 'payload', b''))
                wrapped = RetainedFetchError(f'{msg_cls.__name__} retained-fetch dispatch failed: {exc}')
                wrapped.__cause__ = exc
                if on_error is not None:
                    on_error(wrapped, raw)
                else:
                    _log.warning(
                        '%s: retained-fetch dispatch failed: %s',
                        msg_cls.__name__,
                        exc,
                    )


def _collect_retained(
    session: SessionLike,
    templates: Iterable[Any],
    msg_cls: type[_M],
    on_error: Callable | None,
) -> list[_M]:
    """Collect retained values as decoded, typed instances.

    Issue ``session.get(wildcard)`` per declared template and return the decoded OK
    replies as typed instances — the collecting sibling of :func:`_fetch_retained`.
    Backs :meth:`Message.fetch_retained`.

    DELETE-kind replies (defensive — retention queryables serve PUTs) are
    skipped. Decode failures and error replies route to ``on_error`` when
    supplied, else log (:func:`_route_error_reply`).
    """
    import zenoh as _zenoh

    import zeared as z

    from ._subscriber_dispatch import _pick_encoding

    out: list[_M] = []
    for tpl in templates:
        try:
            replies = session.get(tpl.wildcard)
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                '%s: fetch_retained get() on %s failed: %s',
                msg_cls.__name__,
                tpl.wildcard,
                exc,
            )
            continue
        for reply in replies:
            ok = reply.ok if hasattr(reply, 'ok') else None
            if ok is None:
                _route_error_reply(reply, msg_cls, tpl.wildcard, on_error)
                continue
            if ok.kind == _zenoh.SampleKind.DELETE:
                continue  # tombstone; not part of the live set
            try:
                raw = bytes(ok.payload)
                key = str(ok.key_expr)
                enc = _pick_encoding(ok, msg_cls.ENCODING, z.debug)
                msg, _captures = msg_cls._decode(raw, key, enc)  # noqa: SLF001
            except Exception as exc:  # noqa: BLE001
                raw_bytes = bytes(getattr(ok, 'payload', b''))
                wrapped = RetainedFetchError(f'{msg_cls.__name__} fetch_retained decode failed: {exc}')
                wrapped.__cause__ = exc
                if on_error is not None:
                    on_error(wrapped, raw_bytes)
                else:
                    _log.warning(
                        '%s: fetch_retained decode failed: %s',
                        msg_cls.__name__,
                        exc,
                    )
                continue
            out.append(msg)
    return out


def _route_error_reply(reply: Any, msg_cls: type[Message], wildcard: str, on_error: Callable | None) -> None:
    """Hand ``fetch_retained`` an error reply as a ``RetainedFetchError``, through ``on_error`` or the log (0.4.4).

    A server's ``reply_err``, or Zenoh's own when a queryable didn't answer in
    time (its payload ``b'Timeout'``): either way the fetched set may be
    partial. As ``query`` routes its error replies: the raw bytes are the
    reply's payload, and there's no ``__cause__``.
    """
    err = getattr(reply, 'err', None)
    payload = b''
    if err is not None:
        try:
            payload = bytes(err.payload)
        except Exception:  # noqa: BLE001
            payload = b''
    wrapped = RetainedFetchError(
        f'{msg_cls.__name__}: error reply to retained fetch {wildcard!r}: {payload.decode("utf-8", "replace")!r}'
    )
    if on_error is not None:
        on_error(wrapped, payload)
    else:
        _log.warning('%s', wrapped)
