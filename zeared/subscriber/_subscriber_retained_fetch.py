"""Retained-fetch helper — issue ``session.get(wildcard)`` per declared
template and route reply samples through the subscriber's dispatch path.

Sibling helper inside the ``subscriber`` Pattern B subdir.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, List, Optional, TypeVar

from ..errors import RetainedFetchError
from ..meta import Origin

if TYPE_CHECKING:
    import zenoh

    from ..message import Message


_M = TypeVar('_M', bound='Message')

_log = logging.getLogger('zeared.subscriber')


def _fetch_retained(
    session: 'zenoh.Session',
    templates,
    dispatch: Callable,
    msg_cls: 'type[Message]',
    on_error: Optional[Callable],
) -> None:
    """Issue ``session.get(wildcard)`` per declared template and route each
    reply sample through the subscriber's dispatch path with
    ``origin=Origin.REPLAY`` — these are cached values delivered after the
    fact, not publishes the subscriber witnessed.

    Failures to issue the get() are logged (no useful recovery) — the live
    subscriber is still active and will deliver future messages.
    """
    for tpl in templates:
        try:
            replies = session.get(tpl.wildcard)
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                '%s: retained-fetch get() on %s failed: %s',
                msg_cls.__name__, tpl.wildcard, exc,
            )
            continue
        for reply in replies:
            ok = reply.ok if hasattr(reply, 'ok') else None
            if ok is None:
                continue   # error reply; skip
            try:
                dispatch(ok, origin=Origin.REPLAY)
            except Exception as exc:  # noqa: BLE001
                raw = bytes(getattr(ok, 'payload', b''))
                wrapped = RetainedFetchError(
                    f'{msg_cls.__name__} retained-fetch dispatch failed: {exc}'
                )
                wrapped.__cause__ = exc
                if on_error is not None:
                    on_error(wrapped, raw)
                else:
                    _log.warning(
                        '%s: retained-fetch dispatch failed: %s',
                        msg_cls.__name__, exc,
                    )


def _collect_retained(
    session: 'zenoh.Session',
    templates,
    msg_cls: 'type[_M]',
    on_error: Optional[Callable],
) -> 'List[_M]':
    """Issue ``session.get(wildcard)`` per declared template and return the
    decoded OK replies as typed instances — the collecting sibling of
    :func:`_fetch_retained`. Backs :meth:`Message.fetch_retained`.

    DELETE-kind replies (defensive — retention queryables serve PUTs) are
    skipped. Decode failures route to ``on_error`` when supplied, else log.
    """
    import zenoh as _zenoh

    import zeared as z

    from ._subscriber_dispatch import _pick_encoding

    out: 'List[_M]' = []
    for tpl in templates:
        try:
            replies = session.get(tpl.wildcard)
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                '%s: fetch_retained get() on %s failed: %s',
                msg_cls.__name__, tpl.wildcard, exc,
            )
            continue
        for reply in replies:
            ok = reply.ok if hasattr(reply, 'ok') else None
            if ok is None:
                continue   # error reply; skip
            if ok.kind == _zenoh.SampleKind.DELETE:
                continue   # tombstone; not part of the live set
            try:
                raw = bytes(ok.payload)
                key = str(ok.key_expr)
                enc = _pick_encoding(ok, msg_cls.ENCODING, z.debug)
                msg, _captures = msg_cls._decode(raw, key, enc)
            except Exception as exc:  # noqa: BLE001
                raw_bytes = bytes(getattr(ok, 'payload', b''))
                wrapped = RetainedFetchError(
                    f'{msg_cls.__name__} fetch_retained decode failed: {exc}'
                )
                wrapped.__cause__ = exc
                if on_error is not None:
                    on_error(wrapped, raw_bytes)
                else:
                    _log.warning(
                        '%s: fetch_retained decode failed: %s',
                        msg_cls.__name__, exc,
                    )
                continue
            out.append(msg)
    return out
