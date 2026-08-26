"""Retained-fetch helper — issue ``session.get(wildcard)`` per declared
template and route reply samples through the subscriber's dispatch path.

Sibling helper inside the ``subscriber`` Pattern B subdir.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Optional, Type

from ..errors import RetainedFetchError

if TYPE_CHECKING:
    import zenoh


_log = logging.getLogger('zeared.subscriber')


def _fetch_retained(
    session: 'zenoh.Session',
    templates,
    dispatch: Callable,
    msg_cls: Type,
    on_error: Optional[Callable],
) -> None:
    """Issue ``session.get(wildcard)`` per declared template and route each
    reply sample through the subscriber's dispatch path.

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
                dispatch(ok)
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
