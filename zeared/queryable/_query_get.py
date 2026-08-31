"""Get-side of ``Cls.query`` — issue, decode, and collect replies.

Get-side of ``Cls.query`` — issue ``session.get(selector)``, decode each OK reply
through the message class's own ``_decode`` path, and collect typed instances.

Stateless (no registry): the getter is to ``Cls.query`` what ``send`` /
``_fetch_retained`` are to publish — one round trip, no long-lived
resource. Error replies and decode failures route to ``on_error`` when
supplied, else log; ``query`` returns only successfully decoded results.

Sibling helper inside the ``queryable`` Pattern B subdir.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar

from .. import _codec as codec
from ..errors import QueryError

_M = TypeVar('_M', bound='Message')

if TYPE_CHECKING:
    from collections.abc import Callable

    import zenoh

    from .._managed_session import SessionLike
    from ..message import Message


_log = logging.getLogger('zeared.queryable')


def _build_selector(msg_cls: type[Message], key_fields: dict, params: dict | None) -> str:
    """Render a selector from the canonical template + key fields + params.

    Missing key slots widen to wildcards (see ``Template.render_selector``).
    ``params`` (a ``{str: str}`` mapping) is appended as ``?k=v&…``.
    """
    template = msg_cls._templates().canonical  # noqa: SLF001
    key = template.render_selector(key_fields)
    if params:
        from urllib.parse import urlencode

        return f'{key}?{urlencode(params)}'
    return key


def _pick_reply_encoding(sample: zenoh.Sample, cls_encoding: codec.Encoding, debug: bool) -> codec.Encoding:  # noqa: FBT001
    """Encoding to decode a reply sample with.

    Encoding to decode a reply sample with — honour its declared hint, else the class
    default (debug forces JSON outbound only).
    """
    declared = str(sample.encoding) if sample.encoding is not None else ''
    if 'json' in declared:
        return 'json'
    if 'msgpack' in declared:
        return 'msgpack'
    return codec.effective_encoding(cls_encoding, debug)


def _run_query(  # noqa: PLR0913
    session: SessionLike,
    msg_cls: type[_M],
    key_fields: dict,
    *,
    params: dict | None,
    request: Any,
    timeout: float | None,
    target: Any,
    consolidation: Any,
    on_error: Callable[[Exception, bytes], None] | None,
) -> list[_M]:
    """Issue the get, decode OK replies, return typed instances."""
    import zeared as z

    selector = _build_selector(msg_cls, key_fields, params)

    kwargs: dict = {}
    if timeout is not None:
        kwargs['timeout'] = timeout
    if target is not None:
        kwargs['target'] = target
    # Default to NO consolidation: a zeared query is a fan-out (ask every
    # queryable — the RTLS "ask every reader" pattern), and each queryable
    # may reply more than once. Zenoh's default (LATEST/MONOTONIC)
    # collapses replies that share a key expression, silently dropping
    # legitimate answers. Callers who want dedup pass ``consolidation=``.
    import zenoh

    kwargs['consolidation'] = consolidation if consolidation is not None else zenoh.ConsolidationMode.NONE
    if request is not None:
        enc = codec.effective_encoding(
            getattr(request, 'ENCODING', 'msgpack'),
            z.debug,
        )
        raw_req = type(request).dump(request, format=enc)
        kwargs['payload'] = codec.pack(raw_req, enc)
        kwargs['encoding'] = codec.MIME[enc]

    replies = session.get(selector, **kwargs)

    out: list[_M] = []
    for reply in replies:
        ok = reply.ok if hasattr(reply, 'ok') else None
        if ok is None:
            _route_error_reply(reply, msg_cls, selector, on_error)
            continue
        try:
            raw = bytes(ok.payload)
            key = str(ok.key_expr)
            enc = _pick_reply_encoding(ok, msg_cls.ENCODING, z.debug)
            msg, _captures = msg_cls._decode(raw, key, enc)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            raw_bytes = bytes(getattr(ok, 'payload', b''))
            wrapped = QueryError(f'{msg_cls.__name__}: query reply decode failed: {exc}')
            wrapped.__cause__ = exc
            if on_error is not None:
                on_error(wrapped, raw_bytes)
            else:
                _log.warning(
                    '%s: query reply decode failed: %s',
                    msg_cls.__name__,
                    exc,
                )
            continue
        out.append(msg)
    return out


def _route_error_reply(reply: Any, msg_cls: type[Message], selector: str, on_error: Callable | None) -> None:
    """Surface a remote error reply (``query.reply_err``) via ``on_error`` or logging."""
    err = getattr(reply, 'err', None)
    payload = b''
    if err is not None:
        try:
            payload = bytes(err.payload)
        except Exception:  # noqa: BLE001
            payload = b''
    wrapped = QueryError(
        f'{msg_cls.__name__}: error reply to query {selector!r}: {payload.decode("utf-8", "replace")!r}'
    )
    if on_error is not None:
        on_error(wrapped, payload)
    else:
        _log.warning('%s', wrapped)
