"""``QueryContext`` — a narrow wrapper over ``zenoh.Query`` handed to
``on_query`` handlers so user code never imports Zenoh types (same
principle as :class:`zeared.ZenohMeta` on the subscribe side).

Exposes the queried key expression, parsed selector parameters, matched
template captures, and an optional decoded request payload; plus
``reply`` / ``reply_err`` / ``reply_del`` to answer.

Sibling helper inside the ``queryable`` Pattern B subdir.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import parse_qsl

from .. import _codec as codec

if TYPE_CHECKING:
    import zenoh

    from ..message import Message


_log = logging.getLogger('zeared.queryable')


def _parse_params(parameters: str) -> dict[str, str]:
    """Parse a Zenoh selector parameter string (``a=1&b=2``) into a dict.

    Permissive: bare flags (``verbose``) map to an empty string; malformed
    input degrades to whatever ``parse_qsl`` recovers rather than raising.
    """
    if not parameters:
        return {}
    return dict(parse_qsl(parameters, keep_blank_values=True))


def _pick_query_encoding(query: 'zenoh.Query') -> codec.Encoding:
    """Encoding to decode a request payload with — honour the query's
    declared encoding hint, else default to msgpack."""
    enc = getattr(query, 'encoding', None)
    declared = str(enc) if enc is not None else ''
    if 'json' in declared:
        return 'json'
    return 'msgpack'


class QueryContext:
    """The request side of an incoming query, handed to an ``on_query``
    handler.

    Attributes:
        key_expr: the queried key expression (may be concrete or contain
            wildcards, depending on what the getter asked for).
        selector: the full selector string (key expression + parameters).
        parameters: the raw parameter string (``''`` when none).
        params: ``parameters`` parsed into a ``{str: str}`` dict.
        captures: template-slot values extracted from ``key_expr`` — empty
            when the query used a wildcard the template regex can't bind.
        request: the decoded request payload. When the class sets
            ``REQUEST = SomeClass`` and the query carried a payload, this is
            a decoded ``SomeClass`` instance; otherwise the raw payload
            ``bytes`` (or ``None`` when the query carried no payload).
    """

    __slots__ = (
        '_query', '_msg_cls', '_matched_template',
        'key_expr', 'selector', 'parameters', 'params', 'captures', 'request',
    )

    def __init__(self, query: 'zenoh.Query', msg_cls: 'type[Message]'):
        self._query = query
        self._msg_cls = msg_cls
        self.key_expr = str(query.key_expr)
        self.selector = str(query.selector)
        self.parameters = str(query.parameters) if query.parameters else ''
        self.params = _parse_params(self.parameters)

        # Best-effort template captures — a wildcard query key may not bind.
        # Also remember which template matched so ``reply`` renders on it:
        # a reply key must intersect the query key-expr, so an EXTRA_TOPICS
        # query must be answered on that same template, not the canonical.
        captures: dict[str, str] = {}
        self._matched_template = None
        tpls = msg_cls._templates()
        if tpls.all:
            match = tpls.match(self.key_expr)
            if match is not None:
                self._matched_template, captures = match
        self.captures = captures

        self.request = self._decode_request(query)

    def _decode_request(self, query: 'zenoh.Query') -> Any:
        """Decode the query payload into ``REQUEST`` (if declared) or return
        the raw bytes / ``None``."""
        payload = getattr(query, 'payload', None)
        if payload is None:
            return None
        raw = bytes(payload)
        req_cls = getattr(self._msg_cls, 'REQUEST', None)
        if req_cls is None:
            return raw
        try:
            enc = _pick_query_encoding(query)
            data = codec.unpack(raw, enc)
            return req_cls.load(data, format=enc)
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                '%s: failed to decode REQUEST payload: %s',
                self._msg_cls.__name__, exc,
            )
            return raw

    # -- reply surface ----------------------------------------------------

    def reply(self, instance: 'Message') -> None:
        """Answer the query with a message instance.

        Renders the instance's canonical topic (the reply key expression),
        strips template fields from the body, packs with the instance's
        effective encoding, and stamps the class ``SCHEMA`` attachment —
        identical wire shape to ``instance.send()``.
        """
        import zeared as z

        enc = codec.effective_encoding(instance.ENCODING, z.debug)
        # seared's ``Seared.load`` / ``Seared.dump`` base stubs omit the
        # ``format=`` carrier hint the decorator-attached implementations
        # actually take. Correct call, wrong stub — drop the suppression
        # once seared widens them.
        data = type(instance).dump(instance, format=enc)  # ty: ignore[unknown-argument]
        # Render on the template the query matched (so the reply key
        # intersects the query key-expr); fall back to the canonical topic
        # for a same-class instance when nothing matched (e.g. a wildcard
        # query key the regex couldn't bind).
        template = self._matched_template
        if template is None or type(instance) is not self._msg_cls:
            template = type(instance)._templates().resolve_publish_topic(None)
        key = template.render(data)
        payload_dict = {
            k: v for k, v in data.items() if k not in template.field_names
        }
        raw = codec.pack(payload_dict, enc)
        attachment = type(instance)._schema_attachment_bytes()
        self._query.reply(
            key, raw, encoding=codec.MIME[enc], attachment=attachment,
        )

    def reply_err(self, payload: Any) -> None:
        """Answer the query with an error reply. ``payload`` may be ``str``
        (utf-8 encoded) or ``bytes``."""
        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        self._query.reply_err(payload)

    def reply_del(self, key_expr: str) -> None:
        """Answer the query with a delete (tombstone) reply for ``key_expr``."""
        self._query.reply_del(key_expr)


__all__ = ['QueryContext']
