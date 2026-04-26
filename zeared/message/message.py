"""``Message`` — the zeared message base class.

Primary file of the ``message`` Pattern B subdir. The class itself is
slim: declared class attributes plus the ``unretain`` descriptor and
the ``_decode`` classmethod. Method clusters extracted into mixins
per the variant codified in ``CLAUDE.local.md``:

- ``_MessageTopicMixin`` (``_message_topic.py``) — template parsing,
  schema attachment cache, multi-segment field-binding validation.
- ``_MessageSendMixin`` (``_message_send.py``) — sync ``send`` /
  ``send_batch``.
- ``_MessageAsyncMixin`` (``_message_async.py``) — async siblings:
  ``asend`` / ``asend_batch`` / ``aunretain`` / ``alisten``.
- ``_MessageWillMixin`` (``_message_will.py``) — LWT registration:
  ``register_will`` / ``aregister_will``.
- ``_MessageSubscribeMixin`` (``_message_subscribe.py``) — subscribe
  + introspection: ``on_message`` / ``published_topics``.

Plus ``_message_unretain.py`` for the ``_UnretainDescriptor`` and the
shared ``_unretain_impl`` function (instance-vs-class ``unretain``
dispatch).
"""
from __future__ import annotations

from typing import ClassVar, Literal, Optional, Tuple, Union

import seared as s

from .. import _codec as codec
from ._message_async import _MessageAsyncMixin
from ._message_send import _MessageSendMixin
from ._message_subscribe import _MessageSubscribeMixin
from ._message_topic import _MessageTopicMixin
from ._message_unretain import _UnretainDescriptor
from ._message_will import _MessageWillMixin


Encoding = Literal['msgpack', 'json']


class Message(
    _MessageTopicMixin,
    _MessageSendMixin,
    _MessageAsyncMixin,
    _MessageWillMixin,
    _MessageSubscribeMixin,
    s.Seared,
):
    """Base class for zeared messages.

    Subclasses declare a ``TOPIC`` (optionally with ``{field}`` slots) and
    any ``@s.seared`` field set they like. Use ``ENCODING = 'json'`` to
    opt out of msgpack for a specific class; set ``zeared.debug = True`` at
    the module level to force JSON globally regardless of this attribute.

    ``PUBLISHER`` governs whether zeared caches a long-lived
    ``zenoh.Publisher`` per concrete topic. ``True`` (the default) enables
    caching with a soft cap of 256 concrete keys per ``(cls, session)``;
    an ``int`` overrides the cap; ``False`` disables caching and falls
    through to ``session.put`` on every send.

    ``EXTRA_TOPICS`` declares additional topic templates the class can
    receive from. The canonical ``TOPIC`` remains the publish default; a
    restrictive per-call override ``send(topic=...)`` picks a different
    declared template. Each declared template parses independently — slot
    sets are NOT required to match across templates. Captures from any
    template land on ``meta.captures``; declared seared fields with
    matching names are coerced onto the instance.
    """

    TOPIC: ClassVar[str]
    ENCODING: ClassVar[Encoding] = 'msgpack'
    PUBLISHER: ClassVar[Union[bool, int]] = True
    EXTRA_TOPICS: ClassVar[Tuple[str, ...]] = ()
    RETAINED: ClassVar[bool] = False
    LIVELINESS: ClassVar[bool] = False
    DEDUPE: ClassVar[bool] = True
    # Optional retention TTL in seconds. ``None`` (default) means
    # retained values live forever (or until the publishing session
    # closes / a tombstone is emitted). Any positive float opts the
    # class into lazy time-based expiration: entries older than the
    # TTL are skipped + pruned at query time. No background thread —
    # expiration is checked when ``_RetentionCache._handle_query`` runs
    # (i.e. when a subscriber issues a retained-fetch get). Topics
    # that never get queried may keep stale entries in the cache;
    # acceptable for typical use, documented as a limit.
    RETENTION_TTL: ClassVar[Optional[float]] = None
    # Optional schema-version marker. ``None`` (default) opts the class
    # out of attachment-based schema stamping; any string value enables
    # it — the value rides as a Zenoh sample attachment (msgpack-encoded
    # ``{schema: <value>}``). Subscribers compare against their own
    # ``SCHEMA`` and route mismatches via ``on_error`` as
    # :class:`SchemaMismatchError`. zeared does not enforce a specific
    # shape — semver / hash / build-id all work.
    SCHEMA: ClassVar[Optional[str]] = None

    # ``unretain`` dispatches on instance-vs-class access via _UnretainDescriptor.
    #   msg.unretain()                    → uses self's template fields
    #   Cls.unretain(**key_fields)        → uses explicit kwargs
    unretain = _UnretainDescriptor()

    @classmethod
    def _decode(
        cls, raw: bytes, key_expr: str, encoding: Encoding,
    ) -> tuple['Message', dict[str, str]]:
        """Reconstruct a message instance from wire bytes + key expression.

        Returns ``(instance, captures_dict)``: the decoded message plus the
        raw (string) captures from the matched template. Slot names that
        are also declared seared fields on the class are additionally
        coerced onto the instance; slot names without a matching declared
        field are returned only in the captures dict (the subscriber puts
        them on ``meta.captures``).
        """
        payload = codec.unpack(raw, encoding)
        if not isinstance(payload, dict):
            raise ValueError(
                f'{cls.__name__}: expected dict payload, got {type(payload).__name__}'
            )
        tpls = cls._templates()
        captured: dict[str, str] = {}
        if tpls.all:
            match = tpls.match(key_expr)
            if match is None:
                raise ValueError(
                    f'{cls.__name__}: key_expr {key_expr!r} does not match '
                    f'any declared topic'
                )
            _, captured = match
            spec_by_attr = {attr: f for attr, _, f in cls.__seared_fields__}
            for name, raw_val in captured.items():
                f = spec_by_attr.get(name)
                if f is not None:
                    payload[name] = f.deserialize(raw_val, validate=True)
                # else: capture-only; left to meta.captures
        # Thread ``format=`` into seared's load so native-bytes payloads
        # from msgpack carriers decode through ``Bytes.deserialize``'s
        # native-bytes path. JSON path unchanged.
        return cls.load(payload, format=encoding), captured


__all__ = ['Message', 'Encoding']
