"""``_MessageTopicMixin`` — topic-template parsing + schema attachment cache.

One extracted method cluster: ``Message`` composes this stateless mixin
via the MRO so the methods stay classmethods, rather than becoming helper
functions taking ``cls``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .. import _codec as codec
from .._template import Templates
from ..errors import TopicError

if TYPE_CHECKING:
    from seared import FieldSpec

    from .message import Message


class _MessageTopicMixin:
    """Topic / schema metadata methods for :class:`Message`. Mixin — contributes no instance state."""

    __slots__ = ()

    @classmethod
    def _templates(cls) -> Templates:
        """Parse the class's declared topics once and cache on the class.

        Template slots are **not** required to be declared seared fields:
        slots that aren't declared become capture-only (surfaced via
        ``ZenohMeta.captures`` on receive). Slots that ARE declared fields
        still get their captured value coerced onto the instance.

        Publishability is validated at send-time (in
        ``Templates.resolve_publish_topic``): ``**`` templates are
        subscribe-only, and missing fields at render-time produce a clear
        ``TopicError``.
        """
        cached = cls.__dict__.get('_TEMPLATES_CACHE')
        if cached is not None:
            return cached
        topic = getattr(cls, 'TOPIC', None)
        if topic is None:
            msg = f'{cls.__name__}: TOPIC is not defined'
            raise TopicError(msg)
        extras = tuple(getattr(cls, 'EXTRA_TOPICS', ()) or ())
        tpls = Templates.build(topic, extras)
        cls._validate_slot_field_bindings(tpls)
        cls._TEMPLATES_CACHE = tpls
        return tpls

    @classmethod
    def _field_specs(cls) -> dict[str, FieldSpec]:
        """Map each declared field's attribute name to its ``FieldSpec``, cached on the class.

        The topic render and capture coercion (``_message_render.py``) look
        slot fields up by attribute name on every send and every decode, so
        the map is built once per class rather than per call. Same
        ``cls.__dict__`` keying as ``_templates`` — a subclass gets its own.
        """
        cached = cls.__dict__.get('_FIELD_SPECS_CACHE')
        if cached is not None:
            return cached
        specs = {spec.attr: spec for spec in getattr(cls, '__seared_fields__', ())}
        cls._FIELD_SPECS_CACHE = specs
        return specs

    @classmethod
    def _schema_attachment_bytes(cls: type[Message]) -> bytes | None:
        """Cached msgpack attachment bytes for this class's ``SCHEMA``.

        Return the cached msgpack-encoded attachment bytes for this class's ``SCHEMA``
        value, or ``None`` if ``SCHEMA`` is not set.

        Built lazily on first call and cached on the class — the schema
        value is fixed at class definition, so per-send rebuild would
        be wasted work. Cache key uses ``cls.__dict__`` so subclasses
        with their own ``SCHEMA`` get their own cache entry.
        """
        cached = cls.__dict__.get('_SCHEMA_ATTACHMENT_CACHE')
        if cached is not None:
            return cached if cached != b'' else None
        schema = cls.SCHEMA
        if schema is None:
            cls._SCHEMA_ATTACHMENT_CACHE = b''  # sentinel for "no schema"
            return None
        attachment = codec.pack({'schema': schema}, 'msgpack')
        cls._SCHEMA_ATTACHMENT_CACHE = attachment
        return attachment

    @classmethod
    def _validate_slot_field_bindings(cls, tpls: Templates) -> None:
        """Reject slot bindings whose field can't render as its key segment(s).

        - ``{name**}`` (multi-segment): path tails are slash-containing strings,
          so only ``z.Str(many=False, keyed=False)`` makes sense.
        - ``{name}`` (single-segment, 0.4.0): the field's string form must never
          contain ``/``, or the rendered key splits and matches no template.
          ``Path`` can, and so can ``Bytes`` in its default base64 form;
          ``Bytes(encoding='hex')`` can't.

        Catching this here (first ``_templates()`` call) beats a key that
        silently matches nothing, or a coercion failure, on a peer in production.
        """
        specs = getattr(cls, '__seared_fields__', ())
        if not specs:
            return
        spec_by_attr = {attr: f for attr, _, f in specs}
        for name in tpls.multi_field_names:
            f = spec_by_attr.get(name)
            if f is None:
                continue  # capture-only — lands on meta.captures as a str
            cls_name = type(f).__name__
            ok = (cls_name == 'Str') and not getattr(f, 'many', False) and not getattr(f, 'keyed', False)
            if not ok:
                msg = (
                    f'{cls.__name__}: multi-segment field {name!r} must bind '
                    f'to z.Str(many=False, keyed=False); got {cls_name}'
                    f'{"(many=True)" if getattr(f, "many", False) else ""}'
                    f'{"(keyed=True)" if getattr(f, "keyed", False) else ""}'
                )
                raise TopicError(msg)
        single = {n for t in tpls.all for n in t.field_names if n != t._named_multi}  # noqa: SLF001
        for name in sorted(single):
            f = spec_by_attr.get(name)
            if f is None:
                continue
            kind = type(f).__name__
            if kind == 'Path':
                hint = 'bind it to z.Str, or use a trailing {' + name + '**} slot for a path tail'
            elif kind == 'Bytes' and getattr(f, 'encoding', 'base64') != 'hex':
                hint = "use z.Bytes(encoding='hex'), whose string form is segment-safe"
            else:
                continue
            msg = (
                f'{cls.__name__}: single-segment slot {name!r} is bound to {kind}, whose string form '
                f"can contain '/' and split the key; {hint}"
            )
            raise TopicError(msg)
