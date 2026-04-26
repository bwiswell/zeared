"""``_MessageTopicMixin`` — topic-template parsing + schema attachment cache.

Methods extracted into a mixin per the variant of Pattern B codified in
``CLAUDE.local.md``: ``Message`` composes this mixin via MRO so the
methods stay as classmethods (not helper functions taking ``cls``).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from .. import _codec as codec
from .._template import Templates
from ..errors import TopicError

if TYPE_CHECKING:
    pass


class _MessageTopicMixin:
    """Topic / schema metadata methods for :class:`Message`. Mixin —
    contributes no instance state.
    """
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
            raise TopicError(f'{cls.__name__}: TOPIC is not defined')
        extras = tuple(getattr(cls, 'EXTRA_TOPICS', ()) or ())
        tpls = Templates.build(topic, extras)
        cls._validate_multi_segment_field_bindings(tpls)
        cls._TEMPLATES_CACHE = tpls
        return tpls

    @classmethod
    def _schema_attachment_bytes(cls) -> Optional[bytes]:
        """Return the cached msgpack-encoded attachment bytes for this
        class's ``SCHEMA`` value, or ``None`` if ``SCHEMA`` is not set.

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
            cls._SCHEMA_ATTACHMENT_CACHE = b''   # sentinel for "no schema"
            return None
        attachment = codec.pack({'schema': schema}, 'msgpack')
        cls._SCHEMA_ATTACHMENT_CACHE = attachment
        return attachment

    @classmethod
    def _validate_multi_segment_field_bindings(cls, tpls: 'Templates') -> None:
        """Reject {name**} slots whose declared field can't hold a path-tail
        string. Path tails are slash-containing strings; only z.Str(many=False,
        keyed=False) makes sense. Catching this here (first _templates() call)
        beats waiting for a runtime coercion failure on a peer in production.
        """
        if not tpls.multi_field_names:
            return
        specs = getattr(cls, '__seared_fields__', ())
        if not specs:
            return
        spec_by_attr = {attr: f for attr, _, f in specs}
        for name in tpls.multi_field_names:
            f = spec_by_attr.get(name)
            if f is None:
                continue   # capture-only — lands on meta.captures as a str
            cls_name = type(f).__name__
            ok = (cls_name == 'Str') and not getattr(f, 'many', False) \
                and not getattr(f, 'keyed', False)
            if not ok:
                raise TopicError(
                    f'{cls.__name__}: multi-segment field {name!r} must bind '
                    f'to z.Str(many=False, keyed=False); got {cls_name}'
                    f'{"(many=True)" if getattr(f, "many", False) else ""}'
                    f'{"(keyed=True)" if getattr(f, "keyed", False) else ""}'
                )
