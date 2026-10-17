"""Wire-aware introspection producing :class:`MessageDoc`.

Wire-aware introspection: a :class:`MessageDoc` = seared's ``SchemaDoc`` plus the
``Message`` ClassVar contract (topic, schema version, encoding, retention, request
payload, declared effect).

Reuses ``seared.doc.introspect`` for the field/enum/variant half; adds only
the zeared-specific wire metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeGuard

from seared.doc import SchemaDoc, is_seared_class
from seared.doc import introspect as _seared_introspect

from .._template import Template
from ..effect import Effect, effect_of
from ..message import Message


@dataclass(frozen=True, slots=True)
class SlotDoc:
    name: str
    is_field: bool  # also a declared payload field (surfaced on receive)
    # A trailing ``{name**}`` slot, one segment or more (0.4.2). Appended with
    # a default, so positional construction keeps working.
    multi: bool = False


@dataclass(frozen=True, slots=True)
class MessageDoc:
    schema: SchemaDoc
    topic: str
    extra_topics: tuple[str, ...]
    slots: tuple[SlotDoc, ...]
    category: str | None
    schema_version: str | None
    encoding: str
    retained: bool
    liveliness: bool
    dedupe: bool
    retention_ttl: float | None
    request: type | None
    effect: Effect | None = None  # 0.3.12; ``None`` when undeclared, never inferred


def is_message_class(obj: Any) -> TypeGuard[type[Message]]:
    """True for a Message class that ``@zeared`` built (not the base).

    Asked as seared's ``is_seared_class`` asks it (0.4.2): a decorated Message
    counts whether or not it has fields, and a subclass of one that isn't
    decorated itself doesn't. Until then a Message with no fields got a plain
    page, with no topic, and an undecorated subclass got a Message page.
    """
    return isinstance(obj, type) and issubclass(obj, Message) and obj is not Message and is_seared_class(obj)


def _slots(topics: list[str], field_attrs: set[str]) -> tuple[SlotDoc, ...]:
    """Every slot of every topic, each once, in the order they first appear.

    Read with zeared's own template parser, so ``{name**}`` is found, as the
    regex used until 0.4.2 never did.
    """
    slots: dict[str, SlotDoc] = {}
    for topic in topics:
        for name in Template.parse(topic).field_names:
            if name not in slots:
                multi = '{' + name + '**}' in topic
                slots[name] = SlotDoc(name=name, is_field=name in field_attrs, multi=multi)
    return tuple(slots.values())


def introspect_message(cls: type[Message]) -> MessageDoc:
    """Introspect ``cls`` into a :class:`MessageDoc`."""
    schema = _seared_introspect(cls)
    topic = getattr(cls, 'TOPIC', '') or ''
    extra_topics = tuple(getattr(cls, 'EXTRA_TOPICS', ()) or ())
    slots = _slots([t for t in (topic, *extra_topics) if t], {f.attr for f in schema.fields})
    parts = topic.split('/')
    category = parts[1] if len(parts) > 1 and '{' not in parts[1] else None
    return MessageDoc(
        schema=schema,
        topic=topic,
        extra_topics=extra_topics,
        slots=slots,
        category=category,
        schema_version=getattr(cls, 'SCHEMA', None),
        encoding=getattr(cls, 'ENCODING', 'msgpack'),
        retained=bool(getattr(cls, 'RETAINED', False)),
        liveliness=bool(getattr(cls, 'LIVELINESS', False)),
        dedupe=bool(getattr(cls, 'DEDUPE', True)),
        retention_ttl=getattr(cls, 'RETENTION_TTL', None),
        request=getattr(cls, 'REQUEST', None),
        # Validates: a bad ``EFFECT`` fails doc generation and ``--check``.
        effect=effect_of(cls),
    )
