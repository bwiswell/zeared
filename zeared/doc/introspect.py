"""Wire-aware introspection producing :class:`MessageDoc`.

Wire-aware introspection: a :class:`MessageDoc` = seared's ``SchemaDoc`` plus the
``Message`` ClassVar contract (topic, schema version, encoding, retention, request
payload).

Reuses ``seared.doc.introspect`` for the field/enum/variant half; adds only
the zeared-specific wire metadata.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, TypeGuard

from seared.doc import SchemaDoc
from seared.doc import introspect as _seared_introspect

from ..message import Message

_SLOT = re.compile(r'\{(\w+)\}')


@dataclass(frozen=True, slots=True)
class SlotDoc:
    name: str
    is_field: bool  # also a declared payload field (surfaced on receive)


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


def is_message_class(obj: Any) -> TypeGuard[type[Message]]:
    """True for a concrete ``@zeared`` Message subclass (not the base)."""
    return (
        isinstance(obj, type)
        and issubclass(obj, Message)
        and obj is not Message
        and bool(getattr(obj, '__seared_fields__', ()))
    )


def introspect_message(cls: type[Message]) -> MessageDoc:
    """Introspect ``cls`` into a :class:`MessageDoc`."""
    schema = _seared_introspect(cls)
    topic = getattr(cls, 'TOPIC', '') or ''
    field_attrs = {f.attr for f in schema.fields}
    seen: set[str] = set()
    slots = tuple(
        SlotDoc(name=n, is_field=n in field_attrs) for n in _SLOT.findall(topic) if not (n in seen or seen.add(n))
    )
    parts = topic.split('/')
    category = parts[1] if len(parts) > 1 and '{' not in parts[1] else None
    return MessageDoc(
        schema=schema,
        topic=topic,
        extra_topics=tuple(getattr(cls, 'EXTRA_TOPICS', ()) or ()),
        slots=slots,
        category=category,
        schema_version=getattr(cls, 'SCHEMA', None),
        encoding=getattr(cls, 'ENCODING', 'msgpack'),
        retained=bool(getattr(cls, 'RETAINED', False)),
        liveliness=bool(getattr(cls, 'LIVELINESS', False)),
        dedupe=bool(getattr(cls, 'DEDUPE', True)),
        retention_ttl=getattr(cls, 'RETENTION_TTL', None),
        request=getattr(cls, 'REQUEST', None),
    )
