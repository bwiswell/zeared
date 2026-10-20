"""Canonical topic render — the one place a slot value becomes a key segment.

Sibling helper inside the ``message`` Pattern B subdir. Every path that
builds a concrete key goes through here: ``send`` and ``declare_presence``
(from the instance or caller slots), ``QueryContext.reply`` (from the reply instance),
``unretain`` (instance or caller slots), the query getters' selectors, and
the public ``concrete_topic``. Before 0.3.8 each path rendered from its own
input and they disagreed for ``Enum``, ``DateTime``, ``TimeDelta`` and
``Bytes`` slots, and for any slot field with a ``data_key``.

The rule for one slot value:

- **A non-string value whose exact type is its field's Python type**
  renders as ``str(field.serialize(value))``: the field's JSON-carrier
  string form, which is also what a subscriber's capture decodes back.
  ``serialize`` gets no ``format=`` hint, so a key is the same whatever the
  class's ``ENCODING``. The check is exact, so a ``bool`` is not an ``int``.
- **Anything else renders as** ``str(value)``, exactly as before. Strings
  pass through verbatim, so selector wildcards (``robot='r-$*'``) and
  pre-rendered values keep working, and nothing here raises on a value the
  old render accepted.
- **A slot with no declared field** (capture-only) renders as
  ``str(value)``.

``None`` is left alone for the caller's template method to handle as it
always has: a selector widens it to a wildcard, and ``Template.render``
formats it as ``'None'``. The instance renders skip ``None`` attributes
instead, so a missing slot raises ``TopicError`` as it did when ``send``
rendered from ``dump()`` output.

The reverse direction lives here too. ``_coerce_captures`` turns a key's
raw string captures back into typed values, shared by ``Message._decode``
and the public ``coerce_captures``.

``Template.render`` / ``Template.render_selector`` stay plain
``str.format`` renderers; this module sits above them. Consumers that call
them directly keep their current behaviour.
"""

from __future__ import annotations

import datetime as dt
import decimal
import uuid
from typing import TYPE_CHECKING, Any

from seared import ValidationError

from ..errors import TopicError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .._template import Template
    from .message import Message


# seared field kind (by class name, walked up the MRO so a subclass such as
# a validating ``Str`` counts as its base) → the Python type its values hold.
# ``Enum`` is handled separately (its type is the field's own ``enum``).
# ``Path`` is left out deliberately: its values are ``PosixPath`` /
# ``WindowsPath`` (never exactly ``Path``), and ``str()`` already is its
# POSIX serialized form here.
_SCALAR_TYPES: dict[str, type] = {
    'Bool': bool,
    'Bytes': bytes,
    'Date': dt.date,
    'DateTime': dt.datetime,
    'Decimal': decimal.Decimal,
    'Float': float,
    'Int': int,
    'Str': str,
    'Time': dt.time,
    'TimeDelta': dt.timedelta,
    'UUID': uuid.UUID,
}


def _python_type(field: Any) -> type | None:
    """The Python type a scalar seared field holds, or ``None`` if it isn't one."""
    for klass in type(field).__mro__:
        name = klass.__name__
        if name == 'Enum':
            return getattr(field, 'enum', None)
        found = _SCALAR_TYPES.get(name)
        if found is not None:
            return found
    return None


def _slot_string(field: Any, value: Any) -> str:
    """Render one slot value as its key segment (see the module docstring)."""
    if field is None or type(value) is str:
        return str(value)
    if type(value) is not _python_type(field):
        return str(value)
    try:
        return str(field.serialize(value))
    except Exception:  # noqa: BLE001
        # Unreachable for a value of the field's own type in practice; the
        # fallback keeps the guarantee that this render never raises where
        # the old one didn't. Callers commonly wrap ``unretain`` in
        # ``suppress(Exception)``, where a new raise would silently drop a
        # tombstone. (``_reject_segment_split`` raises separately, on publish
        # renders, for a ``/`` that would send the tombstone nowhere: 0.4.0.)
        return str(value)


def _stringify_slots(
    cls: type[Message],
    template: Template,
    values: Mapping[str, Any],
) -> dict[str, Any]:
    """Render each of ``template``'s slots present in ``values``.

    Keys outside the template are dropped (both template methods ignored
    them anyway); absent slots stay absent and ``None`` stays ``None``, so
    the caller's template method decides what they mean.
    """
    specs = cls._field_specs()
    out: dict[str, Any] = {}
    for name in template.field_names:
        if name not in values:
            continue
        value = values[name]
        if value is None:
            out[name] = None
            continue
        spec = specs.get(name)
        out[name] = _slot_string(spec.field if spec is not None else None, value)
    return out


def _instance_slot_values(msg: Message, template: Template) -> dict[str, Any]:
    """The instance's non-``None`` values for ``template``'s field-backed slots."""
    specs = type(msg)._field_specs()  # noqa: SLF001
    out: dict[str, Any] = {}
    for name in template.field_names:
        if name not in specs:
            continue  # capture-only: no attribute to read, so it renders as missing
        value = getattr(msg, name, None)
        if value is not None:
            out[name] = value
    return out


def _reject_segment_split(cls: type[Message], template: Template, rendered: Mapping[str, Any]) -> None:
    """Raise ``TopicError`` if a single-segment slot's rendered value contains ``/`` (0.4.0).

    A ``/`` turns one segment into two, so the key matches no template and the
    message reaches nobody (a tombstone or a reply included). Publish renders
    call this; selector renders don't, since they embed ``*`` deliberately.
    """
    for name, value in rendered.items():
        if value is None or name == template._named_multi:  # noqa: SLF001
            continue
        if '/' in str(value):
            msg = (
                f'{cls.__name__}: slot {name!r} renders as {value!r}, which contains "/" and would '
                f'split its key segment (TOPIC {template.raw!r})'
            )
            raise TopicError(msg)


def _render_publish_key(cls: type[Message], template: Template, values: Mapping[str, Any]) -> str:
    """Render a key that a message will be *sent* on: canonical, and never split."""
    rendered = _stringify_slots(cls, template, values)
    _reject_segment_split(cls, template, rendered)
    return template.render(rendered)


def _render_instance_on(msg: Message, template: Template) -> str:
    """Render ``msg``'s concrete key on ``template``. A missing slot, or a split one, raises ``TopicError``."""
    return _render_publish_key(type(msg), template, _instance_slot_values(msg, template))


def _render_instance(msg: Message, topic: str | None) -> tuple[Template, str]:
    """Resolve the publish template (``topic=`` override or canonical) and render ``msg`` on it."""
    template = type(msg)._templates().resolve_publish_topic(topic)  # noqa: SLF001
    return template, _render_instance_on(msg, template)


def _render_slots(
    cls: type[Message],
    slots: Mapping[str, Any],
    *,
    topic: str | None,
    caller: str,
) -> str:
    """Render a concrete key from slot values alone, for the public ``Cls.concrete_topic``.

    Unlike a selector, a missing (or ``None``) slot raises ``TopicError``
    rather than widening to a wildcard: callers use this to name exactly one
    key. Keys in ``slots`` that aren't slots of the template are ignored.
    """
    template = cls._templates().resolve_publish_topic(topic)
    for name in template.field_names:
        if slots.get(name) is None:
            msg = f'{caller}: missing slot {name!r} for TOPIC {template.raw!r}'
            raise TopicError(msg)
    return _render_publish_key(cls, template, slots)


def _slot_wire_keys(cls: type[Message], template: Template) -> frozenset[str]:
    """The payload keys ``template``'s slots occupy: each slot field's wire key.

    A publish strips these from the body (the values live in the key). A
    slot field with a ``data_key`` is stored under that key in ``dump()``
    output, not under its attribute name.
    """
    specs = cls._field_specs()
    return frozenset(specs[name].wire if name in specs else name for name in template.field_names)


def _coerce_captures(cls: type[Message], captures: Mapping[str, str]) -> dict[str, Any]:
    """Coerce raw string captures through their declared fields, keyed by attribute name.

    Capture-only slots (no matching field) pass through as their raw string.
    A capture its field refuses raises the field's ``ValidationError``,
    located at its slot as a payload's error is at its key:
    ``Robot.bay: must be <= 12, got 13`` (0.4.4; before, it named no slot).
    """
    specs = cls._field_specs()
    out: dict[str, Any] = {}
    for name, raw in captures.items():
        spec = specs.get(name)
        if spec is None:
            out[name] = raw
            continue
        try:
            out[name] = spec.field.deserialize(raw, validate=True)
        except ValidationError as e:
            # Rebuilt through the public constructor, with the location a
            # seared load adds to a payload's error.
            raise ValidationError(e.reason, path=(name, *e.path), cls_name=cls.__name__) from None
    return out
