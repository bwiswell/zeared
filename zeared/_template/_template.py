"""Template parsing — the per-class TOPIC format-string compiler.

Primary file of the ``_template`` Pattern B subdir. Holds the
``Template`` dataclass plus the module-level regex constants and the
namespace-reservation guard. The aggregate ``Templates`` collection
(canonical + extras) lives in the sibling ``_templates.py``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from string import Formatter
from typing import Any

from ..errors import TopicError

_NAME_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Reserved namespace for zeared internals (liveliness tokens at
# `__zeared/alive/<zid>`, will envelopes at `__zeared/will/<zid>/<slug>`,
# etc.). User templates whose first literal segment is `__zeared` would
# collide with internal routing — silent stomping. Reject at parse time.
# Exemption: the unmodified anonymous catch-all `__zeared/**` (and the
# universal `**`) — diagnostic tooling that subscribes to everything is
# explicit intent. Named multi-segment under the prefix
# (`__zeared/{tail**}`) is NOT exempt — it's a structural claim on
# internal routing and should be opt-in via a future API, not implicit.
_RESERVED_PREFIX = '__zeared'

# Trailing named multi-segment slot: ``.../{name**}`` at end of template, where
# ``name`` is a Python-style identifier. Anchored with ``$`` so non-trailing
# occurrences fall through to the body parser (which then rejects them).
_NAMED_MULTI_TRAILING_RE = re.compile(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\*\*\}$')


@dataclass(frozen=True, slots=True)
class Template:
    """Parsed representation of a format-string TOPIC.

    Grammar:
      - ``{name}``   — single-segment named capture (any position).
      - ``{name**}`` — multi-segment named capture; trailing only. Captures
                       a slash-containing string. Publish-capable: the
                       caller supplies the rendered tail verbatim.
      - ``**``       — anonymous trailing multi-segment wildcard;
                       subscribe-only.

    Anonymous non-trailing ``**`` and named non-trailing multi-segment
    (``{name**}/literal``) are both rejected at parse time.

    Attributes:
        raw: original template string.
        field_names: ordered tuple of all capture slot names (single + multi).
        wildcard: Zenoh key expression for subscribe-side use.
        publishable: ``False`` only if the template contains anonymous
                     ``*`` or ``**`` segments. Named ``{name}`` and
                     ``{name**}`` are both publishable.
        _regex: compiled regex with named captures for each slot.
        _named_multi: name of the trailing ``{name**}`` slot, if any.
    """

    raw: str
    field_names: tuple[str, ...]
    wildcard: str
    publishable: bool
    _regex: re.Pattern
    _named_multi: str | None

    @classmethod
    def parse(cls, template: str) -> Template:  # noqa: C901, PLR0912, PLR0915
        if not isinstance(template, str) or not template:
            msg = f'TOPIC must be a non-empty string, got {template!r}'
            raise TopicError(msg)

        cls._validate_namespace_reservation(template)

        # Detect trailing multi-segment marker — anonymous ``**`` or
        # named ``{name**}``. Both consume the trailing segment; the body
        # is everything before.
        has_anon_multi = False
        named_multi: str | None = None
        body = template

        if template == '**':
            has_anon_multi = True
            body = ''
        elif template.endswith('/**'):
            has_anon_multi = True
            body = template[:-3]
        else:
            m = _NAMED_MULTI_TRAILING_RE.search(template)
            if m is not None and m.end() == len(template):
                named_multi = m.group(1)
                start = m.start()
                # Strip preceding '/' if present so body has no trailing slash.
                body = template[: start - 1] if start > 0 and template[start - 1] == '/' else template[:start]

        # Reject any other ``**`` in remaining body. Catches:
        #   - anonymous non-trailing ``**`` (``a/**/c``)
        #   - named non-trailing multi (``a/{x**}/b``) — the trailing-RE
        #     above only matches at end-of-string, so a non-trailing
        #     ``{x**}`` lands here.
        if '**' in body:
            msg = f'TOPIC template {template!r}: ** may only appear as the final path segment'
            raise TopicError(msg)

        literals: list[str] = []
        single_fields: list[str] = []
        if body:
            try:
                parts = list(Formatter().parse(body))
            except ValueError as e:
                msg = f'invalid TOPIC template {template!r}: {e}'
                raise TopicError(msg) from e

            for literal, field_name, format_spec, conversion in parts:
                literals.append(literal)
                if field_name is None:
                    continue
                if format_spec or conversion:
                    msg = (
                        f'TOPIC template {template!r}: format specs / '
                        f'conversions are not supported in '
                        f'{{{field_name}!{conversion or ""}:{format_spec or ""}}}'
                    )
                    raise TopicError(msg)
                if not _NAME_RE.match(field_name):
                    msg = f'TOPIC template {template!r}: invalid field name {field_name!r}'
                    raise TopicError(msg)
                if field_name in single_fields:
                    msg = f'TOPIC template {template!r}: duplicate field {field_name!r}'
                    raise TopicError(msg)
                single_fields.append(field_name)
        else:
            literals.append('')

        # Validate the named-multi name itself (collisions / identifier shape).
        if named_multi is not None:
            if not _NAME_RE.match(named_multi):
                msg = f'TOPIC template {template!r}: invalid field name {named_multi!r}'
                raise TopicError(msg)
            if named_multi in single_fields:
                msg = f'TOPIC template {template!r}: duplicate field {named_multi!r}'
                raise TopicError(msg)

        has_multi_marker = has_anon_multi or (named_multi is not None)
        # Anonymous ``**`` makes the template subscribe-only. Named
        # ``{name**}`` is publish-capable.
        publishable = not has_anon_multi

        wildcard = cls._render_wildcard(literals, single_fields, has_multi_marker)
        regex = cls._build_regex(literals, single_fields, has_anon_multi, named_multi)
        all_names: tuple[str, ...] = (*single_fields, named_multi) if named_multi else tuple(single_fields)
        return cls(
            raw=template,
            field_names=all_names,
            wildcard=wildcard,
            publishable=publishable,
            _regex=regex,
            _named_multi=named_multi,
        )

    @staticmethod
    def _validate_namespace_reservation(template: str) -> None:
        """Reject templates whose first literal segment is ``__zeared``.

        Reject templates whose first literal segment is ``__zeared``, which collides
        with internal liveliness / will / observer keys.

        Exemption: the anonymous catch-all forms ``**`` (universal) and
        ``__zeared/**`` (diagnostic — "subscribe to all internal traffic
        explicitly") pass. Named multi-segment captures under the prefix
        (``__zeared/{tail**}``) and any single-segment captures
        (``__zeared/alive/{x}``) are rejected.
        """
        # Universal catch-all — exempt.
        if template == '**':
            return
        # Anonymous trailing ``**`` directly after the reserved prefix —
        # exempt as the diagnostic catch-all.
        if template == f'{_RESERVED_PREFIX}/**':
            return
        # First-segment check.
        first_seg = template.split('/', 1)[0]
        # Slot at first position (e.g. `{tenant}/__zeared/x`) — first
        # segment is a runtime value, can't statically validate.
        if first_seg.startswith('{'):
            return
        if first_seg == _RESERVED_PREFIX:
            msg = (
                f'TOPIC template {template!r}: the {_RESERVED_PREFIX!r} '
                f'prefix is reserved for internal zeared use (liveliness '
                f'tokens, will envelopes, etc.); pick a different '
                f'top-level segment. The diagnostic catch-all '
                f'{_RESERVED_PREFIX!r}/** is the only exempted form.'
            )
            raise TopicError(msg)

    @staticmethod
    def _render_wildcard(
        literals: list[str],
        single_fields: list[str],
        has_multi_marker: bool,  # noqa: FBT001
    ) -> str:
        out: list[str] = []
        for i, lit in enumerate(literals):
            out.append(lit)
            if i < len(single_fields):
                out.append('*')
        wildcard = ''.join(out)
        if has_multi_marker:
            wildcard = f'{wildcard}/**' if wildcard else '**'
        return wildcard

    @staticmethod
    def _build_regex(
        literals: list[str],
        single_fields: list[str],
        has_anon_multi: bool,  # noqa: FBT001
        named_multi: str | None,
    ) -> re.Pattern:
        parts: list[str] = []
        for i, lit in enumerate(literals):
            parts.append(re.escape(lit))
            if i < len(single_fields):
                parts.append(f'(?P<{single_fields[i]}>[^/]+)')
        body = ''.join(parts)
        if has_anon_multi:
            body = (body + r'/.+') if body else r'.+'
        elif named_multi is not None:
            suffix = f'(?P<{named_multi}>.+)'
            body = (body + '/' + suffix) if body else suffix
        return re.compile('^' + body + '$')

    def render(self, values: dict[str, Any]) -> str:
        """Render the template against the supplied field values.

        Format the template; raises if a field is missing, the template is not
        publishable, or a multi-segment field is empty.
        """
        if not self.publishable:
            msg = f'TOPIC {self.raw!r} is subscribe-only (contains anonymous wildcards); cannot be rendered for publish'
            raise TopicError(msg)
        # Multi-segment slot: empty value would render `<prefix>/`, which
        # subscribers (regex `(?P<x>.+)`) wouldn't match. Fail loudly.
        if self._named_multi is not None:
            v = values.get(self._named_multi)
            if v == '':
                msg = f'TOPIC {self.raw!r}: multi-segment field {self._named_multi!r} cannot be empty'
                raise TopicError(msg)
        # Python's str.format treats ``{x**}`` as a field name lookup of
        # ``x**`` (which raises). Rewrite the slot to ``{x}`` for the
        # rendering call only — slashes in the value are passed through
        # verbatim, exactly the contract we want.
        fmt = self.raw
        if self._named_multi is not None:
            fmt = fmt.replace(
                f'{{{self._named_multi}**}}',
                f'{{{self._named_multi}}}',
            )
        try:
            return fmt.format(**values)
        except KeyError as e:
            msg = f'TOPIC {self.raw!r}: missing field {e.args[0]!r} when rendering'
            raise TopicError(msg) from e

    def render_selector(self, partial: dict[str, Any]) -> str:
        """Render a query selector key from a partial slot mapping.

        Provided slots are substituted; slots that are missing (absent or
        ``None``) become ``*`` (single-segment) or ``**`` (a trailing
        ``{name**}`` slot), so ``Cls.query()`` with no key fields queries
        the whole template wildcard and ``Cls.query(epc='E280*')`` narrows
        to matching keys. Unlike :meth:`render`, wildcard values (a ``*``
        the caller embeds) pass through verbatim — that is the point.

        Subscribe-only templates (anonymous ``**``) can't be rendered from
        slots; they fall back to the plain subscribe :attr:`wildcard`.
        """
        if not self.publishable:
            return self.wildcard
        fmt = self.raw
        values: dict[str, str] = {}
        for name in self.field_names:
            v = partial.get(name)
            if name == self._named_multi:
                # Rewrite ``{name**}`` → ``{name}`` for the format call (as
                # in ``render``); a missing multi slot widens to ``**``.
                fmt = fmt.replace(f'{{{name}**}}', f'{{{name}}}')
                values[name] = '**' if v is None else str(v)
            else:
                values[name] = '*' if v is None else str(v)
        try:
            return fmt.format(**values)
        except KeyError as e:
            msg = f'TOPIC {self.raw!r}: unknown field {e.args[0]!r} in selector'
            raise TopicError(msg) from e

    def match(self, key_expr: str) -> dict[str, str] | None:
        """Match an incoming key expression; return ``{field: str}`` or None."""
        m = self._regex.match(key_expr)
        if m is None:
            return None
        return m.groupdict()
