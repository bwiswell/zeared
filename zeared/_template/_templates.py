"""``Templates`` — the aggregate of canonical + extra topic templates declared by a single message class.

Sibling helper to ``_template.py`` inside the ``_template`` Pattern B
subdir.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import TopicError
from ._template import Template


@dataclass(frozen=True, slots=True)
class Templates:
    """A class's declared topic set: canonical (publish default) + extras.

    Each declared template is validated and parsed independently — there is
    no cross-template slot-set constraint. Captures from any template land
    on ``ZenohMeta.captures``; declared seared fields that share a name with
    a template slot are additionally coerced on the instance.
    """

    canonical: Template
    extras: tuple[Template, ...]
    all: tuple[Template, ...]
    field_names: frozenset
    multi_field_names: frozenset

    @classmethod
    def build(cls, canonical_src: str, extra_srcs: tuple[str, ...]) -> Templates:
        canonical = Template.parse(canonical_src)
        extras = tuple(Template.parse(src) for src in extra_srcs)
        all_tpls = (canonical, *extras)
        union = set(canonical.field_names)
        multi: set[str] = set()
        if canonical._named_multi is not None:  # noqa: SLF001
            multi.add(canonical._named_multi)  # noqa: SLF001
        for t in extras:
            union.update(t.field_names)
            if t._named_multi is not None:  # noqa: SLF001
                multi.add(t._named_multi)  # noqa: SLF001
        return cls(
            canonical=canonical,
            extras=extras,
            all=all_tpls,
            field_names=frozenset(union),
            multi_field_names=frozenset(multi),
        )

    def match(self, key_expr: str) -> tuple[Template, dict[str, str]] | None:
        """Return the first matching (template, captures) for an incoming key."""
        for t in self.all:
            captured = t.match(key_expr)
            if captured is not None:
                return t, captured
        return None

    def resolve_publish_topic(self, override: str | None) -> Template:
        """Restrictive publish override. Rejects unknown templates and subscribe-only ones."""
        if override is None:
            target = self.canonical
        else:
            target = None
            for t in self.all:
                if t.raw == override:
                    target = t
                    break
            if target is None:
                declared = [t.raw for t in self.all]
                msg = f'{override!r} is not a declared topic for this class; declared: {declared}'
                raise TopicError(msg)
        if not target.publishable:
            msg = f'TOPIC {target.raw!r} is subscribe-only (contains anonymous wildcards); cannot be used for publish'
            raise TopicError(msg)
        return target
