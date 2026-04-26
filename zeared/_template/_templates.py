"""``Templates`` — the aggregate of canonical + extra topic templates
declared by a single message class.

Sibling helper to ``_template.py`` inside the ``_template`` Pattern B
subdir.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

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
    extras: Tuple[Template, ...]
    all: Tuple[Template, ...]
    field_names: frozenset
    multi_field_names: frozenset

    @classmethod
    def build(cls, canonical_src: str, extra_srcs: Tuple[str, ...]) -> 'Templates':
        canonical = Template.parse(canonical_src)
        extras = tuple(Template.parse(src) for src in extra_srcs)
        all_tpls = (canonical, *extras)
        union = set(canonical.field_names)
        multi: set[str] = set()
        if canonical._named_multi is not None:
            multi.add(canonical._named_multi)
        for t in extras:
            union.update(t.field_names)
            if t._named_multi is not None:
                multi.add(t._named_multi)
        return cls(
            canonical=canonical,
            extras=extras,
            all=all_tpls,
            field_names=frozenset(union),
            multi_field_names=frozenset(multi),
        )

    def match(self, key_expr: str) -> Optional[tuple[Template, dict[str, str]]]:
        """Return the first matching (template, captures) for an incoming key."""
        for t in self.all:
            captured = t.match(key_expr)
            if captured is not None:
                return t, captured
        return None

    def resolve_publish_topic(self, override: Optional[str]) -> Template:
        """Restrictive publish override. Rejects unknown templates and
        subscribe-only ones."""
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
                raise TopicError(
                    f'{override!r} is not a declared topic for this class; '
                    f'declared: {declared}'
                )
        if not target.publishable:
            raise TopicError(
                f'TOPIC {target.raw!r} is subscribe-only (contains anonymous '
                f'wildcards); cannot be used for publish'
            )
        return target
