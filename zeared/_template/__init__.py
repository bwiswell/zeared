"""``_template`` — TOPIC format-string parser + per-class topic-set holder.

Pattern B subdir. ``_template.py`` holds the per-string ``Template``
parser; ``_templates.py`` holds the aggregate ``Templates`` collection
(canonical + extras) that a message class declares.

Public surface unchanged: callers continue to write
``from zeared._template import Template, Templates``.
"""
from ._template import Template
from ._templates import Templates


__all__ = ['Template', 'Templates']
