"""Smoke tests for ``zeared/_template/__init__.py`` — the namespace
re-exports for the templating Pattern B subdir."""
from __future__ import annotations

from zeared._template import Template, Templates


class TestReExports:
    def test_template_class(self):
        assert Template is not None
        assert isinstance(Template, type)

    def test_templates_class(self):
        assert Templates is not None
        assert isinstance(Templates, type)
