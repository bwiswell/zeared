"""zeared doc generation over seared's pipeline.

zeared doc generation — reuses seared's discovery / pathing / index / check pipeline
with a Message-aware ``render_one``.

A ``Message`` subclass gets the wire-aware page (topic + contract preamble +
fields); a plain ``@zeared`` class (e.g. a config dataclass, or a ``REQUEST``
payload) falls back to seared's core renderer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from seared.doc import document as _seared_document
from seared.doc import introspect as _seared_introspect
from seared.doc.generate import build_docs as _seared_build_docs
from seared.doc.generate import main as _seared_main

from .introspect import MessageDoc, introspect_message, is_message_class
from .render import LinkFor, _default_link, render_message

if TYPE_CHECKING:
    from seared import Seared
from seared.doc import SchemaDoc


def render_one(cls: type[Seared], link_for: LinkFor) -> str:
    """Render one class — the wire-aware page for a Message, else seared's."""
    if is_message_class(cls):
        return render_message(cls, link_for)
    return _seared_document(cls, link_for=link_for)


def document(cls: type[Seared], *, link_for: LinkFor = _default_link) -> str:
    """Markdown for one class — wire-aware for a Message, core for anything else."""
    return render_one(cls, link_for)


def introspect(cls: type[Seared]) -> MessageDoc | SchemaDoc:
    """``MessageDoc`` for a Message, else seared's ``SchemaDoc``."""
    return introspect_message(cls) if is_message_class(cls) else _seared_introspect(cls)


def build_docs(target: str) -> dict[str, str]:
    """Build the full docs mapping for ``target`` (path → Markdown)."""
    return _seared_build_docs(target, render_one=render_one)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for ``python -m zeared.doc``."""
    return _seared_main(argv, render_one=render_one, prog='python -m zeared.doc')
