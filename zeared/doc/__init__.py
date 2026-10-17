"""Wire-aware schema documentation for ``@zeared`` Message classes.

Layers zeared's topic / schema / retention / request contract on top of
``seared.doc``'s field/enum/variant rendering.

- ``introspect(cls)`` → ``MessageDoc`` (Message) or seared ``SchemaDoc``.
- ``document(cls)`` → a Markdown page (wire-aware for Messages).
- ``build_docs(target)`` → ``{path: markdown}`` for a whole module/package.
- CLI: ``python -m zeared.doc <module-or-package> [-o docs] [--check]``.

TypeScript types for browser consumers (types only — the mesh→websocket
bridge is the consumer's to build):

- ``generate_ts(target)`` → a single ``.ts`` module of ``export interface`` /
  ``export type`` declarations for every message + referenced payload.
- CLI: ``python -m zeared.doc.typescript <module-or-package> [-o out.ts] [--check]``.

Requires ``seared >= 0.2.2``, which is where ``seared.doc`` — the
discovery / reference-closure / index / ``--check`` pipeline this module
layers a Message-aware renderer onto — was introduced.
"""

from typing import TYPE_CHECKING, Any

from .generate import build_docs, document, introspect, main, render_one
from .introspect import MessageDoc, SlotDoc, introspect_message, is_message_class
from .render import render_message, render_topic, render_wire

if TYPE_CHECKING:
    from .typescript import emit as emit_ts
    from .typescript import generate as generate_ts
    from .typescript import render_interface, ts_type

#: The TypeScript names, re-exported lazily (0.4.2): importing ``.typescript``
#: here put it in ``sys.modules`` before ``python -m zeared.doc.typescript``
#: ran it, which runpy warns about.
_TYPESCRIPT = {
    'emit_ts': 'emit',
    'generate_ts': 'generate',
    'render_interface': 'render_interface',
    'ts_type': 'ts_type',
}


def __getattr__(name: str) -> Any:
    """Resolve a TypeScript name on first use (PEP 562)."""
    if name in _TYPESCRIPT:
        from . import typescript

        return getattr(typescript, _TYPESCRIPT[name])
    msg = f'module {__name__!r} has no attribute {name!r}'
    raise AttributeError(msg)


__all__ = [
    'MessageDoc',
    'SlotDoc',
    'build_docs',
    'document',
    'emit_ts',
    'generate_ts',
    'introspect',
    'introspect_message',
    'is_message_class',
    'main',
    'render_interface',
    'render_message',
    'render_one',
    'render_topic',
    'render_wire',
    'ts_type',
]
