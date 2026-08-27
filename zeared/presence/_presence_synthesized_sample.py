"""Synthesised sample shim — minimal stand-in for ``zenoh.Sample`` when
firing a will locally.

Lives in its own file (per one-class-per-file) even though it's small;
both ``_presence_observer.py`` and external callers (subscriber dispatch,
meta extraction) read its attribute set.
"""
from __future__ import annotations

from typing import Optional

import zenoh

from ..meta import build_attachment_schema


class _SynthesizedSample:
    """Minimal stand-in for ``zenoh.Sample`` when firing a will locally.

    Exposes the attributes ``zeared.subscriber.dispatch`` and
    ``meta.from_sample`` read. No real Zenoh types involved.
    """
    __slots__ = (
        'key_expr', 'payload', 'kind', 'encoding',
        'timestamp', 'source_info', 'attachment',
    )

    def __init__(
        self,
        key_expr: str,
        payload: bytes,
        encoding_mime: str,
        source_zid: str,
        schema: Optional[str] = None,
    ):
        self.key_expr = key_expr
        self.payload = payload
        self.kind = zenoh.SampleKind.PUT
        self.encoding = encoding_mime
        self.timestamp = None
        self.source_info = source_zid
        # Re-stamp the registering class's SCHEMA as the attachment a live
        # publish would carry, so a schema-checking subscriber decodes the
        # will instead of dropping it as a mismatch. None when unset.
        self.attachment = build_attachment_schema(schema)
