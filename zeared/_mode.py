"""Session-mode enum.

Replaces the prior plain ``str`` ``SessionConfig.mode`` so invalid values
are caught at config-load time rather than at session-open time.
"""
from __future__ import annotations

import enum


class Mode(enum.Enum):
    """Zenoh session mode supported by zeared factories."""
    PEER = 'peer'
    CLIENT = 'client'
