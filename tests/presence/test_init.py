"""Smoke tests for ``zeared/presence/__init__.py`` — verifies the
public surface of the namespace re-exports.
"""

from __future__ import annotations

import zeared as z
from zeared.presence import (
    PresenceToken,
    _registry,
    _registry_lock,
    _SessionPresence,
    clear_presence_state,
    get_presence,
)


class TestReExports:
    def test_classes_importable(self):
        assert _SessionPresence is not None
        assert PresenceToken is z.PresenceToken

    def test_helpers_callable(self):
        assert callable(get_presence)
        assert callable(clear_presence_state)

    def test_registry_is_a_dict(self):
        assert isinstance(_registry, dict)
        assert _registry_lock is not None

    def test_the_observer_is_gone(self):
        """0.4.0: subscribers watch liveliness themselves; nothing observes for them."""
        import zeared.presence as presence

        assert not hasattr(presence, 'get_observer')
        assert not hasattr(z, 'clear_observer')
