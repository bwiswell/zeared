"""Smoke tests for ``zeared/presence/__init__.py`` — verifies the
public surface of the namespace re-exports.
"""
from __future__ import annotations

import zeared as z
from zeared.presence import (
    ALIVE_PREFIX,
    Dispatcher,
    WILL_PREFIX,
    _PresenceObserver,
    _SessionPresence,
    _SynthesizedSample,
    _WillEnvelope,
    _envelope_encoding,
    _observer_registry,
    _registry,
    _registry_lock,
    _resolve_gc_interval,
    _slug,
    clear_observer,
    clear_presence_state,
    get_observer,
    get_presence,
)


class TestReExports:
    def test_constants_present(self):
        assert ALIVE_PREFIX == '__zeared/alive'
        assert WILL_PREFIX == '__zeared/will'

    def test_classes_importable(self):
        assert _SessionPresence is not None
        assert _PresenceObserver is not None
        assert _SynthesizedSample is not None
        assert _WillEnvelope is not None

    def test_helpers_callable(self):
        assert callable(get_presence)
        assert callable(clear_presence_state)
        assert callable(get_observer)
        assert callable(clear_observer)
        assert callable(_envelope_encoding)
        assert callable(_resolve_gc_interval)
        assert callable(_slug)

    def test_registries_are_dicts(self):
        assert isinstance(_registry, dict)
        assert isinstance(_observer_registry, dict)
        assert _registry_lock is not None

    def test_dispatcher_is_callable_alias(self):
        # Dispatcher is a Callable type alias.
        assert Dispatcher is not None
