"""Smoke tests for ``zeared/presence/_presence_observer.py`` — the
``_PresenceObserver`` class and the per-session observer registry.

End-to-end coverage of liveliness-driven will synthesis lives in
``test_presence.py``; this file confirms the public surface.
"""
from __future__ import annotations

from zeared.presence._presence_observer import (
    Dispatcher,
    _PresenceObserver,
    _observer_lock,
    _observer_registry,
    clear_observer,
    get_observer,
)


class TestPresenceObserver:
    def test_class_importable(self):
        assert _PresenceObserver is not None
        assert hasattr(_PresenceObserver, '__slots__')

    def test_observer_registry_is_dict(self):
        assert isinstance(_observer_registry, dict)

    def test_observer_lock_present(self):
        assert hasattr(_observer_lock, 'acquire')


class TestObserverRegistryHelpers:
    def test_get_observer_callable(self):
        assert callable(get_observer)

    def test_clear_observer_callable(self):
        assert callable(clear_observer)

    def test_clear_with_no_session_clears_all(self):
        # Smoke — registry may have entries from other tests but
        # ``clear_observer()`` shouldn't raise.
        clear_observer()


class TestDispatcherType:
    def test_dispatcher_is_callable_alias(self):
        # ``Callable[[_SynthesizedSample], bool]`` — type alias only.
        assert Dispatcher is not None
