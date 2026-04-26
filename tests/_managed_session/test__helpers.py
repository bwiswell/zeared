"""Tests for ``zeared/_managed_session/_helpers.py`` — the WeakSet
registry, declare-handle RuntimeWarning emitter, and the small
liveness / raw-resolution utilities.
"""
from __future__ import annotations

import warnings
import weakref

import pytest

from zeared._managed_session._helpers import (
    _DECLARE_HANDLE_WARNING,
    _DEFAULT_PROBE_INTERVAL,
    _is_dead,
    _managed_sessions,
    _warn_declare_handle,
    resolve_raw,
)


class TestPublicSurface:
    def test_managed_sessions_is_weakset(self):
        assert isinstance(_managed_sessions, weakref.WeakSet)

    def test_default_probe_interval(self):
        assert _DEFAULT_PROBE_INTERVAL == 10.0

    def test_declare_handle_warning_template(self):
        assert '{method}' in _DECLARE_HANDLE_WARNING


class TestWarnDeclareHandle:
    def test_emits_runtime_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            _warn_declare_handle('declare_publisher')
        assert len(caught) == 1
        assert issubclass(caught[0].category, RuntimeWarning)
        assert 'declare_publisher' in str(caught[0].message)


class TestResolveRaw:
    def test_passes_through_raw_session(self):
        # A non-ManagedSession object is returned as-is.
        class _Fake:
            pass
        f = _Fake()
        assert resolve_raw(f) is f


class TestIsDead:
    def test_returns_true_on_exception(self):
        class _BoomSession:
            def is_closed(self):
                raise RuntimeError('boom')
        assert _is_dead(_BoomSession()) is True

    def test_returns_false_when_not_closed(self):
        class _AliveSession:
            def is_closed(self):
                return False
        assert _is_dead(_AliveSession()) is False

    def test_returns_true_when_is_closed_says_yes(self):
        class _DeadSession:
            def is_closed(self):
                return True
        assert _is_dead(_DeadSession()) is True
