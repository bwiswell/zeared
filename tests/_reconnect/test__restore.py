"""Smoke tests for ``zeared/_reconnect/_restore.py`` — the post-reopen
walks (``_restore_retention`` / ``_restore_subscribers`` /
``_restore_wills``) plus the cancellable ``_open_with_backoff``.

End-to-end reconnect coverage lives in ``test__reconnect.py``. This
file confirms the helpers are importable and the cancel-aborts-cleanly
contract.
"""
from __future__ import annotations

import threading

import pytest

from zeared._reconnect._restore import (
    _ReconnectAborted,
    _open_with_backoff,
    _restore_retention,
    _restore_subscribers,
    _restore_wills,
)


class TestPublicSurface:
    def test_helpers_callable(self):
        assert callable(_open_with_backoff)
        assert callable(_restore_retention)
        assert callable(_restore_subscribers)
        assert callable(_restore_wills)

    def test_aborted_is_exception(self):
        assert issubclass(_ReconnectAborted, Exception)


class TestOpenWithBackoffCancel:
    """Pin: the cancel Event aborts the backoff loop cleanly via
    ``_ReconnectAborted``."""

    def test_cancel_during_backoff_aborts(self):
        cancel = threading.Event()
        cancel.set()   # already cancelled

        def open_fn():
            raise RuntimeError('always fails')

        with pytest.raises(_ReconnectAborted):
            _open_with_backoff(
                open_fn, initial=0.01, cap=0.02, max_attempts=None,
                label='test', cancel=cancel,
            )

    def test_max_attempts_propagates_last_error(self):
        cancel = threading.Event()
        attempts = [0]

        def open_fn():
            attempts[0] += 1
            raise RuntimeError(f'attempt {attempts[0]}')

        # max_attempts=2 → raise after 2 failures.
        with pytest.raises(RuntimeError, match='attempt 2'):
            _open_with_backoff(
                open_fn, initial=0.001, cap=0.002, max_attempts=2,
                label='test', cancel=cancel,
            )
        assert attempts[0] == 2
