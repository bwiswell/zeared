"""Tests for ``zeared/_factories.py`` — the session-opening factories
(``peer`` / ``client`` / ``open``) plus the shared retry / config-build
helpers.

Most end-to-end coverage of ``peer`` / ``client`` lives in
``test_config.py`` (which exercises the retry loop and the
SessionConfig integration). This file targets the helpers introduced
in 0.0.18 (``_resolve_retry_knobs`` and ``_finalise_session``) and the
bench / config-build helpers (``_build_config_for_peer`` /
``_build_config_for_client``) directly.
"""
from __future__ import annotations

import pytest

import zeared as z
from zeared._factories import (
    _MISSING,
    _build_config_for_client,
    _build_config_for_peer,
    _finalise_session,
    _resolve_retry_knobs,
    client,
    open as z_open,
    peer,
)


class TestResolveRetryKnobs:
    """Pin: kwargs override config defaults; ``_MISSING`` falls through."""

    def test_no_config_no_kwargs_uses_defaults(self):
        retry, init_b, max_b, max_a = _resolve_retry_knobs(
            None, _MISSING, _MISSING, _MISSING, _MISSING,
        )
        assert retry is False
        assert init_b == 0.1
        assert max_b == 30.0
        assert max_a is None

    def test_config_provides_base(self):
        cfg = z.SessionConfig(
            mode=z.Mode.PEER, retry=True, initial_backoff=0.5,
            max_backoff=60.0, max_attempts=10,
        )
        retry, init_b, max_b, max_a = _resolve_retry_knobs(
            cfg, _MISSING, _MISSING, _MISSING, _MISSING,
        )
        assert retry is True
        assert init_b == 0.5
        assert max_b == 60.0
        assert max_a == 10

    def test_kwargs_override_config(self):
        cfg = z.SessionConfig(
            mode=z.Mode.PEER, retry=True, initial_backoff=0.5,
            max_backoff=60.0, max_attempts=10,
        )
        retry, init_b, max_b, max_a = _resolve_retry_knobs(
            cfg, False, 0.01, 1.0, 3,
        )
        assert retry is False
        assert init_b == 0.01
        assert max_b == 1.0
        assert max_a == 3

    def test_partial_kwargs_layer_over_config(self):
        cfg = z.SessionConfig(
            mode=z.Mode.PEER, retry=True, initial_backoff=0.5,
            max_backoff=60.0, max_attempts=10,
        )
        # Only ``max_attempts`` overridden — others fall through to config.
        retry, init_b, max_b, max_a = _resolve_retry_knobs(
            cfg, _MISSING, _MISSING, _MISSING, 99,
        )
        assert retry is True
        assert init_b == 0.5
        assert max_b == 60.0
        assert max_a == 99


class TestFinaliseSessionRawSession:
    """Pin: raw sessions reject ``retention_ttl`` (no place to stash it).
    Managed sessions accept it."""

    def test_raw_returns_unchanged_without_retention_ttl(self):
        sess = peer()  # auto_reconnect=False default → raw session
        try:
            assert sess is not None
        finally:
            sess.close()

    def test_raw_with_retention_ttl_raises(self):
        with pytest.raises(TypeError, match='requires auto_reconnect=True'):
            peer(retention_ttl=10.0)


class TestBuildConfigForPeer:
    """Pin: factory injects timestamping by default; respects user-provided config."""

    def test_default_injects_timestamping(self):
        cfg = _build_config_for_peer(None, None, None, timestamping=True)
        val = cfg.get_json('timestamping/enabled').lower()
        assert 'true' in val

    def test_timestamping_false_skips(self):
        cfg = _build_config_for_peer(None, None, None, timestamping=False)
        val = cfg.get_json('timestamping/enabled').lower()
        assert 'true' not in val

    def test_user_config_passthrough(self):
        import zenoh
        user_cfg = zenoh.Config()
        out = _build_config_for_peer(None, None, user_cfg, timestamping=True)
        assert out is user_cfg
        # User didn't enable timestamping — we don't either.
        val = out.get_json('timestamping/enabled').lower()
        assert 'true' not in val


class TestBuildConfigForClient:
    def test_default_injects_timestamping(self):
        cfg = _build_config_for_client(['tcp/x:7447'], None, timestamping=True)
        val = cfg.get_json('timestamping/enabled').lower()
        assert 'true' in val


class TestOpenDispatcher:
    """Pin: ``open(SessionConfig)`` dispatches on ``cfg.mode``."""

    def test_open_peer_mode(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER)
        sess = z_open(cfg)
        try:
            assert sess is not None
        finally:
            sess.close()

    def test_open_unrecognised_mode_raises(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER)
        # Sneak in a bogus mode value.
        object.__setattr__(cfg, 'mode', 'NOT_A_REAL_MODE')
        with pytest.raises(ValueError, match='unrecognised'):
            z_open(cfg)


class TestClientRequiresEndpoint:
    def test_client_with_no_endpoints_raises(self):
        with pytest.raises(TypeError, match='router='):
            client()

    def test_client_with_empty_config_raises(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT)
        with pytest.raises(TypeError, match='router='):
            client(config=cfg)
