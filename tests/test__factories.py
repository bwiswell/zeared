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
    _resolve_retry_knobs,
    _resolve_session_knobs,
    _resolve_zenoh_config,
    client,
    peer,
)
from zeared._factories import (
    open as z_open,
)


class TestResolveRetryKnobs:
    """Pin: kwargs override config defaults; ``_MISSING`` falls through."""

    def test_no_config_no_kwargs_uses_defaults(self):
        retry, init_b, max_b, max_a = _resolve_retry_knobs(
            None,
            _MISSING,
            _MISSING,
            _MISSING,
            _MISSING,
        )
        assert retry is False
        assert init_b == 0.1
        assert max_b == 30.0
        assert max_a is None

    def test_config_provides_base(self):
        cfg = z.SessionConfig(
            mode=z.Mode.PEER,
            retry=True,
            initial_backoff=0.5,
            max_backoff=60.0,
            max_attempts=10,
        )
        retry, init_b, max_b, max_a = _resolve_retry_knobs(
            cfg,
            _MISSING,
            _MISSING,
            _MISSING,
            _MISSING,
        )
        assert retry is True
        assert init_b == 0.5
        assert max_b == 60.0
        assert max_a == 10

    def test_kwargs_override_config(self):
        cfg = z.SessionConfig(
            mode=z.Mode.PEER,
            retry=True,
            initial_backoff=0.5,
            max_backoff=60.0,
            max_attempts=10,
        )
        retry, init_b, max_b, max_a = _resolve_retry_knobs(
            cfg,
            False,  # noqa: FBT003
            0.01,
            1.0,
            3,
        )
        assert retry is False
        assert init_b == 0.01
        assert max_b == 1.0
        assert max_a == 3

    def test_partial_kwargs_layer_over_config(self):
        cfg = z.SessionConfig(
            mode=z.Mode.PEER,
            retry=True,
            initial_backoff=0.5,
            max_backoff=60.0,
            max_attempts=10,
        )
        # Only ``max_attempts`` overridden — others fall through to config.
        retry, init_b, max_b, max_a = _resolve_retry_knobs(
            cfg,
            _MISSING,
            _MISSING,
            _MISSING,
            99,
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

    def test_shared_memory_off_by_default(self):
        cfg = _build_config_for_peer(None, None, None, timestamping=False)
        assert cfg.get_json('transport/shared_memory/enabled') == 'false'

    def test_user_config_passthrough(self):
        import zenoh

        user_cfg = zenoh.Config()
        out = _build_config_for_peer(None, None, user_cfg, timestamping=True)
        assert out is user_cfg
        # User didn't enable timestamping — we don't either.
        val = out.get_json('timestamping/enabled').lower()
        assert 'true' not in val
        # Nor do we turn shared memory off: a caller's own config is taken as given.
        assert out.get_json('transport/shared_memory/enabled') == 'true'


class TestBuildConfigForClient:
    def test_default_injects_timestamping(self):
        cfg = _build_config_for_client(['tcp/x:7447'], None, timestamping=True)
        val = cfg.get_json('timestamping/enabled').lower()
        assert 'true' in val

    def test_shared_memory_off_by_default(self):
        cfg = _build_config_for_client(['tcp/x:7447'], None, timestamping=True)
        assert cfg.get_json('transport/shared_memory/enabled') == 'false'


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


class TestClientEndpointsFromConfig:
    """Pin (0.3.9): a client with no ``router`` / ``connect`` opens on the connect endpoints its
    Zenoh config already carries (a raw ``zenoh_config``, a ``zenoh_config_file`` or
    ``zenoh_overrides``). Before 0.3.9 it raised ``TypeError`` regardless.
    """

    @pytest.fixture
    def hub_ep(self):
        import socket

        s = socket.socket()
        s.bind(('127.0.0.1', 0))
        ep = f'tcp/127.0.0.1:{s.getsockname()[1]}'
        s.close()
        h = z.hub(listen=[ep])
        try:
            yield ep
        finally:
            h.close()

    def test_opens_on_overrides_endpoints(self, hub_ep):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT, zenoh_overrides={'connect/endpoints': [hub_ep]})
        sess = client(config=cfg)
        try:
            assert sess.zid() is not None
        finally:
            sess.close()

    def test_opens_on_a_raw_configs_endpoints(self, hub_ep):
        import zenoh

        raw = zenoh.Config()
        raw.insert_json5('mode', '"client"')
        raw.insert_json5('connect/endpoints', f'["{hub_ep}"]')
        sess = client(zenoh_config=raw)
        try:
            assert sess.zid() is not None
        finally:
            sess.close()


class TestEndpointRule:
    """Pin (0.3.9): one rule in every mode. An endpoint field is written over the Zenoh config
    when it is set; when it isn't, the endpoints the config already names stand."""

    OVERRIDES = {'connect/endpoints': ['tcp/override:1']}  # noqa: RUF012

    def test_client_builder_keeps_the_configs_endpoints(self):
        import zenoh

        raw = zenoh.Config()
        raw.insert_json5('connect/endpoints', '["tcp/own:1"]')
        assert _build_config_for_client(None, raw).get_json('connect/endpoints') == '["tcp/own:1"]'

    def test_client_router_field_beats_an_override(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT, router='tcp/field:1', zenoh_overrides=self.OVERRIDES)
        built = _build_config_for_client(['tcp/field:1'], _resolve_zenoh_config(cfg, None))
        assert built.get_json('connect/endpoints') == '["tcp/field:1"]'

    def test_peer_connect_field_beats_an_override(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER, connect=['tcp/field:1'], zenoh_overrides=self.OVERRIDES)
        built = _build_config_for_peer(['tcp/field:1'], None, _resolve_zenoh_config(cfg, None))
        assert built.get_json('connect/endpoints') == '["tcp/field:1"]'

    def test_peer_override_stands_without_a_field(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER, zenoh_overrides=self.OVERRIDES)
        built = _build_config_for_peer(None, None, _resolve_zenoh_config(cfg, None))
        assert built.get_json('connect/endpoints') == '["tcp/override:1"]'

    def test_client_with_no_endpoints_anywhere_still_raises(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT, zenoh_overrides={'scouting/multicast/enabled': False})
        with pytest.raises(TypeError, match='zenoh_config_file'):
            client(config=cfg)


class TestSessionKnobs:
    """0.4.0: the reconnect knobs layer like the retry ones, so a config can set
    them: an explicit keyword beats ``config=``, which beats the defaults."""

    def test_defaults(self):
        assert _resolve_session_knobs(None, _MISSING, _MISSING, _MISSING, _MISSING) == (False, 10.0, 2.0, 4)

    def test_config_supplies_them(self):
        cfg = z.SessionConfig(
            mode=z.Mode.PEER, auto_reconnect=True, probe_interval=3.0, resync_jitter=0.5, resync_concurrency=2
        )
        assert _resolve_session_knobs(cfg, _MISSING, _MISSING, _MISSING, _MISSING) == (True, 3.0, 0.5, 2)

    def test_explicit_keywords_win(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER, auto_reconnect=True, resync_concurrency=2)
        explicit_off = False
        assert _resolve_session_knobs(cfg, explicit_off, _MISSING, 0.0, _MISSING) == (False, 10.0, 0.0, 2)

    def test_factory_stores_the_resync_knobs(self):
        sess = peer(auto_reconnect=True, probe_interval=0, resync_jitter=0.25, resync_concurrency=7)
        try:
            assert (sess._resync_jitter, sess._resync_concurrency) == (0.25, 7)
        finally:
            z.release(session=sess)


class TestOpenHonoursAutoReconnect:
    """0.4.0 (Q1): a config-opened session can be managed."""

    def test_open_returns_a_managed_session(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER, auto_reconnect=True, probe_interval=0, resync_jitter=0.1)
        sess = z_open(cfg)
        try:
            assert isinstance(sess, z.ManagedSession)
            assert sess._resync_jitter == 0.1
            assert callable(sess.on_link_restored)
        finally:
            z.release(session=sess)

    def test_without_it_open_stays_raw(self):
        sess = z_open(z.SessionConfig(mode=z.Mode.PEER))
        try:
            assert not isinstance(sess, z.ManagedSession)
        finally:
            sess.close()

    def test_an_explicit_keyword_beats_the_config(self):
        sess = peer(config=z.SessionConfig(mode=z.Mode.PEER, auto_reconnect=True), auto_reconnect=False)
        try:
            assert not isinstance(sess, z.ManagedSession)
        finally:
            sess.close()

    def test_router_mode_refuses_it(self):
        with pytest.raises(ValueError, match='peer and client modes, not router'):
            z_open(z.SessionConfig(mode=z.Mode.ROUTER, auto_reconnect=True))
