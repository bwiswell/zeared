from __future__ import annotations

import logging
import os
import time
from unittest import mock

import pytest

import zeared as z
from zeared._factories import _open_with_retry


class TestSessionConfigSeared:
    def test_minimum_peer_config(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER)
        assert cfg.mode is z.Mode.PEER
        assert cfg.retry is False
        assert cfg.initial_backoff == 0.1
        assert cfg.max_backoff == 30.0
        assert cfg.max_attempts is None

    def test_full_client_config(self):
        cfg = z.SessionConfig(
            mode=z.Mode.CLIENT,
            router='tcp/127.0.0.1:17447',
            retry=True,
            initial_backoff=0.05,
            max_backoff=10.0,
            max_attempts=5,
        )
        assert cfg.mode is z.Mode.CLIENT
        assert cfg.router == 'tcp/127.0.0.1:17447'
        assert cfg.retry is True
        assert cfg.max_attempts == 5

    def test_round_trip_via_seared(self):
        cfg = z.SessionConfig(
            mode=z.Mode.PEER, listen=['tcp/0.0.0.0:7447'],
            retry=True, max_backoff=60.0,
        )
        d = z.SessionConfig.dump(cfg)
        loaded = z.SessionConfig.load(d)
        assert loaded.mode is z.Mode.PEER
        assert loaded.listen == ['tcp/0.0.0.0:7447']
        assert loaded.retry is True

    def test_load_rejects_invalid_mode_string(self):
        with pytest.raises(z.ValidationError):
            z.SessionConfig.load({'mode': 'galactic'})

    def test_load_accepts_string_for_mode(self):
        # seared's Enum field auto-coerces strings on load.
        cfg = z.SessionConfig.load({'mode': 'peer'})
        assert cfg.mode is z.Mode.PEER


class TestBuilders:
    def test_replace_updates_fields_returns_new_instance(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT, router='tcp/a:7447')
        updated = cfg.replace(router='tcp/b:7447', retry=True)
        # Original unchanged
        assert cfg.router == 'tcp/a:7447'
        assert cfg.retry is False
        # New instance has the changes
        assert updated.router == 'tcp/b:7447'
        assert updated.retry is True
        # Untouched fields preserved
        assert updated.mode is z.Mode.CLIENT

    def test_replace_rejects_unknown_field(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER)
        with pytest.raises(TypeError):
            cfg.replace(nonsense_key='value')

    def test_with_retry_enables(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT, router='tcp/a:7447')
        updated = cfg.with_retry()
        assert updated.retry is True
        assert updated.initial_backoff == cfg.initial_backoff  # preserved
        assert cfg.retry is False  # original unchanged

    def test_with_retry_disables(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT, router='tcp/a:7447', retry=True)
        updated = cfg.with_retry(retry=False)
        assert updated.retry is False

    def test_with_retry_sets_knobs(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER)
        updated = cfg.with_retry(
            initial_backoff=0.5, max_backoff=60, max_attempts=10,
        )
        assert updated.retry is True
        assert updated.initial_backoff == 0.5
        assert updated.max_backoff == 60
        assert updated.max_attempts == 10

    def test_with_retry_none_knobs_preserve(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER, initial_backoff=1.0)
        updated = cfg.with_retry()   # all knobs None
        assert updated.initial_backoff == 1.0

    def test_with_connect_appends(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER, connect=['tcp/a:7447'])
        updated = cfg.with_connect('tcp/b:7447', 'tcp/c:7447')
        assert cfg.connect == ['tcp/a:7447']  # original unchanged
        assert updated.connect == ['tcp/a:7447', 'tcp/b:7447', 'tcp/c:7447']

    def test_with_listen_appends(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER)
        updated = cfg.with_listen('tcp/0.0.0.0:7447')
        assert cfg.listen == []
        assert updated.listen == ['tcp/0.0.0.0:7447']

    def test_set_router_sets(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT)
        updated = cfg.set_router('tcp/new:7447')
        assert cfg.router is None
        assert updated.router == 'tcp/new:7447'

    def test_with_router_removed(self):
        """Pin: 0.0.13 hard-rename — ``with_router`` no longer exists."""
        cfg = z.SessionConfig(mode=z.Mode.CLIENT)
        assert not hasattr(cfg, 'with_router')

    def test_fluent_chaining(self):
        cfg = (
            z.SessionConfig(mode=z.Mode.CLIENT)
            .set_router('tcp/router:7447')
            .with_retry(max_backoff=60)
            .with_connect('tcp/backup:7447')
        )
        assert cfg.mode is z.Mode.CLIENT
        assert cfg.router == 'tcp/router:7447'
        assert cfg.retry is True
        assert cfg.max_backoff == 60
        assert cfg.connect == ['tcp/backup:7447']


class TestFromEnv:
    """Pin: ``SessionConfig.from_env`` reads ``ZEARED_SESSION_*`` env
    vars (or a custom prefix), coerces field values, and returns a
    valid ``SessionConfig``."""

    @pytest.fixture(autouse=True)
    def _scrub_env(self, monkeypatch):
        # Strip any inherited ZEARED_SESSION_* vars.
        for key in list(os.environ.keys()):
            if key.startswith('ZEARED_SESSION_'):
                monkeypatch.delenv(key, raising=False)

    def test_minimal_peer(self, monkeypatch):
        monkeypatch.setenv('ZEARED_SESSION_MODE', 'peer')
        cfg = z.SessionConfig.from_env()
        assert cfg.mode is z.Mode.PEER
        assert cfg.retry is False     # default

    def test_full_client(self, monkeypatch):
        monkeypatch.setenv('ZEARED_SESSION_MODE', 'client')
        monkeypatch.setenv('ZEARED_SESSION_ROUTER', 'tcp/r:7447')
        monkeypatch.setenv('ZEARED_SESSION_CONNECT', 'tcp/a:7447, tcp/b:7447')
        monkeypatch.setenv('ZEARED_SESSION_RETRY', 'true')
        monkeypatch.setenv('ZEARED_SESSION_INITIAL_BACKOFF', '0.5')
        monkeypatch.setenv('ZEARED_SESSION_MAX_BACKOFF', '60')
        monkeypatch.setenv('ZEARED_SESSION_MAX_ATTEMPTS', '5')
        cfg = z.SessionConfig.from_env()
        assert cfg.mode is z.Mode.CLIENT
        assert cfg.router == 'tcp/r:7447'
        assert cfg.connect == ['tcp/a:7447', 'tcp/b:7447']
        assert cfg.retry is True
        assert cfg.initial_backoff == 0.5
        assert cfg.max_backoff == 60.0
        assert cfg.max_attempts == 5

    def test_missing_mode_raises(self, monkeypatch):
        with pytest.raises(z.ValidationError):
            z.SessionConfig.from_env()

    def test_custom_prefix(self, monkeypatch):
        monkeypatch.setenv('MYAPP_MODE', 'peer')
        cfg = z.SessionConfig.from_env(prefix='MYAPP_')
        assert cfg.mode is z.Mode.PEER

    def test_bool_variants(self, monkeypatch):
        for truthy in ('true', 'TRUE', '1', 'yes', 'on'):
            monkeypatch.setenv('ZEARED_SESSION_MODE', 'peer')
            monkeypatch.setenv('ZEARED_SESSION_RETRY', truthy)
            cfg = z.SessionConfig.from_env()
            assert cfg.retry is True, f'failed for {truthy!r}'
        for falsy in ('false', 'FALSE', '0', 'no', 'off'):
            monkeypatch.setenv('ZEARED_SESSION_RETRY', falsy)
            cfg = z.SessionConfig.from_env()
            assert cfg.retry is False, f'failed for {falsy!r}'

    def test_empty_value_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv('ZEARED_SESSION_MODE', 'peer')
        monkeypatch.setenv('ZEARED_SESSION_ROUTER', '')   # empty string
        cfg = z.SessionConfig.from_env()
        assert cfg.router is None     # not the empty string


class TestFromYaml:
    """Pin: ``SessionConfig.from_yaml`` parses YAML mappings into a
    ``SessionConfig``. Lazy import of PyYAML — not a hard dependency."""

    def test_yaml_string(self):
        pytest.importorskip('yaml')
        yaml_text = '''
        mode: peer
        retry: true
        connect:
          - tcp/a:7447
          - tcp/b:7447
        max_backoff: 30.0
        '''
        cfg = z.SessionConfig.from_yaml(yaml_text)
        assert cfg.mode is z.Mode.PEER
        assert cfg.retry is True
        assert cfg.connect == ['tcp/a:7447', 'tcp/b:7447']
        assert cfg.max_backoff == 30.0

    def test_yaml_file(self, tmp_path):
        pytest.importorskip('yaml')
        f = tmp_path / 'config.yaml'
        f.write_text(
            'mode: client\n'
            'router: tcp/router:7447\n'
            'retry: false\n'
        )
        cfg = z.SessionConfig.from_yaml(str(f))
        assert cfg.mode is z.Mode.CLIENT
        assert cfg.router == 'tcp/router:7447'

    def test_yaml_missing_pyyaml_raises_helpful(self):
        # Simulate missing PyYAML.
        import sys
        from unittest.mock import patch

        # If yaml IS installed, monkeypatch the import to fail.
        real_yaml = sys.modules.pop('yaml', None)
        try:
            with patch.dict(sys.modules, {'yaml': None}):
                with pytest.raises(ImportError, match='PyYAML'):
                    z.SessionConfig.from_yaml('mode: peer')
        finally:
            if real_yaml is not None:
                sys.modules['yaml'] = real_yaml

    def test_yaml_non_mapping_raises(self):
        pytest.importorskip('yaml')
        with pytest.raises(ValueError, match='must be a mapping'):
            z.SessionConfig.from_yaml('- just\n- a\n- list')


class TestPeerFactory:
    def test_no_config_no_retry_kwargs_is_fine(self):
        sess = z.peer()
        assert sess is not None
        sess.close()

    def test_kwargs_override_config(self):
        # Was previously a TypeError; now kwargs win.
        cfg = z.SessionConfig(mode=z.Mode.PEER, retry=False, max_attempts=5)
        sleeps: list[float] = []
        with mock.patch('zeared._factories.time.sleep', lambda s: sleeps.append(s)):
            calls = [0]

            def fake_open(_):
                calls[0] += 1
                raise RuntimeError('down')

            with mock.patch('zeared._factories.zenoh.open', fake_open):
                with pytest.raises(RuntimeError):
                    # retry=True kwarg overrides cfg.retry=False;
                    # max_attempts=2 kwarg overrides cfg.max_attempts=5.
                    z.peer(config=cfg, retry=True, max_attempts=2)
        assert calls[0] == 2

    def test_config_declarative_peer(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER)
        sess = z.peer(config=cfg)
        assert sess is not None
        sess.close()


class TestClientFactory:
    def test_requires_router_or_config(self):
        with pytest.raises(TypeError, match='router'):
            z.client()

    def test_config_without_endpoints_rejected(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT)   # no router, no connect
        with pytest.raises(TypeError, match='router'):
            z.client(config=cfg)

    def test_kwargs_router_overrides_config(self):
        cfg = z.SessionConfig(mode=z.Mode.CLIENT, router='tcp/from-cfg:7447')
        captured: list = []

        def fake_open(c):
            captured.append(c)
            raise RuntimeError('down')

        with mock.patch('zeared._factories.zenoh.open', fake_open):
            with pytest.raises(RuntimeError):
                z.client('tcp/from-kwarg:7447', config=cfg)
        # Config built with the kwarg endpoint, not the cfg one.
        assert 'tcp/from-kwarg:7447' in str(captured[0])


class TestOpen:
    def test_dispatch_on_mode(self):
        cfg = z.SessionConfig(mode=z.Mode.PEER)
        sess = z.open(cfg)
        sess.close()


class TestRetryLoop:
    def test_no_retry_raises_on_first_failure(self):
        def open_fn():
            raise RuntimeError('no router')

        with pytest.raises(RuntimeError):
            _open_with_retry(
                open_fn, retry=False, initial_backoff=0.01,
                max_backoff=0.1, max_attempts=None,
                endpoint_label='test',
            )

    def test_retry_respects_max_attempts(self):
        calls = [0]

        def open_fn():
            calls[0] += 1
            raise RuntimeError('down')

        with pytest.raises(RuntimeError):
            _open_with_retry(
                open_fn, retry=True, initial_backoff=0.001,
                max_backoff=0.002, max_attempts=3,
                endpoint_label='test',
            )
        assert calls[0] == 3   # 1 initial + 2 retries (stops BEFORE the 3rd retry)

    def test_retry_eventually_succeeds(self):
        calls = [0]

        def open_fn():
            calls[0] += 1
            if calls[0] < 3:
                raise RuntimeError('still down')
            return mock.Mock()     # a fake session

        sess = _open_with_retry(
            open_fn, retry=True, initial_backoff=0.001,
            max_backoff=0.01, max_attempts=10,
            endpoint_label='test',
        )
        assert sess is not None
        assert calls[0] == 3

    def test_backoff_doubles_capped_at_max(self):
        """Verify the delay schedule — record sleep calls."""
        calls = [0]
        sleeps: list[float] = []

        def open_fn():
            calls[0] += 1
            if calls[0] < 5:
                raise RuntimeError('down')
            return mock.Mock()

        def fake_sleep(s):
            sleeps.append(s)

        with mock.patch('zeared._factories.time.sleep', fake_sleep):
            _open_with_retry(
                open_fn, retry=True, initial_backoff=1.0,
                max_backoff=4.0, max_attempts=None,
                endpoint_label='t',
            )

        # 4 retries → 4 sleeps: 1.0, 2.0, 4.0, 4.0 (capped)
        assert sleeps == [1.0, 2.0, 4.0, 4.0]

    def test_log_level_escalates_after_third_retry(self, caplog):
        calls = [0]

        def open_fn():
            calls[0] += 1
            if calls[0] < 6:
                raise RuntimeError('down')
            return mock.Mock()

        with mock.patch('zeared._factories.time.sleep', lambda s: None):
            with caplog.at_level(logging.INFO, logger='zeared.connect'):
                _open_with_retry(
                    open_fn, retry=True, initial_backoff=0.001,
                    max_backoff=0.002, max_attempts=None,
                    endpoint_label='thing',
                )

        info_records = [r for r in caplog.records if r.levelno == logging.INFO]
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        # Five failures; first three at INFO, remaining two at WARNING.
        assert len(info_records) >= 3
        assert len(warning_records) >= 2
