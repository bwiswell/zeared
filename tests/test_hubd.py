"""Tests for ``zeared/hubd.py``, the ``python -m zeared.hubd`` relay daemon, and the
``z.hub()`` factory and router-mode config it is built on.

The hub lets nodes that can't reach each other directly (e.g. both NAT-gated,
outbound-only) communicate: each connects out to the hub, which relays
pub/sub, queries, and liveliness. Clients here have multicast off and no
listen endpoints, so the hub is the *only* path between them — a received
message is proof of relay.
"""

from __future__ import annotations

import sys
import threading

import pytest
import zenoh
from conftest import _endpoint, wait

import zeared as z
from zeared._factories import _build_config_for_router, _resolve_hub_listen
from zeared.config import SessionConfig
from zeared.hubd import run


def _raw_client(ep: str) -> zenoh.Session:
    """Outbound-only client: multicast off, no listen — reachable only via hub."""
    c = zenoh.Config()
    c.insert_json5('mode', '"client"')
    c.insert_json5('scouting/multicast/enabled', 'false')
    c.insert_json5('connect/endpoints', f'["{ep}"]')
    return zenoh.open(c)


def _shm_segments() -> int:
    """How many Zenoh shared-memory segments this process has mapped."""
    with open('/proc/self/maps') as maps:  # noqa: PTH123 — a procfs read, not a path to model
        return sum(1 for line in maps if line.rstrip().endswith('.zenoh'))


class TestBuildRouterConfig:
    def test_shared_memory_off_by_default(self):
        cfg = _build_config_for_router(['tcp/0.0.0.0:7447'], None, None)
        assert cfg.get_json('transport/shared_memory/enabled') == 'false'

    def test_sets_router_mode_and_listen(self):
        cfg = _build_config_for_router(['tcp/0.0.0.0:7447'], None, None)
        js = cfg.get_json('mode')
        assert 'router' in js
        assert '7447' in cfg.get_json('listen/endpoints')

    def test_timestamping_on_by_default(self):
        cfg = _build_config_for_router(['tcp/0.0.0.0:7447'], None, None)
        assert 'true' in cfg.get_json('timestamping/enabled')

    def test_connect_optional(self):
        cfg = _build_config_for_router(
            ['tcp/0.0.0.0:7447'],
            ['tcp/other:7447'],
            None,
        )
        assert 'other' in cfg.get_json('connect/endpoints')


@pytest.mark.skipif(not sys.platform.startswith('linux'), reason='reads /proc/self/maps')
class TestSharedMemoryStaysOff:
    """A large message through the hub travels inline, never through Zenoh's shared memory.

    With Zenoh's shared-memory transport on (its default), anything over 3 KB between two
    processes on one host goes through a shared-memory pool. The receiver locks the pages it maps
    and keeps every closed session's segment mapped, and once that reaches ``RLIMIT_MEMLOCK``
    Zenoh drops large messages, saying so only at DEBUG. No segment mapped is the proof the path
    is never taken; sessions in one process take it too, so this runs in-process.
    """

    def test_a_large_put_and_query_through_the_hub_map_no_segment(self):
        ep = _endpoint()
        h = z.hub(listen=[ep])
        a, b = z.client(ep), z.client(ep)
        wait(0.4)
        before = _shm_segments()
        got: list[int] = []
        sub = a.declare_subscriber('svc/big', lambda sample: got.append(len(sample.payload.to_bytes())))
        qbl = a.declare_queryable('svc/q', lambda q: q.reply('svc/q', b'ok'))
        wait(0.2)
        b.put('svc/big', b'x' * 100_000)
        replies = [bytes(r.ok.payload) for r in b.get('svc/q', payload=b'x' * 100_000, timeout=3.0) if r.ok]
        wait(0.3)
        after = _shm_segments()
        sub.undeclare()
        qbl.undeclare()
        for sess in (a, b, h):
            sess.close()
        assert replies == [b'ok']
        assert got == [100_000]
        assert after == before

    def test_the_hub_alone_keeps_it_off_for_clients_that_allow_it(self):
        """Shared memory is agreed per link, so a hub with it off covers clients on Zenoh's defaults."""
        ep = _endpoint()
        h = z.hub(listen=[ep])
        a, b = _raw_client(ep), _raw_client(ep)
        wait(0.4)
        before = _shm_segments()
        qbl = a.declare_queryable('svc/q', lambda q: q.reply('svc/q', b'ok'))
        wait(0.2)
        replies = [bytes(r.ok.payload) for r in b.get('svc/q', payload=b'x' * 100_000, timeout=3.0) if r.ok]
        wait(0.3)
        after = _shm_segments()
        qbl.undeclare()
        for sess in (a, b, h):
            sess.close()
        assert replies == [b'ok']
        assert after == before


class TestModeDispatch:
    def test_open_dispatches_router(self):
        ep = _endpoint()
        sess = z.open(SessionConfig(mode=z.Mode.ROUTER, listen=[ep]))
        try:
            assert sess.zid() is not None
        finally:
            sess.close()


class TestHubRelaysTransport:
    def test_pubsub(self):
        ep = _endpoint()
        h = z.hub(listen=[ep])
        a, b = _raw_client(ep), _raw_client(ep)
        wait(0.4)
        got: list[bytes] = []
        sub = b.declare_subscriber('demo/**', lambda s: got.append(bytes(s.payload)))
        wait(0.2)
        a.put('demo/x', b'relayed')
        wait(0.4)
        sub.undeclare()
        for s in (a, b, h):
            s.close()
        assert got == [b'relayed']

    def test_query(self):
        ep = _endpoint()
        h = z.hub(listen=[ep])
        a, b = _raw_client(ep), _raw_client(ep)
        wait(0.4)
        qbl = a.declare_queryable('svc/**', lambda q: q.reply('svc/x', b'ok'))
        wait(0.2)
        replies = [bytes(r.ok.payload) for r in b.get('svc/x') if r.ok is not None]
        qbl.undeclare()
        for s in (a, b, h):
            s.close()
        assert replies == [b'ok']

    def test_liveliness_put_and_delete(self):
        ep = _endpoint()
        h = z.hub(listen=[ep])
        a, b = _raw_client(ep), _raw_client(ep)
        wait(0.4)
        kinds: list[str] = []
        lsub = b.liveliness().declare_subscriber(
            'alive/**',
            lambda s: kinds.append(str(s.kind)),
        )
        wait(0.2)
        tok = a.liveliness().declare_token('alive/nodeA')
        assert tok is not None  # hold the token alive until a.close()
        wait(0.3)
        saw_put = 'SampleKind.PUT' in kinds
        a.close()  # DELETE should propagate via hub
        wait(0.4)
        saw_delete = 'SampleKind.DELETE' in kinds
        lsub.undeclare()
        for s in (b, h):
            s.close()
        assert saw_put
        assert saw_delete


class TestHubRelaysZearedMessages:
    def test_retained_fetch_through_hub(self):
        ep = _endpoint()
        h = z.hub(listen=[ep])
        pub = z.client(router=ep)
        sub_sess = z.client(router=ep)
        wait(0.4)

        @z.zeared
        class Reading(z.Message):
            TOPIC = 'rio/telemetry/reading/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        Reading(id=1, v=42).send(session=pub)
        wait(0.3)

        # Late subscriber joins via the hub — retained-fetch (a query) must
        # route through the hub to reach the publisher's retention queryable.
        got: list[tuple[int, int]] = []
        s = Reading.on_message(lambda m: got.append((m.id, m.v)), session=sub_sess)
        wait(0.4)
        s.close()
        for sess in (pub, sub_sess, h):
            sess.close()
        assert got == [(1, 42)]

    def test_presence_through_hub(self):
        """0.4.0: a presence token relayed by the hub; its holder's close reads as offline."""
        ep = _endpoint()
        h = z.hub(listen=[ep])
        pub = z.client(router=ep)
        sub_sess = z.client(router=ep)
        wait(0.4)

        @z.zeared
        class Status(z.Message):
            TOPIC = 'rio/presence/status/{name}'
            LIVELINESS = True
            name: str = z.Str(required=True)
            state: str = z.Str(default='up')

        Status.declare_presence(name='alice', session=pub)
        wait(0.3)

        events: list[tuple[str, str, z.Origin]] = []
        s = Status.on_message(
            lambda m: None,
            session=sub_sess,
            on_online=lambda meta: events.append(('online', meta.captures['name'], meta.origin)),
            on_offline=lambda meta: events.append(('offline', meta.captures['name'], meta.origin)),
        )
        wait(0.3)
        pub.close()  # its token goes; the hub relays the liveliness DELETE
        wait(0.5)
        s.close()
        for sess in (sub_sess, h):
            sess.close()
        assert events == [('online', 'alice', z.Origin.REPLAY), ('offline', 'alice', z.Origin.WILL)]


class TestHubDaemon:
    def test_run_lifecycle(self):
        ep = _endpoint()
        stop = threading.Event()
        ready = threading.Event()
        box: dict = {}

        def on_ready(sess):
            box['zid'] = str(sess.zid())
            ready.set()

        t = threading.Thread(
            target=run,
            kwargs={'listen': [ep], 'stop': stop, 'on_ready': on_ready},
            daemon=True,
        )
        t.start()
        assert ready.wait(3.0), 'hub did not come up'

        c = _raw_client(ep)
        wait(0.3)
        connected = not c.is_closed()
        c.close()

        stop.set()
        t.join(timeout=3.0)
        assert box.get('zid')
        assert connected
        assert not t.is_alive()

    def test_main_wires_args_to_run(self, monkeypatch):
        captured: dict = {}

        def fake_run(**kwargs):
            captured.update(kwargs)

        monkeypatch.setattr('zeared.hubd.run', fake_run)
        from zeared.hubd import main

        rc = main(
            [
                '--listen',
                'tcp/0.0.0.0:9999',
                '--connect',
                'tcp/other:7447',
                '--no-timestamping',
            ]
        )
        assert rc == 0
        assert captured['listen'] == ['tcp/0.0.0.0:9999']
        assert captured['connect'] == ['tcp/other:7447']
        assert captured['timestamping'] is False

    def test_main_turns_shared_memory_off_over_a_config_file(self, monkeypatch, tmp_path):
        """A hub's ``--config`` (TLS, access control) cannot switch shared memory back on."""
        captured: dict = {}
        monkeypatch.setattr('zeared.hubd.run', lambda **kw: captured.update(kw))
        from zeared.hubd import main

        conf = tmp_path / 'hub.json5'
        conf.write_text("{ mode: 'router', transport: { shared_memory: { enabled: true } } }")
        main(['--config', str(conf)])
        assert captured['zenoh_config'].get_json('transport/shared_memory/enabled') == 'false'
        assert captured['zenoh_config'].get_json('mode') == '"router"'  # the rest of the file stands

    def test_main_leaves_listen_to_the_hub(self, monkeypatch):
        """Pin (0.3.9): with no ``--listen``, ``main`` passes ``None``, so the hub binds its
        ``--config`` file's listeners, or ``tcp/0.0.0.0:7447`` when there are none. Before
        0.3.9 it passed the default itself, which overwrote the file's."""
        captured: dict = {}
        monkeypatch.setattr('zeared.hubd.run', lambda **kw: captured.update(kw))
        from zeared.hubd import main

        main([])
        assert captured['listen'] is None
        assert captured['timestamping'] is True


def _router_config(ep: str) -> zenoh.Config:
    """A hub config that names its own listener (and nothing zeared would add)."""
    c = zenoh.Config()
    c.insert_json5('mode', '"router"')
    c.insert_json5('scouting/multicast/enabled', 'false')
    c.insert_json5('listen/endpoints', f'["{ep}"]')
    return c


def _relays(ep: str) -> bool:
    """Two outbound-only clients on ``ep`` see each other's put: the hub there relays."""
    a, b = _raw_client(ep), _raw_client(ep)
    try:
        wait(0.4)
        got: list[bytes] = []
        sub = b.declare_subscriber('keep/**', lambda s: got.append(bytes(s.payload)))
        wait(0.2)
        a.put('keep/x', b'relayed')
        wait(0.4)
        sub.undeclare()
        return got == [b'relayed']
    finally:
        a.close()
        b.close()


class TestHubKeepsConfigListen:
    """Pin (0.3.9): a hub binds its own config's listen endpoints when no listen is given.

    Before 0.3.9 every hub path wrote ``listen/endpoints`` over the config, with
    ``listen=`` or the ``tcp/0.0.0.0:7447`` default, so a config file's own
    listeners (a TLS-only one, say) were never bound.
    """

    def test_hub_binds_a_raw_configs_listener(self):
        ep = _endpoint()
        h = z.hub(zenoh_config=_router_config(ep))
        try:
            assert _relays(ep)
        finally:
            h.close()

    def test_open_binds_a_config_files_listener(self, tmp_path):
        ep = _endpoint()
        conf = tmp_path / 'hub.json5'
        conf.write_text(f'{{ listen: {{ endpoints: ["{ep}"] }}, scouting: {{ multicast: {{ enabled: false }} }} }}')
        h = z.open(SessionConfig(mode=z.Mode.ROUTER, zenoh_config_file=str(conf)))
        try:
            assert _relays(ep)
        finally:
            h.close()

    def test_open_binds_an_overrides_listener(self):
        ep = _endpoint()
        h = z.open(SessionConfig(mode=z.Mode.ROUTER, zenoh_overrides={'listen/endpoints': [ep]}))
        try:
            assert _relays(ep)
        finally:
            h.close()


class TestDaemonConfigFile:
    """Pin (0.3.9): ``python -m zeared.hubd --config`` applies zeared's settings over the file.

    Router mode, timestamping (unless ``--no-timestamping``) and shared memory off land over
    the file, as they do over a ``SessionConfig.zenoh_config_file``. Before 0.3.9 only shared
    memory did, so a file without ``mode`` opened a peer.
    """

    def test_a_modeless_file_runs_a_router(self, monkeypatch, tmp_path):
        captured: dict = {}
        monkeypatch.setattr('zeared.hubd.run', lambda **kw: captured.update(kw))
        from zeared.hubd import main

        conf = tmp_path / 'hub.json5'
        conf.write_text('{ scouting: { multicast: { enabled: false } } }')
        main(['--config', str(conf)])
        cfg = captured['zenoh_config']
        assert cfg.get_json('mode') == '"router"'
        assert cfg.get_json('timestamping/enabled') == 'true'
        assert cfg.get_json('scouting/multicast/enabled') == 'false'  # the rest of the file stands

    def test_no_timestamping_is_honoured_with_a_file(self, monkeypatch, tmp_path):
        captured: dict = {}
        monkeypatch.setattr('zeared.hubd.run', lambda **kw: captured.update(kw))
        from zeared.hubd import main

        conf = tmp_path / 'hub.json5'
        conf.write_text("{ mode: 'router' }")
        main(['--config', str(conf), '--no-timestamping'])
        assert captured['zenoh_config'].get_json('timestamping/enabled') != 'true'


class TestDaemonLogging:
    def test_logs_to_stdout(self, monkeypatch):
        """Pin (0.3.9): the daemon logs to stdout (journald under systemd), as its docs say."""
        seen: dict = {}
        monkeypatch.setattr('zeared.hubd.run', lambda **kw: None)
        monkeypatch.setattr('zeared.hubd.logging.basicConfig', lambda **kw: seen.update(kw))
        from zeared.hubd import main

        main([])
        assert seen['stream'] is sys.stdout


class TestResolveHubListen:
    """Pin (0.3.9): which endpoints a hub binds, from every source, in order."""

    def test_listen_kwarg_first(self):
        cfg = SessionConfig(mode=z.Mode.ROUTER, listen=['tcp/127.0.0.1:1'])
        assert _resolve_hub_listen(['tcp/127.0.0.1:2'], cfg, _router_config('tcp/127.0.0.1:3')) == (
            ['tcp/127.0.0.1:2'],
            ['tcp/127.0.0.1:2'],
        )

    def test_session_config_listen_next(self):
        cfg = SessionConfig(mode=z.Mode.ROUTER, listen=['tcp/127.0.0.1:1'])
        assert _resolve_hub_listen(None, cfg, _router_config('tcp/127.0.0.1:3')) == (
            ['tcp/127.0.0.1:1'],
            ['tcp/127.0.0.1:1'],
        )

    def test_the_configs_own_listeners_are_kept(self):
        assert _resolve_hub_listen(None, None, _router_config('tls/0.0.0.0:7448')) == (None, ['tls/0.0.0.0:7448'])

    def test_default_when_the_config_names_none(self):
        c = zenoh.Config()
        c.insert_json5('mode', '"router"')
        assert _resolve_hub_listen(None, None, c) == (['tcp/0.0.0.0:7447'], ['tcp/0.0.0.0:7447'])

    def test_default_when_there_is_no_config(self):
        assert _resolve_hub_listen(None, None, None) == (['tcp/0.0.0.0:7447'], ['tcp/0.0.0.0:7447'])

    def test_empty_listen_kwarg_counts_as_unset(self):
        # As before 0.3.9: an empty ``listen=`` falls through to the next source.
        assert _resolve_hub_listen([], None, None) == (['tcp/0.0.0.0:7447'], ['tcp/0.0.0.0:7447'])


class TestBuildRouterConfigKeepsListen:
    def test_none_keeps_the_configs_listen(self):
        cfg = _build_config_for_router(None, None, _router_config('tls/0.0.0.0:7448'))
        assert cfg.get_json('listen/endpoints') == '["tls/0.0.0.0:7448"]'

    def test_explicit_listen_overrides_the_configs(self):
        cfg = _build_config_for_router(['tcp/127.0.0.1:9'], None, _router_config('tls/0.0.0.0:7448'))
        assert cfg.get_json('listen/endpoints') == '["tcp/127.0.0.1:9"]'


class TestDaemonListenOverridesFile:
    def test_listen_flag_wins_over_the_file(self, monkeypatch, tmp_path):
        captured: dict = {}
        monkeypatch.setattr('zeared.hubd.run', lambda **kw: captured.update(kw))
        from zeared.hubd import main

        conf = tmp_path / 'hub.json5'
        conf.write_text('{ listen: { endpoints: ["tls/0.0.0.0:7448"] } }')
        main(['--config', str(conf), '--listen', 'tcp/127.0.0.1:9'])
        write, bound = _resolve_hub_listen(captured['listen'], None, captured['zenoh_config'])
        assert write == bound == ['tcp/127.0.0.1:9']

    def test_without_the_flag_the_files_listener_stands(self, monkeypatch, tmp_path):
        captured: dict = {}
        monkeypatch.setattr('zeared.hubd.run', lambda **kw: captured.update(kw))
        from zeared.hubd import main

        conf = tmp_path / 'hub.json5'
        conf.write_text('{ listen: { endpoints: ["tls/0.0.0.0:7448"] } }')
        main(['--config', str(conf)])
        assert _resolve_hub_listen(captured['listen'], None, captured['zenoh_config']) == (
            None,
            ['tls/0.0.0.0:7448'],
        )
