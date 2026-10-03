"""Tests for the relay hub — ``z.hub()`` factory, router-mode config, and the
``python -m zeared.hubd`` daemon.

The hub lets nodes that can't reach each other directly (e.g. both NAT-gated,
outbound-only) communicate: each connects out to the hub, which relays
pub/sub, queries, and liveliness. Clients here have multicast off and no
listen endpoints, so the hub is the *only* path between them — a received
message is proof of relay.
"""

from __future__ import annotations

import socket
import sys
import threading

import pytest
import zenoh
from conftest import wait

import zeared as z
from zeared._factories import _build_config_for_router
from zeared.config import SessionConfig
from zeared.hubd import run


def _free_port() -> int:
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _endpoint() -> str:
    return f'tcp/127.0.0.1:{_free_port()}'


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

    def test_liveliness_will_through_hub(self):
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
            state: str = z.Str(required=True)

        Status(name='alice', state='offline').register_will(session=pub)
        wait(0.3)

        received: list[tuple[str, str]] = []
        s = Status.on_message(
            lambda m: received.append((m.name, m.state)),
            session=sub_sess,
        )
        wait(0.3)
        pub.close()  # will fires via hub-relayed liveliness DELETE
        wait(0.5)
        s.close()
        for sess in (sub_sess, h):
            sess.close()
        assert ('alice', 'offline') in received


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

    def test_main_defaults_listen(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr('zeared.hubd.run', lambda **kw: captured.update(kw))
        from zeared.hubd import main

        main([])
        assert captured['listen'] == ['tcp/0.0.0.0:7447']
        assert captured['timestamping'] is True
