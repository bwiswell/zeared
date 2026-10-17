"""End-to-end tests for presence as liveliness tokens at contract keys (0.4.0).

A publisher holds a key with ``declare_presence``; subscribers hear about it on
``on_message(on_online=…, on_offline=…)``. Each test runs real in-process
Zenoh sessions. Among them: a handle's ``undeclare()`` fires offline for that
key only, with the session alive; and offline reaches ``on_offline`` with
``origin=WILL``, distinct from a tombstone.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import wait

import zeared as z


def _status_class(topic: str, *, retained: bool = False) -> type[z.Message]:
    @z.zeared
    class Status(z.Message):
        TOPIC = topic
        LIVELINESS = True
        RETAINED = retained
        name: str = z.Str(required=True)
        state: str = z.Str(default='up')

    return Status


class _Events:
    """Collects presence events as ``(kind, name, origin)``."""

    def __init__(self) -> None:
        self.items: list[tuple[str, str, z.Origin]] = []

    def online(self, meta: z.ZenohMeta) -> None:
        self.items.append(('online', meta.captures['name'], meta.origin))

    def offline(self, meta: z.ZenohMeta) -> None:
        self.items.append(('offline', meta.captures['name'], meta.origin))


class TestCallbacksNeedLiveliness:
    def test_non_liveliness_class_rejects_them(self, session):
        @z.zeared
        class Plain(z.Message):
            TOPIC = 'pres/plain/{name}'
            name: str = z.Str(required=True)

        with pytest.raises(z.TopicError, match='LIVELINESS = True'):
            Plain.on_message(lambda m: None, session=session, on_offline=lambda meta: None)


class TestOnlineAndOffline:
    def test_offline_when_the_holder_closes(self, connected_pair):
        session_a, session_b = connected_pair
        status = _status_class('pres/close/{name}')
        status.declare_presence(name='alice', session=session_a)
        wait(0.3)

        events = _Events()
        sub = status.on_message(lambda m: None, session=session_b, on_online=events.online, on_offline=events.offline)
        wait(0.3)
        session_a.close()
        wait(0.5)
        sub.close()

        assert events.items == [('online', 'alice', z.Origin.REPLAY), ('offline', 'alice', z.Origin.WILL)]

    def test_online_is_live_after_subscribing(self, connected_pair):
        session_a, session_b = connected_pair
        status = _status_class('pres/live/{name}')

        events = _Events()
        sub = status.on_message(lambda m: None, session=session_b, on_online=events.online, on_offline=events.offline)
        wait(0.3)
        token = status.declare_presence(name='bob', session=session_a)
        wait(0.3)
        token.undeclare()
        wait(0.3)
        sub.close()

        assert events.items == [('online', 'bob', z.Origin.LIVE), ('offline', 'bob', z.Origin.WILL)]

    def test_undeclare_withdraws_one_key_while_the_session_lives(self, connected_pair):
        """A proxy holding several devices' keys drops one and stays up itself."""
        session_a, session_b = connected_pair
        status = _status_class('pres/proxy/{name}')
        dev1 = status.declare_presence(name='dev1', session=session_a)
        status.declare_presence(name='dev2', session=session_a)
        wait(0.3)

        events = _Events()
        sub = status.on_message(lambda m: None, session=session_b, on_offline=events.offline)
        wait(0.3)
        dev1.undeclare()
        wait(0.3)
        held = sorted(str(r.ok.key_expr) for r in session_b.liveliness().get('pres/proxy/**', timeout=1.0) if r.ok)
        sub.close()

        assert events.items == [('offline', 'dev1', z.Origin.WILL)]
        assert held == ['pres/proxy/dev2']

    def test_release_withdraws_every_token(self, connected_pair):
        session_a, session_b = connected_pair
        status = _status_class('pres/release/{name}')
        status.declare_presence(name='a', session=session_a)
        status.declare_presence(name='b', session=session_a)
        wait(0.3)

        events = _Events()
        sub = status.on_message(lambda m: None, session=session_b, on_offline=events.offline)
        wait(0.3)
        z.release(session=session_a)
        wait(0.5)
        sub.close()

        assert sorted(name for _kind, name, _origin in events.items) == ['a', 'b']

    def test_own_session_tokens_are_delivered(self, session):
        status = _status_class('pres/own/{name}')
        events = _Events()
        sub = status.on_message(lambda m: None, session=session, on_online=events.online)
        wait(0.2)
        status.declare_presence(name='me', session=session)
        wait(0.3)
        sub.close()

        assert events.items == [('online', 'me', z.Origin.LIVE)]


class TestOfflineIsNotATombstone:
    """Offline and a deletion arrive on different callbacks: one is presence, the other retained state."""

    def test_feeds_stay_separate(self, connected_pair):
        session_a, session_b = connected_pair
        status = _status_class('pres/tomb/{name}', retained=True)
        status(name='alice').send(session=session_a)
        status.declare_presence(name='alice', session=session_a)
        wait(0.3)

        typed: list[str] = []
        removed: list[str] = []
        events = _Events()
        sub = status.on_message(
            lambda m: typed.append(m.name),
            session=session_b,
            on_remove=lambda meta: removed.append(meta.captures['name']),
            on_offline=events.offline,
        )
        wait(0.3)
        status.unretain(name='alice', session=session_a)  # a tombstone, not an offline
        wait(0.3)
        assert removed == ['alice']
        assert events.items == []

        z.release(session=session_a)  # an offline, not a tombstone
        wait(0.5)
        sub.close()

        assert removed == ['alice']
        assert events.items == [('offline', 'alice', z.Origin.WILL)]
        assert typed == ['alice']  # cb saw the retained record, never a presence event


def _star(n: int) -> list:
    """A listening peer and ``n`` peers connected to it: ``[hub, *spokes]``."""
    import json
    import socket

    import zenoh

    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()

    def open_peer(key: str) -> zenoh.Session:
        c = zenoh.Config()
        c.insert_json5('mode', '"peer"')
        c.insert_json5('scouting/multicast/enabled', 'false')
        c.insert_json5(key, json.dumps([f'tcp/127.0.0.1:{port}']))
        return zenoh.open(c)

    hub = open_peer('listen/endpoints')
    sessions = [hub, *(open_peer('connect/endpoints') for _ in range(n))]
    wait(0.4)
    return sessions


class TestTokensAggregateByKey:
    """The day-one spike, pinned: to an observer that holds none itself, a key
    goes offline when its *last* token goes, from whichever session.

    (A session that holds a key *and* watches it sees it go offline when
    another holder leaves; ``docs/presence/presence.md`` records that.)
    """

    def test_a_second_holder_keeps_the_key_online(self):
        status = _status_class('pres/agg/{name}')
        observer, holder_a, holder_b = _star(2)
        try:
            events = _Events()
            sub = status.on_message(lambda m: None, session=observer, on_offline=events.offline)
            wait(0.3)
            status.declare_presence(name='shared', session=holder_a)
            second = status.declare_presence(name='shared', session=holder_b)
            wait(0.3)
            holder_a.close()
            wait(0.5)
            assert events.items == []  # holder_b still holds it
            second.undeclare()
            wait(0.4)
            sub.close()
            assert events.items == [('offline', 'shared', z.Origin.WILL)]
        finally:
            for sess in (holder_b, observer):
                sess.close()


class TestHubLoss:
    """In client mode every key a subscriber knows goes offline when the hub dies,
    whether it saw the key come online or found it online when it subscribed.

    The router declares a token to a client only for a subscriber that asks for
    history, and a client withdraws, when its link drops, only the tokens it was
    declared. Before the presence watch kept a ``history=True`` subscriber of its
    own, a key reported by the subscribe-time replay never went offline here.
    """

    @pytest.mark.parametrize('holder', ['hub', 'client'])
    def test_every_known_key_goes_offline(self, restartable_hub, holder):
        status = _status_class(f'pres/hubloss/{holder}/{{name}}')
        hub = restartable_hub.start()
        holding = hub if holder == 'hub' else z.client(restartable_hub.endpoint)
        watcher = z.client(restartable_hub.endpoint)
        try:
            status.declare_presence(name='early', session=holding)  # online before the subscriber
            wait(0.4)
            events = _Events()
            status.on_message(lambda m: None, session=watcher, on_online=events.online, on_offline=events.offline)
            wait(0.3)
            status.declare_presence(name='late', session=holding)  # online after it
            wait(0.4)
            restartable_hub.stop()  # the hub dies; nothing withdraws a token first
            wait(0.6)

            assert events.items[:2] == [('online', 'early', z.Origin.REPLAY), ('online', 'late', z.Origin.LIVE)]
            assert sorted(events.items[2:]) == [('offline', 'early', z.Origin.WILL), ('offline', 'late', z.Origin.WILL)]
        finally:
            z.release(session=watcher)
            if holding is not hub:
                z.release(session=holding)


class TestAsyncCallbacks:
    def test_async_on_offline_is_scheduled_on_the_loop(self, connected_pair):
        session_a, session_b = connected_pair
        status = _status_class('pres/async/{name}')
        status.declare_presence(name='carol', session=session_a)

        async def main():
            got: list[str] = []
            done = asyncio.Event()

            async def on_offline(meta):
                got.append(meta.captures['name'])
                done.set()

            sub = status.on_message(lambda m: None, session=session_b, on_offline=on_offline)
            await asyncio.sleep(0.3)
            await asyncio.to_thread(session_a.close)
            await asyncio.wait_for(done.wait(), timeout=3.0)
            sub.close()
            return got

        assert asyncio.run(main()) == ['carol']
