"""How zeared behaves across a hub restart.

Written in 0.3.10 as characterisation tests of the old behaviour, each naming the
0.4.0 work that would flip it; 0.4.0 flipped them. Each docstring says what changed.

One real restart cycle runs once for the module (``restart_record``), and each test
asserts one facet of what it recorded. The cycle, in the hub topology where the
publisher is the hub:

1. Hub A boots and publishes a ``RETAINED`` + ``LIVELINESS`` status with a fresh boot
   id, and declares presence on its key.
2. A managed client attaches (``auto_reconnect=True``) with a typed subscriber that
   also takes ``on_online`` / ``on_offline``, a raw liveliness subscriber, transport and
   link event listeners, an ``on_reconnect`` counter, and ``on_link_lost`` /
   ``on_link_restored`` recorders.
3. A closes. The client sees A's key go offline.
4. The hub reopens on the same endpoint as B, which publishes and declares presence at
   once, while the client is still detached: the client's re-dial is slowed so this
   lands inside the re-attach window (see ``slow_redial_client``).
5. The client re-attaches; the re-attach walk runs, then ``on_link_restored``. Then
   ``fetch_retained``, then B closes.

The tests run real sessions and take about 6 s together. They carry the ``restart``
marker, so ``-m "not restart"`` deselects them.
"""

from __future__ import annotations

import contextlib
import time
import uuid
from dataclasses import dataclass, field

import pytest
from conftest import RestartableHub, routers, slow_redial_client, wait_until

import zeared as z

pytestmark = pytest.mark.restart


@z.zeared
class Status(z.Message):
    TOPIC = 'restart/status/{node}'
    RETAINED = True
    LIVELINESS = True
    node: str = z.Str(required=True)
    boot: str = z.Str(required=True)
    alive: bool = z.Bool(required=True)


def _kind(sample_or_event: object) -> str:
    """``'PUT'`` / ``'DELETE'`` from a sample's or a link event's ``kind``."""
    return str(sample_or_event.kind).rsplit('.', 1)[-1]  # ty: ignore[unresolved-attribute]


@dataclass
class RestartRecord:
    """What one restart cycle looked like from the client."""

    boot_a: str = ''
    boot_b: str = ''
    zid_a: str = ''
    zid_b: str = ''
    events: list[tuple[str, bool, str]] = field(default_factory=list)  # (boot, alive, origin)
    events_after_restart: list[tuple[str, bool, str]] = field(default_factory=list)
    events_after_kill_b: list[tuple[str, bool, str]] = field(default_factory=list)
    presence: list[tuple[str, str]] = field(default_factory=list)  # (online/offline, origin)
    raw_liveliness: list[tuple[str, str]] = field(default_factory=list)  # (key, PUT/DELETE)
    transport_events: list[str] = field(default_factory=list)
    link_events: list[str] = field(default_factory=list)
    routers_timeline: list[tuple[str, int]] = field(default_factory=list)
    states: list[str] = field(default_factory=list)
    reconnects: int = 0
    link_signals: list[str] = field(default_factory=list)  # 'lost' / 'restored'
    fetched: list[tuple[str, bool]] = field(default_factory=list)
    published_in_window: bool = False


@pytest.fixture(scope='module')
def restart_record() -> RestartRecord:
    rec = RestartRecord()
    hub = RestartableHub()

    def boot(slot: str):
        def _boot(session):
            boot_id = uuid.uuid4().hex[:8]
            setattr(rec, f'boot_{slot}', boot_id)
            setattr(rec, f'zid_{slot}', str(session.zid()))
            Status(node='n', boot=boot_id, alive=True).send(session=session)
            Status.declare_presence(node='n', session=session)

        return _boot

    hub.start(boot('a'))
    client = slow_redial_client(hub.endpoint)
    handles: list = []
    try:
        client.on_reconnect(lambda _s: setattr(rec, 'reconnects', rec.reconnects + 1))
        client.on_link_lost(lambda _s: rec.link_signals.append('lost'))
        client.on_link_restored(lambda _s: rec.link_signals.append('restored'))
        Status.on_message(
            lambda m, meta: rec.events.append((m.boot, m.alive, meta.origin.value)),
            session=client,
            on_online=lambda meta: rec.presence.append(('online', meta.origin.value)),
            on_offline=lambda meta: rec.presence.append(('offline', meta.origin.value)),
        )
        raw = client.raw()
        handles.append(
            raw.liveliness().declare_subscriber(
                'restart/status/**',
                lambda s: rec.raw_liveliness.append((str(s.key_expr), _kind(s))),
                history=True,
            )
        )
        handles.append(raw.info.declare_transport_events_listener(lambda e: rec.transport_events.append(_kind(e))))
        handles.append(raw.info.declare_link_events_listener(lambda e: rec.link_events.append(_kind(e))))

        def mark(label: str) -> None:
            rec.routers_timeline.append((label, routers(client)))
            rec.states.append(client.state)

        time.sleep(0.5)
        mark('attached')

        hub.stop()  # A goes away; its key goes offline at the client
        wait_until(lambda: routers(client) == 0, 5)
        time.sleep(0.5)
        mark('after_kill_a')

        hub.start(boot('b'))  # B boots and publishes at once
        rec.published_in_window = routers(client) == 0
        mark('at_publish_b')
        if not rec.published_in_window:
            pytest.fail(
                're-attach window lost: the client re-attached before B published, so the '
                "fixture's timing assumption broke (see slow_redial_client)"
            )

        wait_until(lambda: routers(client) == 1, 10)
        mark('after_reattach')
        wait_until(lambda: rec.link_signals[-1:] == ['restored'], 10)  # the walk has run
        time.sleep(0.3)
        rec.events_after_restart = list(rec.events)
        rec.fetched = [(m.boot, m.alive) for m in Status.fetch_retained(session=client)]

        hub.stop()  # B goes away
        wait_until(lambda: routers(client) == 0, 5)
        time.sleep(1.0)
        mark('after_kill_b')
        rec.events_after_kill_b = list(rec.events)
    finally:
        for h in handles:
            with contextlib.suppress(Exception):
                h.undeclare()
        with contextlib.suppress(Exception):
            z.release(session=client)
        hub.stop()
    return rec


def test_on_reconnect_does_not_fire_on_a_hub_restart(restart_record):
    """The client's own session never closed, so it was never swapped: no ``on_reconnect``.

    Unchanged in 0.4.0 (decision 1.2): ``on_reconnect`` keeps its narrow meaning.
    """
    assert restart_record.reconnects == 0
    assert set(restart_record.states) == {'IDLE'}


def test_link_signals_pair_across_the_restart(restart_record):
    """0.4.0 (plan 11 §2.2): the link signals cover what ``on_reconnect`` doesn't.

    Lost when A went, restored once the client re-attached to B (after the walk), lost
    again when B went.
    """
    assert restart_record.link_signals == ['lost', 'restored', 'lost']


def test_retained_value_published_in_the_window_is_delivered(restart_record):
    """B's boot status, published while the client was detached, reaches the subscriber.

    Flipped in 0.4.0 (plan 11 §2.2): the re-attach walk re-fetches it, delivered as
    ``REPLAY`` before ``on_link_restored`` fires. (A's offline no longer reaches ``cb`` as
    a typed will: presence has its own callbacks.)
    """
    rec = restart_record
    assert rec.events_after_restart == [(rec.boot_a, True, 'replay'), (rec.boot_b, True, 'replay')]


def test_fetch_retained_recovers_it(restart_record):
    """The value was never lost: B's retention cache serves it on demand.

    Stays true. The value was never lost; the re-attach walk is the trigger that was
    missing.
    """
    assert restart_record.fetched == [(restart_record.boot_b, True)]


def test_restarted_publishers_death_is_announced(restart_record):
    """B declared presence while the client was detached, and its death is still announced.

    Flipped in 0.4.0 (plan 11 §2.1): presence is a liveliness token at the contract key,
    so there is no will stash to miss. The client sees A online (``REPLAY``, already up when
    it subscribed) and offline, then B's token appear once the link is back (``LIVE``) and go.
    """
    assert restart_record.presence == [
        ('online', 'replay'),
        ('offline', 'will'),
        ('online', 'live'),
        ('offline', 'will'),
    ]


def test_liveliness_reports_each_boots_token_at_the_contract_key(restart_record):
    """The client's raw liveliness subscriber sees A's token and then B's come and go.

    The mechanism the presence feed is built on (0.4.0): the tokens sit on the contract
    key itself, where ``__zeared/alive/<zid>`` carried them before.
    """
    assert restart_record.raw_liveliness == [
        ('restart/status/n', 'PUT'),
        ('restart/status/n', 'DELETE'),
        ('restart/status/n', 'PUT'),
        ('restart/status/n', 'DELETE'),
    ]


def test_router_link_state_is_observable(restart_record):
    """The router link's loss and return are visible, by polling and as pushed events.

    Stays true. It pins the inputs plan 11 §2.2's detection can use: ``routers_zid()``
    (which doc 10 planned to poll) and the transport / link event listeners (present from
    ``eclipse-zenoh`` 1.9.0, zeared's floor).
    """
    rec = restart_record
    assert rec.routers_timeline == [
        ('attached', 1),
        ('after_kill_a', 0),
        ('at_publish_b', 0),
        ('after_reattach', 1),
        ('after_kill_b', 0),
    ]
    assert rec.transport_events == ['DELETE', 'PUT', 'DELETE']
    assert rec.link_events == ['DELETE', 'PUT', 'DELETE']
