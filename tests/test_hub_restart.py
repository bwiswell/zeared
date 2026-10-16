"""Characterisation tests: how zeared behaves across a hub restart, today.

They pin current behaviour and are rewritten when 0.4.0 changes it. Each test's
docstring names the 0.4.0 work that will flip it, so 0.4.0's diff is pure behaviour
with its harness already in place.

One real restart cycle runs once for the module (``restart_record``), and each test
asserts one facet of what it recorded. The cycle, in the hub topology where the
publisher is the hub:

1. Hub A boots and publishes a ``RETAINED`` + ``LIVELINESS`` status with a fresh boot
   id, and registers a will.
2. A managed client attaches (``auto_reconnect=True``) with a typed subscriber, a raw
   liveliness subscriber, transport and link event listeners, and an ``on_reconnect``
   counter.
3. A closes. The client sees A's will.
4. The hub reopens on the same endpoint as B, which publishes and registers its will
   at once, while the client is still detached: the client's re-dial is slowed so this
   lands inside the re-attach window (see ``slow_redial_client``).
5. The client re-attaches. Then ``fetch_retained``, then B closes.

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
    raw_liveliness: list[tuple[str, str]] = field(default_factory=list)  # (zid, PUT/DELETE)
    transport_events: list[str] = field(default_factory=list)
    link_events: list[str] = field(default_factory=list)
    routers_timeline: list[tuple[str, int]] = field(default_factory=list)
    states: list[str] = field(default_factory=list)
    reconnects: int = 0
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
            Status(node='n', boot=boot_id, alive=False).register_will(session=session)

        return _boot

    hub.start(boot('a'))
    client = slow_redial_client(hub.endpoint)
    handles: list = []
    try:
        client.on_reconnect(lambda _s: setattr(rec, 'reconnects', rec.reconnects + 1))
        Status.on_message(
            lambda m, meta: rec.events.append((m.boot, m.alive, meta.origin.value)),
            session=client,
        )
        raw = client.raw()
        handles.append(
            raw.liveliness().declare_subscriber(
                '__zeared/alive/**',
                lambda s: rec.raw_liveliness.append((str(s.key_expr).rsplit('/', 1)[-1], _kind(s))),
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

        hub.stop()  # A goes away; its will fires at the client
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
        time.sleep(1.5)
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
    """The client's own session never closed, so auto-reconnect sees nothing.

    Changes with plan 11 §2.2 (link re-attach): new link-lost / link-restored signals
    fire across this cycle; ``on_reconnect`` itself still doesn't (decision 1.2).
    """
    assert restart_record.reconnects == 0
    assert set(restart_record.states) == {'IDLE'}


def test_retained_value_published_in_the_window_is_not_delivered(restart_record):
    """B's boot status, published while the client was detached, never reaches the subscriber.

    Changes with plan 11 §2.2: the re-attach walk re-fetches it, delivered as ``REPLAY``.
    """
    rec = restart_record
    assert rec.events_after_restart == [
        (rec.boot_a, True, 'replay'),
        (rec.boot_a, False, 'will'),
    ]


def test_fetch_retained_recovers_it(restart_record):
    """The value was never lost: B's retention cache serves it on demand.

    Stays true. It pins that this is a trigger problem, not a protocol problem.
    """
    assert restart_record.fetched == [(restart_record.boot_b, True)]


def test_restarted_publishers_death_is_unannounced(restart_record):
    """B's will was registered while the client was detached, so its death produces no event.

    A's death, before the restart, is the control: announced as a will. Changes with plan
    11 §2.1 (presence as a liveliness token on the contract key), which announces B's death.
    """
    rec = restart_record
    assert (rec.boot_a, False, 'will') in rec.events_after_restart
    assert rec.events_after_kill_b == rec.events_after_restart


def test_liveliness_history_delivers_the_new_zids_put_and_delete(restart_record):
    """The client's raw liveliness subscriber sees B's token come and go.

    Stays true. It pins the mechanism plan 11 §2.1 relies on: under the token model those
    two samples are the online and offline events, with no stash to miss.
    """
    rec = restart_record
    assert rec.raw_liveliness == [
        (rec.zid_a, 'PUT'),
        (rec.zid_a, 'DELETE'),
        (rec.zid_b, 'PUT'),
        (rec.zid_b, 'DELETE'),
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
