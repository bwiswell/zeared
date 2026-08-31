"""Lifecycle tests for queryables — registry, ``release`` ordering, and
reconnect redeclare.
"""

from __future__ import annotations

import contextlib
import threading

from conftest import _peer_session, wait

import zeared as z
from zeared._managed_session import ManagedSession
from zeared._reconnect import _trigger_reconnect, start_probe
from zeared.queryable import _queryables


class TestRegistryAndRelease:
    def test_release_closes_queryables(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/rel/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        qbl = Q.on_query(lambda ctx: None, session=session)
        assert id(session) in _queryables
        z.release(session=session)
        # Registry bucket dropped + handle closed.
        assert id(session) not in _queryables
        assert qbl._closed

    def test_clear_queryable_cache_all(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/clr/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        qbl = Q.on_query(lambda ctx: None, session=session)
        z.clear_queryable_cache()
        assert qbl._closed
        assert id(session) not in _queryables

    def test_close_deregisters(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/dereg/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        qbl = Q.on_query(lambda ctx: None, session=session)
        qbl.close()
        assert id(session) not in _queryables


class TestReconnect:
    def test_queryable_redeclared_against_new_raw(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/reco/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        new_raw = _peer_session()
        m = None
        try:
            m = ManagedSession(
                session,
                lambda: new_raw,
                endpoint_label='q-reco',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )
            qbl = Q.on_query(
                lambda ctx: Q(id=ctx.captures['id'], v=1),
                session=m,
            )
            old_handles = qbl._zenoh_queryables

            done = threading.Event()
            m._on_reconnect = lambda mgr: done.set()
            start_probe(m)
            _trigger_reconnect(m)
            assert done.wait(timeout=3.0)
            wait(0.2)

            # Redeclared against new_raw — fresh handles, still registered.
            assert qbl._zenoh_queryables
            assert qbl._zenoh_queryables is not old_handles
            assert not qbl._closed
            qbl.close()
        finally:
            if m is not None:
                with contextlib.suppress(Exception):
                    m._teardown(call_close=False)
            with contextlib.suppress(Exception):
                new_raw.close()

    def test_auto_reconnect_false_skipped(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/reco2/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        new_raw = _peer_session()
        m = None
        try:
            m = ManagedSession(
                session,
                lambda: new_raw,
                endpoint_label='q-reco2',
                probe_interval=0,
                initial_backoff=0.001,
                max_backoff=0.005,
                max_attempts=None,
            )
            qbl = Q.on_query(
                lambda ctx: None,
                session=m,
                auto_reconnect=False,
            )
            old_handles = qbl._zenoh_queryables

            done = threading.Event()
            m._on_reconnect = lambda mgr: done.set()
            start_probe(m)
            _trigger_reconnect(m)
            assert done.wait(timeout=3.0)
            wait(0.2)

            # Not redeclared — still the old (now-dead) handles.
            assert qbl._zenoh_queryables is old_handles
            qbl.close()
        finally:
            if m is not None:
                with contextlib.suppress(Exception):
                    m._teardown(call_close=False)
            with contextlib.suppress(Exception):
                new_raw.close()
