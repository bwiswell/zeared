from __future__ import annotations

import asyncio
import threading
import time

import pytest

import zeared as z

from conftest import wait


# ---------------------------------------------------------------------------
# Unit-ish tests of the watchdog primitive directly
# ---------------------------------------------------------------------------


class TestSubscriberWatchdogPrimitive:
    def test_optimistic_does_not_fire_until_first_ping(self):
        from zeared.watchdog import _SubscriberWatchdog

        quiet_count = [0]
        wd = _SubscriberWatchdog(
            interval=0.1,
            on_quiet=lambda: quiet_count.__setitem__(0, quiet_count[0] + 1),
            on_active=None,
        )
        # Sleep past several intervals — without any ping, watchdog must
        # not have started a thread.
        time.sleep(0.4)
        wd.cancel()
        assert quiet_count[0] == 0

    def test_quiet_fires_after_interval_without_ping(self):
        from zeared.watchdog import _SubscriberWatchdog

        events: list[str] = []
        wd = _SubscriberWatchdog(
            interval=0.1,
            on_quiet=lambda: events.append('quiet'),
            on_active=lambda: events.append('active'),
        )
        wd.ping()                  # establish cadence
        time.sleep(0.3)            # well past interval
        wd.cancel()
        assert 'quiet' in events
        # Active wasn't ever reset → no on_active fired.
        assert events.count('active') == 0

    def test_active_fires_on_resume(self):
        from zeared.watchdog import _SubscriberWatchdog

        events: list[str] = []
        wd = _SubscriberWatchdog(
            interval=0.1,
            on_quiet=lambda: events.append('quiet'),
            on_active=lambda: events.append('active'),
        )
        wd.ping()
        time.sleep(0.25)           # quiet should fire
        wd.ping()                  # resume → active
        time.sleep(0.05)
        wd.cancel()
        assert events == ['quiet', 'active']

    def test_cancel_suppresses_pending_quiet(self):
        from zeared.watchdog import _SubscriberWatchdog

        events: list[str] = []
        wd = _SubscriberWatchdog(
            interval=0.5,
            on_quiet=lambda: events.append('quiet'),
            on_active=None,
        )
        wd.ping()
        # Cancel before the interval expires.
        time.sleep(0.05)
        wd.cancel()
        time.sleep(0.6)            # past the original interval
        assert events == []

    def test_async_callbacks_dispatched(self):
        from zeared.watchdog import _SubscriberWatchdog

        results: list[str] = []

        async def quiet_cb():
            results.append('quiet')

        async def active_cb():
            results.append('active')

        wd = _SubscriberWatchdog(
            interval=0.1, on_quiet=quiet_cb, on_active=active_cb,
        )
        wd.ping()
        time.sleep(0.3)
        wd.ping()
        time.sleep(0.1)
        wd.cancel()
        assert 'quiet' in results
        assert 'active' in results

    def test_zero_interval_rejected(self):
        from zeared.watchdog import _SubscriberWatchdog

        with pytest.raises(ValueError):
            _SubscriberWatchdog(interval=0, on_quiet=None, on_active=None)


class TestStartupGrace:
    def test_grace_fires_quiet_when_no_first_message(self):
        from zeared.watchdog import _SubscriberWatchdog

        events: list[str] = []
        wd = _SubscriberWatchdog(
            interval=10.0,        # long, so we know on_quiet came from grace
            on_quiet=lambda: events.append('quiet'),
            on_active=None,
            startup_grace=0.1,
        )
        time.sleep(0.3)            # past grace
        wd.cancel()
        assert events == ['quiet']

    def test_grace_does_not_fire_if_first_message_arrives_in_time(self):
        from zeared.watchdog import _SubscriberWatchdog

        events: list[str] = []
        wd = _SubscriberWatchdog(
            interval=10.0,
            on_quiet=lambda: events.append('quiet'),
            on_active=None,
            startup_grace=0.5,
        )
        # Ping before grace expires.
        time.sleep(0.05)
        wd.ping()
        time.sleep(0.6)            # past original grace
        wd.cancel()
        assert events == []        # grace was satisfied

    def test_grace_then_resume_uses_interval(self):
        from zeared.watchdog import _SubscriberWatchdog

        events: list[str] = []
        wd = _SubscriberWatchdog(
            interval=0.1,
            on_quiet=lambda: events.append('quiet'),
            on_active=lambda: events.append('active'),
            startup_grace=0.05,
        )
        # Don't ping → grace expires → quiet.
        time.sleep(0.15)
        # Now ping → active.
        wd.ping()
        time.sleep(0.05)
        # Stop pinging → quiet again at interval.
        time.sleep(0.2)
        wd.cancel()

        assert events.count('quiet') >= 2     # initial grace + interval
        assert events.count('active') == 1

    def test_zero_grace_rejected(self):
        from zeared.watchdog import _SubscriberWatchdog

        with pytest.raises(ValueError):
            _SubscriberWatchdog(
                interval=1.0, on_quiet=None, on_active=None,
                startup_grace=0,
            )


# ---------------------------------------------------------------------------
# Integration: watchdog wired through Cls.on_message
# ---------------------------------------------------------------------------


class TestWatchdogViaOnMessage:
    def test_on_quiet_fires_when_publisher_pauses(self, session):
        @z.zeared
        class Tick(z.Message):
            TOPIC = 'wd/tick/{n}'
            n: int = z.Int(required=True)

        events: list[str] = []
        z.session = session
        sub = Tick.on_message(
            lambda m: None,
            expected_interval=0.2,
            on_quiet=lambda: events.append('quiet'),
            on_active=lambda: events.append('active'),
        )
        wait()

        # Publish two ticks, then pause for > interval.
        Tick(n=1).send()
        wait(0.05)
        Tick(n=2).send()
        wait(0.5)                  # past interval

        # Resume.
        Tick(n=3).send()
        wait(0.1)
        sub.close()

        assert 'quiet' in events
        assert 'active' in events

    def test_close_suppresses_pending_quiet(self, session):
        @z.zeared
        class Tick(z.Message):
            TOPIC = 'wd/close/{n}'
            n: int = z.Int(required=True)

        fired: list[str] = []
        z.session = session
        sub = Tick.on_message(
            lambda m: None,
            expected_interval=0.5,
            on_quiet=lambda: fired.append('quiet'),
        )
        wait()
        Tick(n=1).send()
        wait(0.1)
        # Close BEFORE the watchdog's interval would expire.
        sub.close()
        wait(0.6)                  # past the original interval
        assert fired == []

    def test_no_watchdog_when_interval_unset(self, session):
        """Default behaviour unchanged when expected_interval is None."""
        @z.zeared
        class Tick(z.Message):
            TOPIC = 'wd/none/{n}'
            n: int = z.Int(required=True)

        z.session = session
        sub = Tick.on_message(lambda m: None)   # no interval kwarg
        # Just verify no AttributeError on close, no thread leak.
        sub.close()

    def test_startup_grace_via_on_message(self, session):
        """Subscriber that gets no producer fires on_quiet after grace."""
        @z.zeared
        class Tick(z.Message):
            TOPIC = 'wd/grace/{n}'
            n: int = z.Int(required=True)

        events: list[str] = []
        z.session = session
        sub = Tick.on_message(
            lambda m: None,
            expected_interval=10.0,         # long, so on_quiet must come from grace
            startup_grace=0.2,
            on_quiet=lambda: events.append('quiet'),
        )
        wait(0.5)                           # past grace, no Tick ever sent
        sub.close()

        assert 'quiet' in events
