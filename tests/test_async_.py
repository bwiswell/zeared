from __future__ import annotations

import asyncio

import pytest

import zeared as z

from conftest import wait


# ---------------------------------------------------------------------------
# asend / alisten
# ---------------------------------------------------------------------------

class TestAsend:
    def test_asend_roundtrip(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'aio/tele'
            v: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = Tele.on_message(lambda m: received.append(m.v))
        wait()

        async def pub():
            for i in range(5):
                await Tele(v=i).asend()

        asyncio.run(pub())
        wait()
        sub.close()
        assert received == [0, 1, 2, 3, 4]

    def test_asend_accepts_topic_override(self, session):
        @z.zeared
        class Status(z.Message):
            TOPIC = 'aio/robot/{id}/status'
            EXTRA_TOPICS = ('aio/vehicle/{id}/status',)
            id: int = z.Int(required=True)
            status: str = z.Str(required=True)

        received: list[tuple[int, str]] = []
        z.session = session
        sub = Status.on_message(lambda m, meta: received.append((m.id, meta.key_expr)))
        wait()

        async def pub():
            await Status(id=1, status='x').asend()
            await Status(id=2, status='y').asend(topic='aio/vehicle/{id}/status')

        asyncio.run(pub())
        wait()
        sub.close()
        as_dict = dict(received)
        assert as_dict[1] == 'aio/robot/1/status'
        assert as_dict[2] == 'aio/vehicle/2/status'


class TestAlisten:
    def test_async_generator_yields_messages(self, session):
        @z.zeared
        class Tick(z.Message):
            TOPIC = 'aio/tick'
            n: int = z.Int(required=True)

        async def main():
            z.session = session
            produced: list[int] = []

            async def producer():
                await asyncio.sleep(0.1)
                for i in range(3):
                    await Tick(n=i).asend()

            async def consumer():
                async for msg in Tick.alisten():
                    produced.append(msg.n)
                    if len(produced) >= 3:
                        break

            await asyncio.gather(consumer(), producer())
            return produced

        got = asyncio.run(main())
        assert got == [0, 1, 2]

    def test_break_closes_subscriber(self, session):
        @z.zeared
        class Tick(z.Message):
            TOPIC = 'aio/breaktopic'
            n: int = z.Int(required=True)

        async def main():
            z.session = session

            async def producer():
                await asyncio.sleep(0.1)
                await Tick(n=1).asend()
                await asyncio.sleep(0.05)
                await Tick(n=2).asend()

            got: list[int] = []

            async def consumer():
                async for msg in Tick.alisten():
                    got.append(msg.n)
                    break

            await asyncio.gather(consumer(), producer())
            return got

        got = asyncio.run(main())
        assert got == [1]


# ---------------------------------------------------------------------------
# Coroutine callback detection in on_message
# ---------------------------------------------------------------------------

class TestCoroutineCallback:
    def test_async_callback_scheduled_on_loop(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'aio/cb'
            v: int = z.Int(required=True)

        async def main():
            z.session = session
            received: list[int] = []
            done = asyncio.Event()

            async def handler(msg):
                received.append(msg.v)
                if len(received) >= 3:
                    done.set()

            sub = Tele.on_message(handler)
            await asyncio.sleep(0.1)
            for i in range(3):
                await Tele(v=i).asend()
            await asyncio.wait_for(done.wait(), timeout=2.0)
            sub.close()
            return received

        got = asyncio.run(main())
        assert got == [0, 1, 2]

    def test_async_cb_without_running_loop_raises(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'aio/nolup'
            v: int = z.Int(required=True)

        async def handler(msg):  # pragma: no cover — only inspected
            pass

        z.session = session
        with pytest.raises(z.SubscriptionError, match='no running event loop'):
            Tele.on_message(handler)


# ---------------------------------------------------------------------------
# abatch
# ---------------------------------------------------------------------------

class TestAbatch:
    def test_abatch_flushes_on_exit(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'aio/abatch/flush'
            v: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        async def pub():
            async with z.abatch():
                for i in range(4):
                    await M(v=i).asend()
                # nothing sent yet
                assert received == []

        asyncio.run(pub())
        wait()
        sub.close()
        assert sorted(received) == [0, 1, 2, 3]

    def test_abatch_discards_on_exception(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'aio/abatch/discard'
            v: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        async def pub():
            try:
                async with z.abatch():
                    await M(v=1).asend()
                    await M(v=2).asend()
                    raise RuntimeError('boom')
            except RuntimeError:
                pass

        asyncio.run(pub())
        wait()
        sub.close()
        assert received == []


class TestTaskIsolation:
    def test_contextvar_isolates_per_task(self, session):
        """Two asyncio tasks in the same thread should have independent batches."""
        @z.zeared
        class M(z.Message):
            TOPIC = 'aio/task/isol'
            v: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        async def main():
            started_a = asyncio.Event()
            started_b = asyncio.Event()
            continue_a = asyncio.Event()

            async def task_a():
                async with z.abatch():
                    await M(v=1).asend()
                    started_a.set()
                    await continue_a.wait()
                    await M(v=2).asend()

            async def task_b():
                await started_a.wait()
                # Task A is inside a batch; task B must NOT see that buffer.
                from zeared.batch import current_buffer
                assert current_buffer() is None
                # B sends outside any batch — should flush immediately.
                await M(v=99).asend()
                started_b.set()
                continue_a.set()

            await asyncio.gather(task_a(), task_b())

        asyncio.run(main())
        wait()
        sub.close()

        # Task A flushed [1, 2] at its batch exit; Task B sent 99 immediately.
        # Ordering between A's final flush and B's 99 isn't guaranteed, but
        # the full set must be present.
        assert sorted(received) == [1, 2, 99]


class TestApeerAclient:
    """Pin: 0.0.15 refactor — ``apeer`` / ``aclient`` / ``aopen`` are
    sync functions returning an ``_AsyncSessionContextManager``. The
    only valid spelling is ``async with z.apeer(...) as sess:``."""

    def test_apeer_async_with(self):
        async def main():
            async with z.apeer() as sess:
                assert sess is not None
                assert not sess.is_closed()

        asyncio.run(main())

    def test_apeer_releases_on_exit(self):
        async def main():
            async with z.apeer(auto_reconnect=True, probe_interval=0) as sess:
                assert sess.state == 'IDLE'
                worker = sess._reconnect_thread
                assert worker is not None
            # Outside the block: release ran, worker joined.
            worker.join(timeout=2.0)
            assert not worker.is_alive()

        asyncio.run(main())

    def test_apeer_releases_on_exception(self):
        class _Boom(RuntimeError):
            pass

        async def main():
            with pytest.raises(_Boom):
                async with z.apeer(auto_reconnect=True, probe_interval=0):
                    raise _Boom('block raised')

        asyncio.run(main())

    def test_aopen_async_with(self):
        async def main():
            cfg = z.SessionConfig(mode=z.Mode.PEER)
            async with z.aopen(cfg) as sess:
                assert sess is not None
                assert not sess.is_closed()

        asyncio.run(main())


# ---------------------------------------------------------------------------
# Mixed sync + async
# ---------------------------------------------------------------------------

class TestRetentionAsync:
    def test_asend_with_retain(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'aio/ret/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        async def main():
            z.session = session
            await Tele(id=1, v=10).asend()
            await Tele(id=2, v=20).asend(retain=False)

            from zeared.retention import get_retention_cache
            cache = get_retention_cache(Tele, session)
            return cache.size

        size = asyncio.run(main())
        assert size == 1   # id=1 cached; id=2 live-only

    def test_aunretain_removes_cache_entry(self, session):
        @z.zeared
        class Tele(z.Message):
            TOPIC = 'aio/retdel/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        async def main():
            z.session = session
            await Tele(id=1, v=1).asend()
            await Tele(id=2, v=2).asend()
            await Tele(id=1, v=1).aunretain()       # instance form
            await z.aunretain(Tele, id=2)            # class form via helper
            from zeared.retention import get_retention_cache
            return get_retention_cache(Tele, session).size

        assert asyncio.run(main()) == 0

    def test_alisten_fetches_retained_values_at_start(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Tele(z.Message):
            TOPIC = 'aio/alistenret/{id}'
            RETAINED = True
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        # Publish retained on A (sync).
        Tele(id=1, v=10).send(session=session_a)
        Tele(id=2, v=20).send(session=session_a)
        wait()

        async def main():
            seen: list[tuple[int, int]] = []

            async def consumer():
                async for m in Tele.alisten(session=session_b):
                    seen.append((m.id, m.v))
                    if len(seen) >= 2:
                        break

            await asyncio.wait_for(consumer(), timeout=3.0)
            return sorted(seen)

        got = asyncio.run(main())
        assert got == [(1, 10), (2, 20)]


class TestAsendInsideSyncBatch:
    """Pin the contextvar-propagation behaviour: ``await msg.asend()``
    inside a sync ``with z.batch():`` block correctly buffers and flushes.

    Why this works: ``asyncio.run(coro)`` runs the coroutine in a context
    that inherits from the calling thread; ``asyncio.to_thread(send)``
    inside the coroutine then does ``ctx = copy_context()`` +
    ``ctx.run(send)`` — the worker thread reads the SAME contextvar list
    reference and mutates the SAME buffer the main thread sees on
    ``__exit__``.

    If a future Python version regresses this propagation, this test fires
    loudly. The same propagation does NOT happen for raw
    ``threading.Thread``-spawned worker threads — that's documented as a
    known sharp edge in batch.md.
    """
    def test_asend_inside_sync_batch_buffers_and_flushes(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'cv/batch/{id}'
            id: int = z.Int(required=True)
            v: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        async def coro():
            await M(id=1, v=10).asend()
            await M(id=2, v=20).asend()

        with z.batch():
            asyncio.run(coro())
            # Inside the batch — nothing should have flushed yet.
            assert received == []

        # After exiting the batch, both messages must arrive.
        wait(0.2)
        sub.close()
        assert sorted(received) == [10, 20]


class TestMixed:
    def test_sync_send_then_async_send(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'aio/mixed'
            v: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        M(v=1).send()

        async def main():
            await M(v=2).asend()

        asyncio.run(main())
        wait()
        sub.close()

        assert sorted(received) == [1, 2]
