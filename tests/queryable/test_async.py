"""Async façade tests — ``aon_query`` / ``aquery`` / ``aquery_one``.

zeared has no pytest-asyncio dependency; async bodies run via
``asyncio.run`` inside sync test functions (matching ``test_async_.py``).
"""

from __future__ import annotations

import asyncio
import time

import pytest
from conftest import _peer_session

import zeared as z


class TestAsyncQueryable:
    def test_async_handler_roundtrip(self, session):
        @z.zeared
        class Aq(z.Message):
            TOPIC = 'q/async/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}

        async def body():
            async def handler(ctx):
                await asyncio.sleep(0.01)
                return Aq(id=ctx.captures['id'], v=42)

            qbl = await z.aon_query(Aq, handler)
            await asyncio.sleep(0.3)
            res = await z.aquery(Aq, id='z', timeout=2.0)
            qbl.close()
            out['res'] = res

        asyncio.run(body())
        assert out['res']
        assert out['res'][0].v == 42

    def test_aquery_one(self, session):
        @z.zeared
        class Aq(z.Message):
            TOPIC = 'q/async1/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}

        async def body():
            qbl = await z.aon_query(
                Aq,
                lambda ctx: Aq(id=ctx.captures['id'], v=1),
            )
            await asyncio.sleep(0.3)
            out['one'] = await z.aquery_one(Aq, id='a', timeout=2.0)
            qbl.close()

        asyncio.run(body())
        assert out['one'] is not None
        assert out['one'].v == 1

    def test_sync_handler_via_aon_query(self, session):
        @z.zeared
        class Aq(z.Message):
            TOPIC = 'q/async2/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}

        async def body():
            qbl = await z.aon_query(
                Aq,
                lambda ctx: Aq(id=ctx.captures['id'], v=9),
            )
            await asyncio.sleep(0.3)
            out['res'] = await z.aquery(Aq, id='a', timeout=2.0)
            qbl.close()

        asyncio.run(body())
        assert out['res']
        assert out['res'][0].v == 9


class TestAsyncGeneratorHandler:
    """Regression: an ``async def`` generator handler answered with nothing.

    ``inspect.iscoroutinefunction`` is ``False`` for an async generator
    function, so ``Queryable._declare`` routed one down the *sync* path.
    ``handler(ctx)`` there returns an ``async_generator`` — not awaitable,
    and not iterable either — so ``_reply_result``'s ``iter()`` raised
    ``TypeError``, logged "expected a Message, an iterable of Messages, or
    None", and replied nothing at all. The querying client got zero rows
    and no error; the only trace was a warning in the *server's* log.

    This matters beyond tidiness: an async generator is the natural way to
    express a streaming multi-reply handler under async, so the shape most
    likely to be reached for was the one that silently failed.
    """

    def test_async_generator_replies_every_yield(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/agen/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}

        async def body():
            async def handler(ctx):
                for v in range(3):
                    await asyncio.sleep(0.01)
                    yield Row(id=ctx.captures['id'], v=v)

            qbl = await z.aon_query(Row, handler)
            await asyncio.sleep(0.3)
            out['res'] = await z.aquery(Row, id='a', timeout=2.0)
            qbl.close()

        asyncio.run(body())
        assert sorted(m.v for m in out['res']) == [0, 1, 2]

    def test_async_generator_raising_midstream_is_reported(self, session):
        """Mirrors the sync generator contract — partial replies land, and
        the getter is told the stream was truncated."""

        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/agenboom/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}
        served: list[Exception] = []
        got: list[Exception] = []

        async def body():
            async def handler(ctx):
                yield Row(id=ctx.captures['id'], v=0)
                await asyncio.sleep(0.01)
                msg = 'boom mid-stream'
                raise RuntimeError(msg)

            qbl = await z.aon_query(Row, handler, on_error=lambda e, raw: served.append(e))
            await asyncio.sleep(0.3)
            out['res'] = await z.aquery(Row, id='a', timeout=1.0, on_error=lambda e, raw: got.append(e))
            qbl.close()

        asyncio.run(body())
        assert [m.v for m in out['res']] == [0]
        assert any(isinstance(e, z.QueryableError) and 'boom mid-stream' in str(e) for e in served), served
        assert any(isinstance(e, z.QueryError) and 'boom mid-stream' in str(e) for e in got), got

    def test_async_generator_without_running_loop_raises(self):
        """``on_query`` from sync code with an async generator must fail
        loud at declare time, exactly as an ``async def`` handler does —
        not no-op silently on the Zenoh callback thread later."""

        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/agenloop/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        async def handler(ctx):
            yield Row(id=ctx.captures['id'], v=1)

        s = _peer_session()
        try:
            with pytest.raises(z.QueryableError, match='no running event loop'):
                Row.on_query(handler, session=s)
        finally:
            s.close()


class TestAqueryIter:
    """``aquery_iter`` — the async streaming getter.

    Bridges the blocking channel loop to the event loop through a worker
    thread, terminating on a sentinel (a query ends; a subscription
    doesn't, which is why ``alisten`` needs no such thing).
    """

    def test_yields_same_rows_as_aquery(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/aiter/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}

        async def body():
            async def handler(ctx):
                for v in range(4):
                    yield Row(id=ctx.captures['id'], v=v)

            qbl = await z.aon_query(Row, handler)
            await asyncio.sleep(0.3)
            out['streamed'] = [m.v async for m in z.aquery_iter(Row, id='a', timeout=2.0)]
            out['collected'] = [m.v for m in await z.aquery(Row, id='a', timeout=2.0)]
            qbl.close()

        asyncio.run(body())
        assert sorted(out['streamed']) == sorted(out['collected']) == [0, 1, 2, 3]

    def test_first_row_arrives_before_the_last_is_produced(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/aitertiming/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}

        async def body():
            async def handler(ctx):
                for v in range(4):
                    await asyncio.sleep(0.3)
                    yield Row(id=ctx.captures['id'], v=v)

            qbl = await z.aon_query(Row, handler)
            await asyncio.sleep(0.3)

            t0 = time.monotonic()
            async for m in z.aquery_iter(Row, id='a', timeout=5.0):
                out['first'] = m
                out['t_first'] = time.monotonic() - t0
                break

            t1 = time.monotonic()
            out['all'] = await z.aquery(Row, id='a', timeout=5.0)
            out['t_all'] = time.monotonic() - t1
            qbl.close()

        asyncio.run(body())
        assert out['first'].v == 0
        assert len(out['all']) == 4
        assert out['t_first'] < out['t_all']
        assert out['t_first'] < 0.9, f'first reply took {out["t_first"]:.2f}s — not streaming'

    def test_early_break_does_not_hang_the_loop(self, session):
        """Breaking out must return promptly and not wedge on the worker.

        ``asyncio.to_thread`` can't interrupt a blocked thread, so the
        generator's ``finally`` signals it to stand down instead. The
        serving side still runs to completion — that asymmetry is
        documented, not fixed.
        """

        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/aiterbreak/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}

        async def body():
            async def handler(ctx):
                for v in range(6):
                    await asyncio.sleep(0.2)
                    yield Row(id=ctx.captures['id'], v=v)

            qbl = await z.aon_query(Row, handler)
            await asyncio.sleep(0.3)
            t0 = time.monotonic()
            seen = []
            async for m in z.aquery_iter(Row, id='a', timeout=10.0):
                seen.append(m.v)
                if len(seen) == 2:
                    break
            out['elapsed'] = time.monotonic() - t0
            out['seen'] = seen
            qbl.close()

        asyncio.run(asyncio.wait_for(body(), timeout=20.0))
        assert out['seen'] == [0, 1]
        # Returned near the second reply (~0.4s), not the 10s timeout.
        assert out['elapsed'] < 3.0, f'break took {out["elapsed"]:.2f}s'

    def test_empty_when_nobody_answers(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/aiternone/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        out = {}

        async def body():
            out['rows'] = [m async for m in z.aquery_iter(Row, id='nobody', timeout=0.5)]

        asyncio.run(body())
        assert out['rows'] == []
