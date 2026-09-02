"""Async façade tests — ``aon_query`` / ``aquery`` / ``aquery_one``.

zeared has no pytest-asyncio dependency; async bodies run via
``asyncio.run`` inside sync test functions (matching ``test_async_.py``).
"""

from __future__ import annotations

import asyncio

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
