"""Async façade tests — ``aon_query`` / ``aquery`` / ``aquery_one``.

zeared has no pytest-asyncio dependency; async bodies run via
``asyncio.run`` inside sync test functions (matching ``test_async_.py``).
"""

from __future__ import annotations

import asyncio

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
