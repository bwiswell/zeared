"""Tests for ``zeared/stream.py``: the ``Stream`` that ``alisten`` and ``aquery_iter`` return (0.4.0)."""

from __future__ import annotations

import asyncio
import gc
import logging

import pytest

import zeared as z
from zeared.stream import Stream, _open_channel


def _stream(maxsize: int = 0) -> tuple[Stream, list[int]]:
    """A stream on the running loop, with a ``close`` that counts its calls."""
    closes: list[int] = []
    channel = _open_channel(maxsize, 'Tick')
    return Stream(channel, lambda: closes.append(1)), closes


async def _drain_callbacks() -> None:
    """Let every ``call_soon_threadsafe`` hand-off run."""
    for _ in range(3):
        await asyncio.sleep(0)


class TestBound:
    def test_full_stream_drops_the_oldest_and_counts(self, caplog):
        async def main():
            stream, _ = _stream(maxsize=2)
            for i in range(5):
                stream._channel.feed(i)
            await _drain_callbacks()
            return [await anext(stream), await anext(stream)], stream.dropped

        with caplog.at_level(logging.WARNING, logger='zeared.stream'):
            got, dropped = asyncio.run(main())
        assert got == [3, 4]  # the freshest two survive
        assert dropped == 3
        warnings = [r for r in caplog.records if 'stream full' in r.getMessage()]
        assert len(warnings) == 1  # the first drop only
        assert 'Tick' in warnings[0].getMessage()
        assert 'maxsize=2' in warnings[0].getMessage()

    def test_unbounded_never_drops(self):
        async def main():
            stream, _ = _stream(maxsize=0)
            for i in range(2000):
                stream._channel.feed(i)
            await _drain_callbacks()
            return stream._channel.queue.qsize(), stream.dropped, stream.maxsize

        assert asyncio.run(main()) == (2000, 0, 0)

    def test_end_marker_is_never_dropped(self):
        async def main():
            stream, closes = _stream(maxsize=1)
            stream._channel.feed('a')
            stream._channel.end()
            await _drain_callbacks()
            items = [item async for item in stream]
            return items, closes

        items, closes = asyncio.run(main())
        assert items == ['a']
        assert closes == [1]  # reaching the end closes the source


class TestEndAndErrors:
    def test_a_source_failure_reraises_in_the_consumer(self):
        async def main():
            stream, closes = _stream()
            stream._channel.feed(1)
            stream._channel.end(ValueError('boom'))
            await _drain_callbacks()
            got = [await anext(stream)]
            with pytest.raises(ValueError, match='boom'):
                await anext(stream)
            return got, closes

        got, closes = asyncio.run(main())
        assert got == [1]
        assert closes == [1]


class TestClosing:
    def test_aclose_closes_once_and_ends_iteration(self):
        async def main():
            stream, closes = _stream()
            await stream.aclose()
            await stream.aclose()
            with pytest.raises(StopAsyncIteration):
                await anext(stream)
            return closes

        assert asyncio.run(main()) == [1]

    def test_aclose_from_another_task_ends_a_waiting_consumer(self):
        async def main():
            stream, closes = _stream()
            got: list[int] = []

            async def consume() -> None:
                got.extend([item async for item in stream])

            consumer = asyncio.create_task(consume())
            stream._channel.feed(1)
            await _drain_callbacks()  # the consumer has taken 1 and waits for more
            await stream.aclose()
            await asyncio.wait_for(consumer, timeout=1.0)
            return got, closes

        assert asyncio.run(main()) == ([1], [1])

    def test_async_with_closes(self):
        async def main():
            stream, closes = _stream()
            async with stream as s:
                assert s is stream
            return closes

        assert asyncio.run(main()) == [1]

    def test_an_abandoned_stream_closes_when_collected(self):
        """A ``break`` leaves the stream unreferenced; collecting it releases the source."""

        async def main():
            stream, closes = _stream()
            stream._channel.feed(1)
            stream._channel.feed(2)
            await _drain_callbacks()
            async for _ in stream:
                break
            del stream
            gc.collect()
            return closes

        assert asyncio.run(main()) == [1]

    def test_exported(self):
        assert z.Stream is Stream

    def test_needs_a_running_loop(self):
        with pytest.raises(RuntimeError):
            _open_channel(0, 'Tick')
