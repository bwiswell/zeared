from __future__ import annotations

import threading

import pytest

import zeared as z
from zeared.batch import _buffer_stack, current_buffer

from conftest import wait


@pytest.fixture(autouse=True)
def _reset_batch_stack():
    """Ensure no lingering buffer between tests (in case of bugs)."""
    _buffer_stack.set(None)
    yield
    _buffer_stack.set(None)


class TestCurrentBuffer:
    def test_no_batch_active(self):
        assert current_buffer() is None

    def test_inside_batch_returns_buffer(self):
        with z.batch():
            assert current_buffer() is not None

    def test_buffer_cleared_on_exit(self):
        with z.batch():
            pass
        assert current_buffer() is None


class TestFlushOnExit:
    def test_buffered_sends_flush_together(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'batch/flush'
            v: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        with z.batch():
            for i in range(5):
                M(v=i).send()
            assert received == []  # nothing sent yet
        wait()
        sub.close()

        assert received == [0, 1, 2, 3, 4]


class TestDiscardOnException:
    def test_exception_discards_buffer(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'batch/discard'
            v: int = z.Int(required=True)

        received = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        with pytest.raises(RuntimeError, match='boom'):
            with z.batch():
                M(v=1).send()
                M(v=2).send()
                raise RuntimeError('boom')
        wait()
        sub.close()

        assert received == []
        assert current_buffer() is None


class TestExplicitFlush:
    def test_flush_drains_mid_block(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'batch/earlyflush'
            v: int = z.Int(required=True)

        received = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        with z.batch() as b:
            M(v=1).send()
            M(v=2).send()
            b.flush()  # drain explicitly
            wait()
            assert received == [1, 2]
            M(v=3).send()  # accumulates for outer-exit flush
        wait()
        sub.close()

        assert received == [1, 2, 3]


class TestFlatNesting:
    def test_inner_batch_shares_outer_buffer(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'batch/nested'
            v: int = z.Int(required=True)

        received = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        with z.batch():
            M(v=1).send()
            with z.batch():
                M(v=2).send()
                # Inner exits here → no flush (outer owns).
            assert received == []  # still nothing sent
            M(v=3).send()
        wait()
        sub.close()

        assert received == [1, 2, 3]

    def test_exception_in_inner_discards_all(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'batch/nested_exc'
            v: int = z.Int(required=True)

        received = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        with pytest.raises(RuntimeError):
            with z.batch():
                M(v=1).send()
                with z.batch():
                    M(v=2).send()
                    raise RuntimeError('boom')
        wait()
        sub.close()

        assert received == []  # exception propagates to outer → discard
        assert current_buffer() is None


class TestMixedSession:
    def test_partitioned_flush(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class M(z.Message):
            TOPIC = 'batch/mixed'
            v: int = z.Int(required=True)

        got_a: list[int] = []
        got_b: list[int] = []
        sub_a = M.on_message(lambda m: got_a.append(m.v), session=session_a)
        sub_b = M.on_message(lambda m: got_b.append(m.v), session=session_b)
        wait()

        with z.batch():
            M(v=1).send(session=session_a)
            M(v=2).send(session=session_b)
            M(v=3).send(session=session_a)
        wait()

        sub_a.close()
        sub_b.close()
        assert got_a == [1, 3]
        assert got_b == [2]


class TestThreadIsolation:
    def test_buffers_are_thread_local(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'batch/threads'
            v: int = z.Int(required=True)

        received = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        main_had_buffer = []
        worker_had_buffer = []

        def worker():
            worker_had_buffer.append(current_buffer() is not None)
            M(v=100).send()  # outside any batch on this thread — flushes immediately
            worker_had_buffer.append(current_buffer() is not None)

        with z.batch():
            main_had_buffer.append(current_buffer() is not None)
            M(v=1).send()
            t = threading.Thread(target=worker)
            t.start()
            t.join()
        wait()
        sub.close()

        assert main_had_buffer == [True]
        assert worker_had_buffer == [False, False]
        assert received == [100, 1]


class TestSendBatch:
    def test_same_class_bulk(self, session):
        @z.zeared
        class M(z.Message):
            TOPIC = 'sendbatch/basic'
            v: int = z.Int(required=True)

        received: list[int] = []
        z.session = session
        sub = M.on_message(lambda m: received.append(m.v))
        wait()

        M.send_batch([M(v=i) for i in range(5)])
        wait()
        sub.close()

        assert received == [0, 1, 2, 3, 4]

    def test_rejects_wrong_type(self, session):
        @z.zeared
        class A(z.Message):
            TOPIC = 'sendbatch/a'
            v: int = z.Int(required=True)

        @z.zeared
        class B(z.Message):
            TOPIC = 'sendbatch/b'
            v: int = z.Int(required=True)

        z.session = session
        with pytest.raises(TypeError, match='expected A'):
            A.send_batch([A(v=1), B(v=2)])

    def test_session_propagates(self, session_pair):
        session_a, session_b = session_pair

        @z.zeared
        class M(z.Message):
            TOPIC = 'sendbatch/sess'
            v: int = z.Int(required=True)

        got_a: list[int] = []
        got_b: list[int] = []
        sub_a = M.on_message(lambda m: got_a.append(m.v), session=session_a)
        sub_b = M.on_message(lambda m: got_b.append(m.v), session=session_b)
        wait()

        M.send_batch([M(v=1), M(v=2), M(v=3)], session=session_b)
        wait()

        sub_a.close()
        sub_b.close()
        assert got_a == []
        assert got_b == [1, 2, 3]
