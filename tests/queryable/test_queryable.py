"""Round-trip tests for first-class queryables — ``on_query`` (serving)
and ``query`` / ``query_one`` (getting).

Queryable + get run on the same loopback peer session, exactly as
retention's queryable answers a same-session retained-fetch.
"""

from __future__ import annotations

import gc
import logging
import time

import pytest
from conftest import wait

import zeared as z


class TestRoundTrip:
    def test_concrete_query_returns_reply(self, session):
        @z.zeared
        class Position(z.Message):
            TOPIC = 'q/tag/{robot}'
            robot: str = z.Str(required=True)
            x: float = z.Float(required=True)

        z.session = session
        qbl = Position.on_query(
            lambda ctx: Position(robot=ctx.captures['robot'], x=1.5),
        )
        wait()
        res = Position.query(robot='r-1', timeout=2.0)
        qbl.close()

        assert len(res) == 1
        assert res[0].robot == 'r-1'
        assert res[0].x == 1.5

    def test_query_one_returns_first(self, session):
        @z.zeared
        class Position(z.Message):
            TOPIC = 'q/one/{robot}'
            robot: str = z.Str(required=True)
            x: float = z.Float(required=True)

        z.session = session
        with Position.on_query(lambda ctx: Position(robot=ctx.captures['robot'], x=2.0)):
            wait()
            one = Position.query_one(robot='r-2', timeout=2.0)
        assert one is not None
        assert one.robot == 'r-2'
        assert one.x == 2.0

    def test_query_one_none_when_no_answer(self, session):
        @z.zeared
        class Nobody(z.Message):
            TOPIC = 'q/nobody/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        z.session = session
        assert Nobody.query_one(id='x', timeout=0.5) is None

    def test_wildcard_query_fans_out(self, session):
        @z.zeared
        class Position(z.Message):
            TOPIC = 'q/fan/{robot}'
            robot: str = z.Str(required=True)
            x: float = z.Float(required=True)

        db = {'A': 1.0, 'B': 2.0, 'C': 3.0}

        def handler(ctx):
            robot = ctx.captures.get('robot')
            if robot in db:
                return Position(robot=robot, x=db[robot])
            # wildcard '*' → return all
            return [Position(robot=k, x=v) for k, v in db.items()]

        z.session = session
        with Position.on_query(handler):
            wait()
            res = Position.query(timeout=2.0)
        got = sorted((m.robot, m.x) for m in res)
        assert got == [('A', 1.0), ('B', 2.0), ('C', 3.0)]

    def test_handler_returning_none_replies_nothing(self, session):
        @z.zeared
        class Maybe(z.Message):
            TOPIC = 'q/maybe/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        z.session = session
        with Maybe.on_query(lambda ctx: None):
            wait()
            res = Maybe.query(id='x', timeout=0.5)
        assert res == []

    def test_explicit_reply_form(self, session):
        @z.zeared
        class Multi(z.Message):
            TOPIC = 'q/multi/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        def handler(ctx):
            # Reply explicitly, twice, then return None.
            for v in (10, 20):
                ctx.reply(Multi(id=ctx.captures['id'], v=v))
            return

        z.session = session
        with Multi.on_query(handler):
            wait()
            res = Multi.query(id='k', timeout=2.0)
        assert sorted(m.v for m in res) == [10, 20]


class TestGeneratorHandler:
    """The streaming shape of the return form.

    A generator handler is consumed lazily by ``_reply_result``, so replies
    go out as they are yielded and the handler never materialises the full
    result set. These pin the two properties that makes worth having:
    every yielded item is replied, and a mid-stream raise is reported
    rather than silently truncating the answer.
    """

    def test_generator_replies_every_yield(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/gen/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        def handler(ctx):
            for v in range(4):
                yield Row(id=ctx.captures['id'], v=v)

        z.session = session
        with Row.on_query(handler):
            wait()
            res = Row.query(id='k', timeout=2.0)
        assert sorted(m.v for m in res) == [0, 1, 2, 3]

    def test_generator_raising_midstream_is_reported(self, session):
        """Regression: the raise escaped into Zenoh's callback.

        ``handler(ctx)`` returns the generator object without executing a
        line of it, so ``dispatch``'s try/except sees nothing; the body
        only runs later, as ``_reply_result`` advances the iterator. An
        unguarded raise there surfaced as a "zenoh.handlers: callback
        error" traceback on stderr — ``on_error`` never fired, no error
        reply was sent, and the getter received the partial stream as
        though it were the whole answer.
        """

        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/genboom/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        def handler(ctx):
            yield Row(id=ctx.captures['id'], v=0)
            yield Row(id=ctx.captures['id'], v=1)
            msg = 'boom mid-stream'
            raise RuntimeError(msg)

        served: list[Exception] = []
        got: list[Exception] = []

        z.session = session
        with Row.on_query(handler, on_error=lambda e, raw: served.append(e)):
            wait()
            res = Row.query(id='k', timeout=1.0, on_error=lambda e, raw: got.append(e))

        # Replies emitted before the raise still land.
        assert sorted(m.v for m in res) == [0, 1]
        # Serving side sees the handler error...
        assert any(isinstance(e, z.QueryableError) and 'boom mid-stream' in str(e) for e in served), served
        # ...and the getter is told the stream was truncated, rather than
        # believing [0, 1] was a complete answer.
        assert any(isinstance(e, z.QueryError) and 'boom mid-stream' in str(e) for e in got), got

    def test_a_raising_on_error_still_sends_the_error_reply(self, session, caplog):
        """Regression: the serving ``on_error`` was called unguarded, so one that raised
        escaped into Zenoh's callback and skipped the ``reply_err``: the getter took the
        cut-off stream for the whole answer."""

        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/badonerror/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        def handler(ctx):
            yield Row(id=ctx.captures['id'], v=0)
            msg = 'boom mid-stream'
            raise RuntimeError(msg)

        def on_error(exc, raw):
            msg = 'on_error broke too'
            raise ValueError(msg)

        got: list[Exception] = []
        z.session = session
        with caplog.at_level(logging.ERROR, logger='zeared.queryable'), Row.on_query(handler, on_error=on_error):
            wait()
            res = Row.query(id='k', timeout=1.0, on_error=lambda e, raw: got.append(e))

        assert [m.v for m in res] == [0]
        assert any(isinstance(e, z.QueryError) and 'boom mid-stream' in str(e) for e in got), got
        logged = [r for r in caplog.records if 'on_error callback itself raised' in r.getMessage()]
        assert logged
        assert 'boom mid-stream' in logged[0].getMessage()


class TestParamsAndRequest:
    def test_params_reach_handler(self, session):
        seen = {}

        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/params/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        def handler(ctx):
            seen.update(ctx.params)
            return Q(id=ctx.captures['id'], v=1)

        z.session = session
        with Q.on_query(handler):
            wait()
            Q.query(id='a', params={'lo': '5', 'hi': '9'}, timeout=2.0)
        assert seen == {'lo': '5', 'hi': '9'}

    def test_typed_request_payload(self, session):
        @z.zeared
        class Req(z.Message):
            TOPIC = 'q/req/unused'
            algo: str = z.Str(default='trilat')

        @z.zeared
        class Resp(z.Message):
            TOPIC = 'q/req/{id}'
            REQUEST = Req
            id: str = z.Str(required=True)
            algo_echo: str = z.Str(default='')

        def handler(ctx):
            assert isinstance(ctx.request, Req)
            return Resp(id=ctx.captures['id'], algo_echo=ctx.request.algo)

        z.session = session
        with Resp.on_query(handler):
            wait()
            res = Resp.query(id='z', request=Req(algo='ml'), timeout=2.0)
        assert res
        assert res[0].algo_echo == 'ml'


class TestErrorsAndConflicts:
    def test_error_reply_routed_to_on_error(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/err/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        z.session = session
        with Q.on_query(lambda ctx: ctx.reply_err('boom')):
            wait()
            errs = []
            res = Q.query(id='a', timeout=2.0, on_error=lambda e, raw: errs.append((e, raw)))
        assert res == []
        assert len(errs) == 1
        assert isinstance(errs[0][0], z.QueryError)
        assert errs[0][1] == b'boom'

    def test_handler_raise_sends_error_reply(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/raise/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        def boom(ctx):
            msg = 'kaboom'
            raise RuntimeError(msg)

        z.session = session
        with Q.on_query(boom):
            wait()
            errs = []
            res = Q.query(id='a', timeout=2.0, on_error=lambda e, raw: errs.append(e))
        assert res == []
        assert errs
        assert isinstance(errs[0], z.QueryError)

    def test_retained_class_rejects_on_query(self, session):
        @z.zeared
        class Ret(z.Message):
            TOPIC = 'q/ret/{id}'
            RETAINED = True
            id: str = z.Str(required=True)

        z.session = session
        with pytest.raises(z.TopicError):
            Ret.on_query(lambda ctx: None)


class TestFailedReply:
    """A reply that fails, or a context that fails to build, lets the query finish (0.4.1).

    Each failure path wrapped its exception in a local whose ``__cause__``'s
    traceback held that same frame, which held the ``zenoh.Query``: a cycle
    only the GC could free. Until it ran the query stayed open, so a getter
    against an idle server waited out its whole timeout, then took a
    ``Timeout`` error reply after the good replies. The GC is off here, so
    only breaking the cycle can end the query early.
    """

    @pytest.fixture
    def no_gc(self):
        gc.disable()
        try:
            yield
        finally:
            gc.enable()

    @pytest.mark.parametrize('form', ['list', 'generator'])
    def test_a_failed_reply_lets_the_query_finish(self, session, no_gc, form):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/failed/{name}'
            name: str = z.Str(required=True)
            v: int = z.Int(required=True)

        rows = [Row(name='a', v=0), Row(name='x/y', v=1), Row(name='b', v=2)]  # 'x/y' splits its key

        def as_list(ctx):
            return rows

        def as_generator(ctx):
            yield from rows

        failures: list[type] = []
        errors: list[Exception] = []
        z.session = session
        handler = as_list if form == 'list' else as_generator
        with Row.on_query(handler, on_error=lambda e, raw: failures.append(type(e.__cause__))):
            wait()
            started = time.monotonic()
            res = Row.query(timeout=2.0, on_error=lambda e, raw: errors.append(e))
            elapsed = time.monotonic() - started

        assert sorted(m.v for m in res) == [0, 2]
        assert failures == [z.TopicError]
        assert not errors, errors
        assert elapsed < 1.0, f'the getter waited {elapsed:.2f}s for a query the server had finished'

    def test_a_context_that_fails_to_build_lets_the_query_finish(self, session, no_gc, monkeypatch):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/noctx/{id}'
            id: str = z.Str(required=True)

        def cannot_build(query, msg_cls):
            msg = 'cannot build context'
            raise RuntimeError(msg)

        monkeypatch.setattr('zeared.queryable._query_dispatch.QueryContext', cannot_build)
        failures: list[str] = []
        errors: list[Exception] = []
        z.session = session
        with Q.on_query(lambda ctx: None, on_error=lambda e, raw: failures.append(str(e))):
            wait()
            started = time.monotonic()
            res = Q.query(id='a', timeout=2.0, on_error=lambda e, raw: errors.append(e))
            elapsed = time.monotonic() - started

        assert res == []
        assert failures == ['Q: failed to build query context: cannot build context']
        assert not errors, errors
        assert elapsed < 1.0, f'the getter waited {elapsed:.2f}s for a query the server had dropped'


class TestEncodingAndSchema:
    def test_json_class_roundtrips(self, session):
        @z.zeared
        class J(z.Message):
            TOPIC = 'q/json/{id}'
            ENCODING = 'json'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        with J.on_query(lambda ctx: J(id=ctx.captures['id'], v=7)):
            wait()
            res = J.query(id='a', timeout=2.0)
        assert res
        assert res[0].v == 7

    def test_schema_stamped_and_matched(self, session):
        @z.zeared
        class S(z.Message):
            TOPIC = 'q/schema/{id}'
            SCHEMA = '1'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        with S.on_query(lambda ctx: S(id=ctx.captures['id'], v=5)):
            wait()
            res = S.query(id='a', timeout=2.0)
        assert res
        assert res[0].v == 5


class TestExtraTopics:
    def test_extra_topic_is_queryable(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/primary/{id}'
            EXTRA_TOPICS = ('q/alt/{id}',)
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        qbl = Q.on_query(lambda ctx: Q(id=ctx.captures['id'], v=99))
        # Two zenoh queryables — one per declared template.
        assert len(qbl._zenoh_queryables) == 2
        wait()
        # A query on the *extra* template must be answered on that same
        # template (the reply key must intersect the query key-expr).
        # query() renders from the canonical template, so issue the alt
        # get directly to confirm the reply lands on the alt key.
        replies = session.get('q/alt/k', timeout=2.0)
        got = [str(r.ok.key_expr) for r in replies if getattr(r, 'ok', None)]
        qbl.close()
        assert got == ['q/alt/k']


class TestHandleLifecycle:
    def test_close_is_idempotent(self, session):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/close/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(default=0)

        z.session = session
        qbl = Q.on_query(lambda ctx: None)
        qbl.close()
        qbl.close()  # no raise
        assert qbl._closed


class TestIterQuery:
    """``iter_query`` — replies yielded as they arrive.

    ``query`` is ``list()`` over this, so the decode and error-routing
    paths are shared by construction; these pin the streaming property
    itself and the two documented asymmetries.
    """

    def test_yields_same_rows_as_query(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/iter/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        def handler(ctx):
            for v in range(4):
                yield Row(id=ctx.captures['id'], v=v)

        z.session = session
        with Row.on_query(handler):
            wait()
            streamed = sorted(m.v for m in Row.iter_query(id='k', timeout=2.0))
            collected = sorted(m.v for m in Row.query(id='k', timeout=2.0))
        assert streamed == collected == [0, 1, 2, 3]

    def test_returns_a_lazy_iterator_not_a_list(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/iterlazy/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        with Row.on_query(lambda ctx: Row(id=ctx.captures['id'], v=1)):
            wait()
            it = Row.iter_query(id='k', timeout=2.0)
            assert not isinstance(it, list)
            assert iter(it) is not None
            assert [m.v for m in it] == [1]

    def test_first_row_arrives_before_the_last_is_produced(self, session):
        """The whole point: time-to-first-usable-reply beats the window.

        The handler spaces four replies 0.3s apart. ``query`` cannot
        return before the last one (~1.2s); ``iter_query`` must hand over
        the first well before that.
        """

        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/itertiming/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        def handler(ctx):
            for v in range(4):
                time.sleep(0.3)
                yield Row(id=ctx.captures['id'], v=v)

        z.session = session
        with Row.on_query(handler):
            wait()
            t0 = time.monotonic()
            it = Row.iter_query(id='k', timeout=5.0)
            first = next(iter(it))
            t_first = time.monotonic() - t0

            t1 = time.monotonic()
            rows = Row.query(id='k', timeout=5.0)
            t_all = time.monotonic() - t1

        assert first.v == 0
        assert len(rows) == 4
        # First streamed reply lands around 0.3s; the collected call can't
        # return before ~1.2s. Generous margin — this is about the shape,
        # not a benchmark.
        assert t_first < t_all, f'no streaming benefit: first={t_first:.2f}s all={t_all:.2f}s'
        assert t_first < 0.9, f'first reply took {t_first:.2f}s — not streaming'


class TestQueryOneShortCircuit:
    """``query_one`` returns at the first decoded reply (0.3.4).

    It used to collect every reply for the full ``timeout`` and then take
    ``[0]``, so it always paid the whole window even when the first
    answer arrived immediately.
    """

    def test_returns_before_the_window_closes(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/one/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        def handler(ctx):
            yield Row(id=ctx.captures['id'], v=0)
            time.sleep(1.5)
            yield Row(id=ctx.captures['id'], v=1)

        z.session = session
        with Row.on_query(handler):
            wait()
            t0 = time.monotonic()
            one = Row.query_one(id='k', timeout=4.0)
            elapsed = time.monotonic() - t0

        assert one is not None
        assert one.v == 0
        assert elapsed < 1.2, f'query_one waited {elapsed:.2f}s — did not short-circuit'

    def test_still_none_when_nobody_answers(self, session):
        @z.zeared
        class Row(z.Message):
            TOPIC = 'q/onenone/{id}'
            id: str = z.Str(required=True)
            v: int = z.Int(required=True)

        z.session = session
        assert Row.query_one(id='nobody', timeout=0.5) is None
