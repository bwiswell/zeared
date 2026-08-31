"""Round-trip tests for first-class queryables — ``on_query`` (serving)
and ``query`` / ``query_one`` (getting).

Queryable + get run on the same loopback peer session, exactly as
retention's queryable answers a same-session retained-fetch.
"""

from __future__ import annotations

import pytest
from conftest import wait

import zeared as z


class TestRoundTrip:
    def test_concrete_query_returns_reply(self, session):
        @z.zeared
        class TagState(z.Message):
            TOPIC = 'q/tag/{epc}'
            epc: str = z.Str(required=True)
            x: float = z.Float(required=True)

        z.session = session
        qbl = TagState.on_query(
            lambda ctx: TagState(epc=ctx.captures['epc'], x=1.5),
        )
        wait()
        res = TagState.query(epc='E280A', timeout=2.0)
        qbl.close()

        assert len(res) == 1
        assert res[0].epc == 'E280A'
        assert res[0].x == 1.5

    def test_query_one_returns_first(self, session):
        @z.zeared
        class TagState(z.Message):
            TOPIC = 'q/one/{epc}'
            epc: str = z.Str(required=True)
            x: float = z.Float(required=True)

        z.session = session
        with TagState.on_query(lambda ctx: TagState(epc=ctx.captures['epc'], x=2.0)):
            wait()
            one = TagState.query_one(epc='AAA', timeout=2.0)
        assert one is not None
        assert one.epc == 'AAA'
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
        class TagState(z.Message):
            TOPIC = 'q/fan/{epc}'
            epc: str = z.Str(required=True)
            x: float = z.Float(required=True)

        db = {'A': 1.0, 'B': 2.0, 'C': 3.0}

        def handler(ctx):
            epc = ctx.captures.get('epc')
            if epc in db:
                return TagState(epc=epc, x=db[epc])
            # wildcard '*' → return all
            return [TagState(epc=k, x=v) for k, v in db.items()]

        z.session = session
        with TagState.on_query(handler):
            wait()
            res = TagState.query(timeout=2.0)
        got = sorted((m.epc, m.x) for m in res)
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
