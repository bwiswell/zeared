"""Tests for the tombstone feed — ``on_message(on_remove=...)`` delivering
DELETE samples (a peer's ``unretain()`` / any ``session.delete``) that the
normal ``cb`` never sees, plus ``Message.coerce_captures``.
"""

from __future__ import annotations

from conftest import wait

import zeared as z


class TestOnRemoveDelivery:
    def test_unretain_fires_on_remove_with_captures(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Reader(z.Message):
            TOPIC = 'rm/basic/{reader_id}'
            RETAINED = True
            reader_id: int = z.Int(required=True)
            zone: str = z.Str(required=True)

        # A retains two readers.
        Reader(reader_id=1, zone='dock').send(session=session_a)
        Reader(reader_id=2, zone='bay').send(session=session_a)
        wait()

        adds: list[int] = []
        removed: list[dict] = []
        sub = Reader.on_message(
            lambda m: adds.append(m.reader_id),
            on_remove=lambda meta: removed.append(meta.captures),
            session=session_b,
        )
        wait(0.3)  # retained-fetch replays existing state to cb

        # A unretains reader 1 — a live DELETE tombstone.
        Reader.unretain(reader_id=1, session=session_a)
        wait(0.3)
        sub.close()

        assert 1 in adds
        assert 2 in adds
        # on_remove saw the removal, keyed by the template capture.
        assert removed == [{'reader_id': '1'}]

    def test_no_on_remove_drops_delete_silently(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Reader(z.Message):
            TOPIC = 'rm/silent/{reader_id}'
            RETAINED = True
            reader_id: int = z.Int(required=True)
            zone: str = z.Str(required=True)

        Reader(reader_id=1, zone='dock').send(session=session_a)
        wait()

        seen: list = []
        errors: list = []
        # No on_remove — the historical silent-drop behaviour must hold.
        sub = Reader.on_message(
            lambda m: seen.append(m.reader_id),
            on_error=lambda exc, raw: errors.append(exc),
            session=session_b,
        )
        wait(0.2)
        Reader.unretain(reader_id=1, session=session_a)
        wait(0.3)
        sub.close()

        # cb fired for the retained value, never for the DELETE; no error.
        assert seen == [1]
        assert errors == []

    def test_on_remove_captures_coerce_to_typed_identity(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Reader(z.Message):
            TOPIC = 'rm/coerce/{reader_id}'
            RETAINED = True
            reader_id: int = z.Int(required=True)
            zone: str = z.Str(required=True)

        Reader(reader_id=7, zone='dock').send(session=session_a)
        wait()

        typed_ids: list[int] = []
        sub = Reader.on_message(
            lambda m: None,
            on_remove=lambda meta: typed_ids.append(
                Reader.coerce_captures(meta.captures)['reader_id'],
            ),
            session=session_b,
        )
        wait(0.2)
        Reader.unretain(reader_id=7, session=session_a)
        wait(0.3)
        sub.close()

        assert typed_ids == [7]  # int, not '7'

    def test_on_remove_exception_routes_to_on_error(self, connected_pair):
        session_a, session_b = connected_pair

        @z.zeared
        class Reader(z.Message):
            TOPIC = 'rm/err/{reader_id}'
            RETAINED = True
            reader_id: int = z.Int(required=True)
            zone: str = z.Str(required=True)

        Reader(reader_id=1, zone='dock').send(session=session_a)
        wait()

        errors: list = []

        def boom(meta):
            msg = 'handler blew up'
            raise RuntimeError(msg)

        sub = Reader.on_message(
            lambda m: None,
            on_remove=boom,
            on_error=lambda exc, raw: errors.append(exc),
            session=session_b,
        )
        wait(0.2)
        Reader.unretain(reader_id=1, session=session_a)
        wait(0.3)
        sub.close()

        assert len(errors) == 1
        assert isinstance(errors[0], z.CallbackError)
        assert isinstance(errors[0].__cause__, RuntimeError)


class TestCoerceCaptures:
    def test_declared_slot_coerced_capture_only_passthrough(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'cc/{reader_id}/{corr}'
            reader_id: int = z.Int(required=True)
            # `corr` is capture-only (no declared field).

        out = M.coerce_captures({'reader_id': '42', 'corr': 'abc'})
        assert out == {'reader_id': 42, 'corr': 'abc'}

    def test_empty_captures(self):
        @z.zeared
        class M(z.Message):
            TOPIC = 'cc/plain'

        assert M.coerce_captures({}) == {}
