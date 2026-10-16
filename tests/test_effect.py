"""Tests for ``zeared/effect.py``: the ``EFFECT`` vocabulary (0.3.12)."""

from __future__ import annotations

import random

import pytest

import zeared as z
from zeared.effect import Effect, effect_of


class TestEffect:
    def test_values_are_the_names(self):
        assert [e.value for e in Effect] == ['read', 'write', 'action']
        assert Effect('write') is Effect.WRITE
        assert f'{Effect.ACTION}' == 'action'

    def test_exported(self):
        assert z.Effect is Effect
        assert z.effect_of is effect_of

    def test_ordered_by_rank(self):
        assert Effect.READ < Effect.WRITE < Effect.ACTION
        assert Effect.ACTION > Effect.WRITE > Effect.READ
        assert Effect.WRITE <= Effect.WRITE
        assert Effect.WRITE >= Effect.WRITE
        assert not Effect.ACTION < Effect.READ  # lexically, 'action' < 'read'

    def test_sorted_and_max(self):
        effects = list(Effect) * 3
        random.Random(0).shuffle(effects)
        assert sorted(effects) == [Effect.READ] * 3 + [Effect.WRITE] * 3 + [Effect.ACTION] * 3
        assert max(effects) is Effect.ACTION
        assert min(effects) is Effect.READ

    def test_names_coerce_in_either_operand_position(self):
        # ``Effect`` subclasses ``str``, so its reflected method runs first.
        # The operand order is the point here, so SIM300 is waived.
        assert Effect.ACTION > 'read'
        assert 'read' < Effect.ACTION  # noqa: SIM300
        assert not 'action' < Effect.READ  # noqa: SIM300
        assert Effect.READ <= 'read'

    @pytest.mark.parametrize('other', ['banana', 'READ', 3, None])
    def test_other_operands_raise(self, other):
        with pytest.raises(TypeError, match='cannot order an Effect'):
            _ = Effect.READ < other  # noqa: SIM300

    def test_equality_and_hash_stay_strs(self):
        assert Effect.READ == 'read'
        assert {Effect.READ: 1}['read'] == 1


class TestEffectOf:
    def test_undeclared_is_none(self):
        class Plain:
            pass

        @z.zeared
        class Undeclared(z.Message):
            TOPIC = 'effect/undeclared'
            x: int = z.Int(default=0)

        assert effect_of(Plain) is None
        assert effect_of(Undeclared) is None
        assert z.Message.EFFECT is None

    def test_member_passes_through(self):
        class C:
            EFFECT = Effect.WRITE

        assert effect_of(C) is Effect.WRITE

    def test_name_normalises_to_its_member(self):
        class C:
            EFFECT = 'action'

        assert effect_of(C) is Effect.ACTION

    def test_unknown_name_raises_value_error_naming_the_class(self):
        class Mutating:
            EFFECT = 'mutating'

        with pytest.raises(ValueError, match=r"Mutating\.EFFECT = 'mutating': expected one of read, write, action"):
            effect_of(Mutating)

    def test_other_types_raise_type_error(self):
        class C:
            EFFECT = 1

        with pytest.raises(TypeError, match='must be an Effect, an effect name or None, not int'):
            effect_of(C)

    def test_inherited_and_overridden(self):
        class Base:
            EFFECT = Effect.ACTION

        class Inherits(Base):
            pass

        class Undeclares(Base):
            EFFECT = None

        assert effect_of(Inherits) is Effect.ACTION
        assert effect_of(Undeclares) is None
