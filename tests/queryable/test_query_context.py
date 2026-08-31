"""Unit tests for ``QueryContext`` parsing helpers and the selector
renderer — no live session needed for most of these.
"""

from __future__ import annotations

import zeared as z
from zeared.queryable._query_context import _parse_params


class TestParseParams:
    def test_empty(self):
        assert _parse_params('') == {}

    def test_pairs(self):
        assert _parse_params('a=1&b=2') == {'a': '1', 'b': '2'}

    def test_blank_flag(self):
        assert _parse_params('verbose&a=1') == {'verbose': '', 'a': '1'}


class TestRenderSelector:
    def test_all_fields_provided(self):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/sel/{a}/{b}'
            a: str = z.Str(required=True)
            b: str = z.Str(required=True)

        tpl = Q._templates().canonical
        assert tpl.render_selector({'a': 'x', 'b': 'y'}) == 'q/sel/x/y'

    def test_missing_field_widens_to_star(self):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/sel2/{a}/{b}'
            a: str = z.Str(required=True)
            b: str = z.Str(required=True)

        tpl = Q._templates().canonical
        assert tpl.render_selector({'a': 'x'}) == 'q/sel2/x/*'
        assert tpl.render_selector({}) == 'q/sel2/*/*'

    def test_embedded_wildcard_passes_through(self):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/sel3/{epc}'
            epc: str = z.Str(required=True)

        tpl = Q._templates().canonical
        assert tpl.render_selector({'epc': 'E280*'}) == 'q/sel3/E280*'

    def test_static_topic(self):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/static'
            v: int = z.Int(default=0)

        tpl = Q._templates().canonical
        assert tpl.render_selector({}) == 'q/static'

    def test_named_multi_widens_to_double_star(self):
        @z.zeared
        class Q(z.Message):
            TOPIC = 'q/sel4/{tail**}'
            tail: str = z.Str(required=True)

        tpl = Q._templates().canonical
        assert tpl.render_selector({}) == 'q/sel4/**'
        assert tpl.render_selector({'tail': 'a/b/c'}) == 'q/sel4/a/b/c'
