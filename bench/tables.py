"""Render the benchmark numbers in the docs from ``bench/results.json``.

The README and ``docs/overview/benchmarks.md`` carry them between
``<!-- bench:NAME -->`` / ``<!-- /bench:NAME -->`` markers: everything inside a
pair is generated, everything outside it is prose. A *table block* sits on lines
of its own; an *inline block* wraps just a number in a sentence (HTML comments
don't render), so the prose stays hand-written and only the number is generated.

Every number is a median over the report's runs (``python -m bench --runs N``),
and every ratio is a ratio of medians. ``python -m bench`` re-renders the docs
after recording to the committed artifact; ``python -m bench.tables`` does it
from the artifact alone, and ``--check`` exits non-zero when a doc has drifted
from it. Hand-copied numbers had drifted for three releases before 0.3.11.
"""

from __future__ import annotations

import argparse
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

from .harness import Report

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

_ROOT = Path(__file__).parent.parent
RESULTS = Path(__file__).parent / 'results.json'
DOCS = (_ROOT / 'README.md', _ROOT / 'docs' / 'overview' / 'benchmarks.md')

#: Spelled by name: ruff (RUF001) reads the literal sign as a typo for ``x``.
_TIMES = '\N{MULTIPLICATION SIGN}'

_BLOCK = re.compile(r'(<!-- bench:(?P<name>[\w-]+) -->)(?P<body>.*?)(<!-- /bench:(?P=name) -->)', re.DOTALL)

# Per suite: (strategy as recorded, label in the docs), in display order. A
# strategy a report has no measurement for (an uninstalled comparator) drops
# out of its table.
_WIRE = (
    ('zenoh + marshmallow (json)', 'Zenoh + `marshmallow` (JSON)'),
    ('zenoh + zeared (json, cached)', '`zeared` (JSON, cached)'),
    ('zenoh + zeared (msgpack, cached)', '`zeared` (msgpack, cached)'),
    ('zenoh + zeared (msgpack, PUBLISHER=False)', '`zeared` (msgpack, `PUBLISHER=False`)'),
)
_THROUGHPUT = (
    ('zenoh + marshmallow (json)', 'Zenoh + `marshmallow` (JSON)'),
    ('sync (json, cached)', 'sync `send` (JSON, cached)'),
    ('sync (msgpack, cached)', 'sync `send` (msgpack, cached)'),
    ('sync (msgpack, PUBLISHER=False)', 'sync `send` (msgpack, `PUBLISHER=False`)'),
)
_ASYNC = (
    ('zenoh + marshmallow (json)', 'Zenoh + `marshmallow` (JSON)'),
    ('sync send + sync on_message (json)', 'sync `send` + sync `on_message` (JSON)'),
    ('sync send + sync on_message (msgpack)', 'sync `send` + sync `on_message` (msgpack)'),
    ('asend + sync on_message (msgpack)', '`asend` + sync `on_message` (msgpack)'),
    ('sync send + alisten (msgpack)', 'sync `send` + `alisten` (msgpack)'),
    ('asend + alisten (msgpack)', '`asend` + `alisten` (msgpack)'),
    ('asend + alisten (json)', '`asend` + `alisten` (JSON)'),
    ('sync send + async-def on_message (msgpack)', 'sync `send` + `async def` `on_message` (msgpack)'),
)
_RUSTED = (
    ('zenoh + zeared + rusted (json, cached)', '`zeared` + `rusted` (JSON, cached)'),
    ('zenoh + zeared + rusted (msgpack, cached)', '`zeared` + `rusted` (msgpack, cached)'),
)
_STACKS = (
    ('pydantic + MQTT (QoS 0)', 'pydantic + MQTT (QoS 0)'),
    ('pydantic + MQTT (QoS 1)', 'pydantic + MQTT (QoS 1)'),
    ('seared + MQTT (QoS 0)', 'seared + MQTT (QoS 0)'),
    ('seared + MQTT (QoS 1)', 'seared + MQTT (QoS 1)'),
    ('pydantic + Zenoh (router)', 'pydantic + Zenoh (router, raw)'),
    ('seared + Zenoh (router, raw)', 'seared + Zenoh (router, raw)'),
    ('zeared + Zenoh (router)', 'zeared + Zenoh (router)'),
    ('zeared + Zenoh (peer)', 'zeared + Zenoh (peer)'),
    ('zeared + Zenoh (peer, msgpack default)', 'zeared + Zenoh (peer, msgpack default)'),
    ('zeared + rusted + Zenoh (peer, json)', 'zeared + `rusted` + Zenoh (peer, JSON)'),
    ('zeared + rusted + Zenoh (peer, msgpack default)', 'zeared + `rusted` + Zenoh (peer, msgpack default)'),
)

# The rows the derived numbers compare, by role.
_MARSHMALLOW = ('wire', 'zenoh + marshmallow (json)')
_WIRE_JSON = ('wire', 'zenoh + zeared (json, cached)')
_WIRE_MSGPACK = ('wire', 'zenoh + zeared (msgpack, cached)')
_ASYNC_FASTEST = ('async', 'sync send + sync on_message (msgpack)')
_PYD_Q0 = ('stacks', 'pydantic + MQTT (QoS 0)')
_PYD_Q1 = ('stacks', 'pydantic + MQTT (QoS 1)')
_SEARED_Q0 = ('stacks', 'seared + MQTT (QoS 0)')
_PYD_ZENOH = ('stacks', 'pydantic + Zenoh (router)')
_SEARED_ZENOH = ('stacks', 'seared + Zenoh (router, raw)')
_ZEARED_ROUTER = ('stacks', 'zeared + Zenoh (router)')
_ZEARED_PEER = ('stacks', 'zeared + Zenoh (peer)')
_ZEARED_DEFAULT = ('stacks', 'zeared + Zenoh (peer, msgpack default)')
_RUSTED_DEFAULT = ('stacks', 'zeared + rusted + Zenoh (peer, msgpack default)')

_NA = 'n/a'


class Results:
    """A report's measurements as medians and spread over its runs."""

    def __init__(self, report: Report) -> None:
        """Group ``report``'s values by ``(suite, strategy, metric)``, one per run."""
        values: dict[tuple[str, str, str], list[float]] = defaultdict(list)
        for m in report.measurements:
            values[m.suite, m.strategy, m.metric].append(m.value)
        self.report = report
        self._values = dict(values)

    def has(self, row: tuple[str, str]) -> bool:
        """Whether ``row`` (``(suite, strategy)``) was measured."""
        return (*row, 'e2e_rate') in self._values

    def rows(self) -> list[tuple[str, str]]:
        """Every measured ``(suite, strategy)``."""
        return [(suite, strategy) for suite, strategy, metric in self._values if metric == 'e2e_rate']

    def median(self, row: tuple[str, str], metric: str = 'e2e_rate') -> float | None:
        """The median of ``metric`` for ``row`` (``(suite, strategy)``), or ``None`` if unmeasured."""
        values = self._values.get((*row, metric))
        return statistics.median(values) if values else None

    def spread(self, row: tuple[str, str]) -> float | None:
        """How far ``row``'s end-to-end rate moved across the runs: max/min - 1."""
        values = self._values.get((*row, 'e2e_rate'))
        if not values or min(values) <= 0:
            return None
        return max(values) / min(values) - 1

    def ratio(self, numerator: tuple[str, str], denominator: tuple[str, str], metric: str = 'e2e_rate') -> float | None:
        """``median(numerator) / median(denominator)``, or ``None`` if either is unmeasured."""
        top, bottom = self.median(numerator, metric), self.median(denominator, metric)
        if top is None or bottom is None or bottom == 0:
            return None
        return top / bottom


# --- formatting -------------------------------------------------------------


def _fmt(value: float | None, render: Callable[[float], str]) -> str:
    return _NA if value is None else render(value)


def _rate(v: float | None) -> str:
    return _fmt(v, lambda x: f'{x:,.0f}')


def _pct(v: float | None) -> str:
    """A fraction as a whole percentage: ``0.428`` → ``43%``."""
    return _fmt(v, lambda x: f'{x * 100:.0f}%')


def _less(v: float | None) -> float | None:
    """A ratio as how much smaller it is: ``0.57`` → ``0.43``."""
    return None if v is None else 1 - v


def _more(v: float | None) -> float | None:
    """A ratio as how much larger it is: ``1.49`` → ``0.49``."""
    return None if v is None else v - 1


def _x(v: float | None, places: int = 1, sign: str = 'x') -> str:
    return _fmt(v, lambda x: f'{x:.{places}f}{sign}')


def _approx(rendered: str) -> str:
    """A rounded prose number, marked ``~``; an unmeasured one stays plain ``n/a``."""
    return rendered if rendered == _NA else f'~{rendered}'


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]], *, right: set[int], pad: bool = False) -> str:
    """A Markdown table in one of the docs' two house styles.

    Unpadded (``pad=False``): cells as they are, each rule cell as wide as its
    header plus two. Padded: every cell left-justified to its column's widest,
    and the rule sized to match. A right-aligned column's rule ends in ``:``.
    """
    if pad:
        widths = [max(len(r[i]) for r in [headers, *rows]) for i in range(len(headers))]
    else:
        widths = [len(h) for h in headers]

    def line(cells: Sequence[str]) -> str:
        if pad:
            cells = [c.ljust(w) for c, w in zip(cells, widths, strict=True)]
        return '| ' + ' | '.join(cells) + ' |'

    rule = '|' + '|'.join(('-' * (w + 1) + ':') if i in right else '-' * (w + 2) for i, w in enumerate(widths)) + '|'
    return '\n'.join([line(headers), rule, *(line(r) for r in rows)])


# --- table blocks -----------------------------------------------------------


def rate_table(results: Results, suite: str, labels: Sequence[tuple[str, str]]) -> str:
    """pub/s, e2e/s, MB/s and wire bytes per strategy: the wire, throughput, async and ``rusted`` tables."""
    rows = []
    for strategy, label in labels:
        row = (suite, strategy)
        if not results.has(row):
            continue
        mbps = results.median(row, 'mb_per_s')
        wire = results.median(row, 'wire_bytes')
        rows.append(
            [
                label,
                _rate(results.median(row, 'pub_rate')),
                _rate(results.median(row)),
                _fmt(mbps, lambda x: f'{x:.2f}'),
                _fmt(wire, lambda x: f'{x:.0f}'),
            ]
        )
    return _table(['Strategy', 'pub/s', 'e2e/s', 'MB/s', 'wire (B)'], rows, right={1, 2, 3, 4})


def stacks_table(results: Results) -> str:
    """End-to-end rate and wire bytes for every stack comparison row."""
    rows = []
    for strategy, label in _STACKS:
        row = ('stacks', strategy)
        if results.has(row):
            rows.append(
                [label, _rate(results.median(row)), _fmt(results.median(row, 'wire_bytes'), lambda x: f'{x:.0f}')]
            )
    return _table(['Row', 'e2e/s', 'wire (B)'], rows, right={1, 2})


def pairs_table(results: Results) -> str:
    """What each pair of stacks rows says, one axis held fixed at a time."""
    r = results
    smaller = _less(r.ratio(_ZEARED_DEFAULT, _ZEARED_PEER, 'wire_bytes'))
    rows = [
        [
            '**Codec**, same raw Zenoh transport',
            f'pydantic **{_x(r.ratio(_PYD_ZENOH, _SEARED_ZENOH))}** faster than seared',
        ],
        ['**Codec**, same MQTT transport (QoS 0)', f'pydantic **{_x(r.ratio(_PYD_Q0, _SEARED_Q0))}** faster'],
        ['**Transport**, same pydantic codec', f'Zenoh **{_x(r.ratio(_PYD_ZENOH, _PYD_Q0))}** faster than MQTT'],
        [
            "zeared's `Message` wrapper over raw seared + Zenoh",
            f'**{_pct(_less(r.ratio(_ZEARED_ROUTER, _SEARED_ZENOH)))}** cost',
        ],
        ['Zenoh peer vs router topology', f'**{_pct(_more(r.ratio(_ZEARED_PEER, _ZEARED_ROUTER)))}** faster'],
        [
            'msgpack vs JSON on the same path',
            f'**{_pct(_more(r.ratio(_ZEARED_DEFAULT, _ZEARED_PEER)))}** faster, **{_pct(smaller)}** smaller',
        ],
        ['`rusted` accelerator on the native stack', f'**{_x(r.ratio(_RUSTED_DEFAULT, _ZEARED_DEFAULT))}**'],
    ]
    return _table(['Comparison', 'Result'], rows, right=set())


def spread_table(results: Results) -> str:
    """How far each stacks row's end-to-end rate moved between runs."""
    rows = [
        [label, _pct(results.spread(('stacks', strategy)))]
        for strategy, label in _STACKS
        if results.has(('stacks', strategy))
    ]
    return _table(['Row', 'spread'], rows, right={1})


def answer_table(results: Results) -> str:
    """zeared, default and with ``rusted``, against pydantic + MQTT at QoS 0 and QoS 1."""
    r = results
    rows = [
        [
            'default (msgpack, peer)',
            f'**{_x(r.ratio(_ZEARED_DEFAULT, _PYD_Q0), 2)}**',
            f'**{_x(r.ratio(_ZEARED_DEFAULT, _PYD_Q1), 2)}**',
        ],
        [
            'with `rusted`',
            f'**{_x(r.ratio(_RUSTED_DEFAULT, _PYD_Q0), 2)}**',
            f'**{_x(r.ratio(_RUSTED_DEFAULT, _PYD_Q1), 2)}**',
        ],
    ]
    return _table(['zeared config', 'vs pydantic + MQTT QoS 0', 'vs QoS 1'], rows, right={1, 2})


def readme_headline(results: Results) -> str:
    """The README's two-row headline: marshmallow vs zeared's default path (wire suite)."""
    rows = []
    for row, label in (
        (_MARSHMALLOW, 'Zenoh + `marshmallow` (JSON)'),
        (_WIRE_MSGPACK, '`zeared` sync (msgpack, cached)'),
    ):
        if results.has(row):
            rows.append(
                [
                    label,
                    _rate(results.median(row, 'pub_rate')),
                    _fmt(results.median(row, 'wire_bytes'), lambda x: f'{x:.0f}'),
                ]
            )
    return _table(['Strategy', 'pub/s', 'wire (B)'], rows, right={1, 2}, pad=True)


# --- inline blocks ----------------------------------------------------------


def run_line(report: Report) -> str:
    """The provenance of every number: versions, runs, and when."""
    versions: dict[str, str] = {}
    for m in report.measurements:
        for token in m.version.split('/'):
            name, _, version = token.strip().partition(' ')
            versions.setdefault(name, version)
    parts = [f'zeared {versions["zeared"]}'] if 'zeared' in versions else []
    if 'seared' in versions:
        parts.append(f'seared {versions["seared"]}')
    if 'rusted' in versions:
        parts.append(f'`rusted` {versions["rusted"]}')
    parts += [f'`eclipse-zenoh` {report.zenoh}', f'CPython {report.python}']
    runs = 'one run' if report.runs == 1 else f'the median of {report.runs} runs'
    return f'{", ".join(parts)}; {runs}, recorded {report.timestamp[:10]}'


def _runs_parenthetical(report: Report) -> str:
    if report.runs == 1:
        return '(one run, here and below)'
    words = {2: 'two', 3: 'three', 4: 'four', 5: 'five'}
    return f'(medians of {words.get(report.runs, str(report.runs))} runs, here and below)'


def _largest_spread(results: Results) -> float | None:
    """The most any row's end-to-end rate moved between runs, across every suite."""
    spreads = [s for s in (results.spread(row) for row in results.rows()) if s is not None]
    return max(spreads) if spreads else None


def _enqueue_overstatement(r: Results, row: tuple[str, str]) -> float | None:
    """How far a row's publish rate overstates its end-to-end rate (``paho`` returns at enqueue)."""
    pub, e2e = r.median(row, 'pub_rate'), r.median(row)
    return None if pub is None or not e2e else pub / e2e


def _largest_gap(results: Results) -> float | None:
    """The widest pub/s-to-e2e/s gap in the async suite, as a fraction of pub/s."""
    gaps = []
    for strategy, _ in _ASYNC:
        row = ('async', strategy)
        pub, e2e = results.median(row, 'pub_rate'), results.median(row)
        if pub and e2e is not None:
            gaps.append(1 - e2e / pub)
    return max(gaps) if gaps else None


def blocks(report: Report) -> dict[str, str]:
    """Every generated block by marker name. Table blocks sit on lines of their own."""
    r = Results(report)
    fastest = _ASYNC_FASTEST
    gap = _largest_gap(r)
    return {
        # Table blocks.
        'readme-headline': f'\n{readme_headline(r)}\n',
        'wire': f'\n{rate_table(r, "wire", _WIRE)}\n',
        'throughput': f'\n{rate_table(r, "throughput", _THROUGHPUT)}\n',
        'async': f'\n{rate_table(r, "async", _ASYNC)}\n',
        'rusted': f'\n{rate_table(r, "rusted", _RUSTED)}\n',
        'stacks': f'\n{stacks_table(r)}\n',
        'pairs': f'\n{pairs_table(r)}\n',
        'spread': f'\n{spread_table(r)}\n',
        'answer': f'\n{answer_table(r)}\n',
        # Inline blocks, provenance.
        'run': run_line(report),
        'runs': str(report.runs),
        'spread-max': _pct(_largest_spread(r)),
        'readme-runs': _runs_parenthetical(report),
        # Inline blocks, the README.
        'readme-faster': _approx(_pct(_more(r.ratio(_WIRE_MSGPACK, _MARSHMALLOW, 'pub_rate')))),
        'readme-smaller': _approx(_pct(_less(r.ratio(_WIRE_MSGPACK, _MARSHMALLOW, 'wire_bytes')))),
        'readme-codec': _approx(_x(r.ratio(_PYD_ZENOH, _SEARED_ZENOH))),
        'readme-transport': _approx(_x(r.ratio(_PYD_ZENOH, _PYD_Q0))),
        'readme-behind': _approx(_x(r.ratio(_PYD_Q0, _ZEARED_DEFAULT))),
        'readme-qos1': _approx(_x(r.ratio(_ZEARED_DEFAULT, _PYD_Q1))),
        'readme-rusted-q0': _approx(_x(r.ratio(_RUSTED_DEFAULT, _PYD_Q0))),
        'readme-rusted-q1': _approx(_x(r.ratio(_RUSTED_DEFAULT, _PYD_Q1))),
        # Inline blocks, the wire suite.
        'wire-smaller': _pct(_less(r.ratio(_WIRE_MSGPACK, _WIRE_JSON, 'wire_bytes'))),
        'wire-msgpack-bytes': _fmt(r.median(_WIRE_MSGPACK, 'wire_bytes'), lambda x: f'{x:.0f}'),
        'wire-json-bytes': _fmt(r.median(_WIRE_JSON, 'wire_bytes'), lambda x: f'{x:.0f}'),
        # Inline blocks, the async headline bullets (end to end, against the fastest row).
        'async-marshmallow': _approx(_pct(_less(r.ratio(('async', 'zenoh + marshmallow (json)'), fastest)))),
        'async-larger': _approx(_pct(_more(r.ratio(('async', 'zenoh + marshmallow (json)'), fastest, 'wire_bytes')))),
        'async-json': _approx(_pct(_less(r.ratio(('async', 'sync send + sync on_message (json)'), fastest)))),
        'async-asend': _approx(_pct(_less(r.ratio(('async', 'asend + sync on_message (msgpack)'), fastest)))),
        'async-alisten': _approx(_pct(_less(r.ratio(('async', 'sync send + alisten (msgpack)'), fastest)))),
        'async-asyncdef': _approx(
            _pct(_less(r.ratio(('async', 'sync send + async-def on_message (msgpack)'), fastest)))
        ),
        'async-gap': _NA if gap is None else f'{math.ceil(gap * 100)}%',
        # Inline blocks, the accelerated suite.
        'rusted-json': _x(r.ratio(('rusted', 'zenoh + zeared + rusted (json, cached)'), _WIRE_JSON), 2, _TIMES),
        'rusted-msgpack': _x(
            r.ratio(('rusted', 'zenoh + zeared + rusted (msgpack, cached)'), _WIRE_MSGPACK), 2, _TIMES
        ),
        # Inline blocks, the answer's prose.
        'answer-behind': _x(r.ratio(_PYD_Q0, _ZEARED_DEFAULT)),
        'mqtt-qos1-cost': _x(r.ratio(_PYD_Q0, _PYD_Q1), 0),
        'mqtt-enqueue-q0': _approx(_x(_enqueue_overstatement(r, _PYD_Q0), 0)),
        'mqtt-enqueue-q1': _approx(_x(_enqueue_overstatement(r, _PYD_Q1), 0)),
    }


# --- rendering --------------------------------------------------------------


def render(text: str, generated: dict[str, str]) -> str:
    """``text`` with every marker pair's body replaced by its generated block.

    Raises:
        KeyError: a marker names a block this module doesn't generate.
    """

    def replace(match: re.Match[str]) -> str:
        return f'{match[1]}{generated[match["name"]]}{match[4]}'

    return _BLOCK.sub(replace, text)


def update(report: Report, *, check: bool = False, docs: Sequence[Path] = DOCS) -> list[Path]:
    """Re-render ``docs`` from ``report``; return the files that differed.

    With ``check=True`` nothing is written: the return value is the drift.
    """
    generated = blocks(report)
    drifted = []
    for path in docs:
        text = path.read_text(encoding='utf-8')
        new = render(text, generated)
        if new != text:
            drifted.append(path)
            if not check:
                path.write_text(new, encoding='utf-8')
    return drifted


def load(path: Path = RESULTS) -> Report:
    """The report at ``path``, the committed artifact by default."""
    return Report.loads(path.read_text(encoding='utf-8'))


def main() -> None:
    """Re-render the docs' numbers from the committed results, or check them."""
    parser = argparse.ArgumentParser(prog='python -m bench.tables', description=__doc__.splitlines()[0])
    parser.add_argument('--check', action='store_true', help='exit 1 if a doc has drifted from results.json')
    args = parser.parse_args()

    drifted = update(load(), check=args.check)
    for path in drifted:
        print(f'{"drifted" if args.check else "updated"}: {path.relative_to(_ROOT)}')
    if args.check and drifted:
        sys.exit(1)


if __name__ == '__main__':
    main()
