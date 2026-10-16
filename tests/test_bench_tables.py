"""Tests for ``bench/tables.py``: the generated benchmark numbers (0.3.11).

``bench/`` is repo-only, not part of the package. The ``prek`` hook proves the
docs match the artifact; it can't prove the medians and ratios are right, which
seared's single-run tables never had to compute. These pin that on synthetic
reports.
"""

from __future__ import annotations

import json

import pytest

from bench.harness import Measurement, Report
from bench.tables import Results, _table, blocks, render, run_line, update

_VERSION = 'zeared 0.3.11/seared 0.3.3'


def _m(suite: str, strategy: str, metric: str, value: float, run: int = 0, version: str = _VERSION) -> Measurement:
    unit = {'wire_bytes': 'bytes', 'mb_per_s': 'MB/s', 'drops': 'msgs'}.get(metric, 'msgs/s')
    return Measurement(suite=suite, strategy=strategy, version=version, metric=metric, value=value, unit=unit, run=run)


def _report(measurements: list[Measurement], runs: int = 1) -> Report:
    return Report(
        timestamp='2026-10-16T12:00:00+00:00',
        python='3.14.3',
        platform='Linux',
        zenoh='1.10.1',
        iterations=5000,
        duration_s=5.0,
        runs=runs,
        measurements=measurements,
    )


def _row(suite: str, strategy: str, e2e: list[float], *, pub: float = 1000.0, wire: float = 533.0) -> list[Measurement]:
    """One strategy's measurements across ``len(e2e)`` runs."""
    out = []
    for run, rate in enumerate(e2e):
        out += [
            _m(suite, strategy, 'pub_rate', pub, run),
            _m(suite, strategy, 'e2e_rate', rate, run),
            _m(suite, strategy, 'mb_per_s', pub * wire / 1e6, run),
            _m(suite, strategy, 'wire_bytes', wire, run),
            _m(suite, strategy, 'drops', 0.0, run),
        ]
    return out


class TestAggregation:
    def test_median_and_spread_over_runs(self):
        r = Results(_report(_row('wire', 'w', [130.0, 100.0, 110.0]), runs=3))
        assert r.median(('wire', 'w')) == 110.0
        assert r.spread(('wire', 'w')) == pytest.approx(0.3)

    def test_ratio_is_a_ratio_of_medians(self):
        # Medians 100 and 50 give 2.0; the median of the per-run ratios
        # (10/50, 100/40, 200/60) would be 2.5 instead.
        rows = _row('stacks', 'a', [10.0, 100.0, 200.0]) + _row('stacks', 'b', [50.0, 40.0, 60.0])
        r = Results(_report(rows, runs=3))
        assert r.ratio(('stacks', 'a'), ('stacks', 'b')) == pytest.approx(2.0)

    def test_unmeasured_rows_give_none(self):
        r = Results(_report(_row('wire', 'w', [1.0])))
        assert r.median(('wire', 'missing')) is None
        assert r.spread(('wire', 'missing')) is None
        assert r.ratio(('wire', 'w'), ('wire', 'missing')) is None


class TestBlocks:
    def test_missing_strategy_drops_its_row_and_its_numbers_read_na(self):
        report = _report(_row('wire', 'zenoh + marshmallow (json)', [4000.0], wire=796.0))
        out = blocks(report)
        assert out['wire'].count('\n| ') == 2  # header + the one measured row
        assert '`marshmallow`' in out['wire']
        assert out['readme-faster'] == 'n/a'  # no zeared row to compare against
        assert out['rusted-json'] == 'n/a'

    def test_run_line_names_versions_and_runs(self):
        rows = _row('wire', 'w', [1.0, 2.0, 3.0])
        rows += [_m('rusted', 'r', 'e2e_rate', 1.0, version='zeared 0.3.11/seared 0.3.3/rusted 0.3.1')]
        line = run_line(_report(rows, runs=3))
        assert line == (
            'zeared 0.3.11, seared 0.3.3, `rusted` 0.3.1, `eclipse-zenoh` 1.10.1, CPython 3.14.3; '
            'the median of 3 runs, recorded 2026-10-16'
        )

    def test_one_run_says_so(self):
        out = blocks(_report(_row('wire', 'w', [1.0])))
        assert out['run'].endswith('; one run, recorded 2026-10-16')
        assert out['readme-runs'] == '(one run, here and below)'


class TestTableStyles:
    def test_unpadded_rule_is_header_width_plus_two(self):
        text = _table(['Strategy', 'pub/s'], [['a long label', '1']], right={1})
        assert text.splitlines() == ['| Strategy | pub/s |', '|----------|------:|', '| a long label | 1 |']

    def test_padded_cells_and_rule_follow_the_widest_cell(self):
        text = _table(['Strategy', 'pub/s'], [['a long label', '1']], right={1}, pad=True)
        assert text.splitlines() == [
            '| Strategy     | pub/s |',
            '|--------------|------:|',
            '| a long label | 1     |',
        ]


class TestRendering:
    def test_only_marker_bodies_change(self):
        text = (
            'Prose **stays** <!-- bench:x -->old<!-- /bench:x --> as written.\n'
            '<!-- bench:t -->\nstale\n<!-- /bench:t -->\n'
        )
        new = render(text, {'x': 'NEW', 't': '\n| fresh |\n'})
        assert new == (
            'Prose **stays** <!-- bench:x -->NEW<!-- /bench:x --> as written.\n'
            '<!-- bench:t -->\n| fresh |\n<!-- /bench:t -->\n'
        )

    def test_an_unknown_marker_raises(self):
        with pytest.raises(KeyError):
            render('<!-- bench:nope -->x<!-- /bench:nope -->', {})

    def test_update_check_reports_drift_and_writes_nothing(self, tmp_path):
        doc = tmp_path / 'doc.md'
        doc.write_text('Runs: <!-- bench:runs -->9<!-- /bench:runs -->.\n', encoding='utf-8')
        report = _report(_row('wire', 'w', [1.0, 2.0]), runs=2)

        assert update(report, check=True, docs=[doc]) == [doc]
        assert '-->9<!--' in doc.read_text(encoding='utf-8')

        assert update(report, docs=[doc]) == [doc]
        assert doc.read_text(encoding='utf-8') == 'Runs: <!-- bench:runs -->2<!-- /bench:runs -->.\n'
        assert update(report, check=True, docs=[doc]) == []


class TestArtifactCompatibility:
    def test_a_pre_0_3_11_artifact_loads_as_one_run(self):
        """An artifact recorded before runs were tagged has no ``run`` / ``runs`` keys."""
        raw = json.loads(Report.dumps(_report(_row('wire', 'w', [1.0]))))
        del raw['runs']
        for m in raw['measurements']:
            del m['run']
        report = Report.loads(json.dumps(raw))
        assert report.runs == 1
        assert {m.run for m in report.measurements} == {0}
        assert blocks(report)['runs'] == '1'
