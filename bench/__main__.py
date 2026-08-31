"""Bench runner. From the repo root: ``uv run python -m bench``.

Prints a table per suite and (unless ``--no-write``) records the run to
``bench/results.json`` — the committed artifact behind
``docs/overview/benchmarks.md``.

Suites that need a package a default dev sync doesn't install (the
``marshmallow`` comparator, the ``rusted`` accelerator) — or an external
binary it can't find (``mosquitto``, for the stack comparison) — are skipped
with a note rather than failing the run.
"""

from __future__ import annotations

import argparse
import importlib
from datetime import UTC, datetime
from importlib.metadata import version as dist_version
from pathlib import Path

from .harness import DEFAULT_DURATION_S, DEFAULT_ITERATIONS, N_ITEMS, N_TAGS, Report, environment, peer_session

#: (module, kind) — 'n' suites take an iteration count, 'duration' a window.
_SUITES = [
    ('suite_wire', 'n'),
    ('suite_rusted', 'n'),
    ('suite_throughput', 'duration'),
    ('suite_async', 'duration'),
    ('suite_stacks', 'n'),
]

_DEFAULT_OUT = Path(__file__).parent / 'results.json'
_TABLE_WIDTH = 104


def _print_suite(name: str, runs: list) -> None:
    """Print one suite's table."""
    if not runs:
        return
    print()
    print(f'== {name} ==')
    print('-' * _TABLE_WIDTH)
    print(f'{"strategy":44s}  {"pub/s":>9s}  {"e2e/s":>9s}  {"MB/s":>6s}  {"wire":>6s}  {"drops":>6s}')
    print('-' * _TABLE_WIDTH)
    for r in runs:
        print(
            f'{r.strategy:44s}  {r.pub_rate:>9,.0f}  {r.e2e_rate:>9,.0f}  '
            f'{r.mb_per_s:>6.2f}  {r.wire_bytes:>6d}  {r.drops:>6d}'
        )
    print('-' * _TABLE_WIDTH)


def main() -> None:
    """Parse args, run every discovered suite, print and write the results."""
    parser = argparse.ArgumentParser(prog='python -m bench', description=__doc__.splitlines()[0])
    parser.add_argument(
        '-n', '--iterations', type=int, default=DEFAULT_ITERATIONS, help='messages for the fixed-N suites'
    )
    parser.add_argument(
        '-d', '--duration', type=float, default=DEFAULT_DURATION_S, help='publish window for the duration suites'
    )
    parser.add_argument('--out', type=Path, default=_DEFAULT_OUT, help='results JSON path')
    parser.add_argument('--no-write', action='store_true', help='print the tables only; do not write the artifact')
    args = parser.parse_args()

    print(f'Schema: Outer(name, items[{N_ITEMS} x Inner], tags[{N_TAGS}])')
    print(f'Fixed-N suites: {args.iterations:,} messages | duration suites: {args.duration:.1f}s window')

    session = peer_session()
    measurements = []
    try:
        for name, kind in _SUITES:
            try:
                module = importlib.import_module(f'.{name}', package=__package__)
            except ImportError as exc:
                print(f'{name}: skipped ({exc.name} not installed)')
                continue
            runs = module.run(session, args.iterations if kind == 'n' else args.duration)
            _print_suite(name, runs)
            for r in runs:
                measurements.extend(r.to_measurements(module.SUITE))
    finally:
        session.close()

    if args.no_write:
        return

    python_version, platform_string = environment()
    report = Report(
        timestamp=datetime.now(UTC).isoformat(timespec='seconds'),
        python=python_version,
        platform=platform_string,
        zenoh=dist_version('eclipse-zenoh'),
        iterations=args.iterations,
        duration_s=args.duration,
        measurements=measurements,
    )
    args.out.write_text(Report.dumps(report) + '\n', encoding='utf-8')
    print(f'\nwrote {args.out} ({len(measurements)} measurements)')


if __name__ == '__main__':
    main()
