"""Tests for ``zeared/__init__.py`` — the package root. Pins that the version
it stamps agrees with ``pyproject.toml``'s.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import zeared as z

_ROOT = Path(__file__).parent.parent


def test_the_version_is_stamped_the_same_in_both_places():
    project = tomllib.loads((_ROOT / 'pyproject.toml').read_text())
    # The release workflow tags from one and refuses to run if they differ;
    # consumers read the other. Failing here catches it before the merge.
    assert project['project']['version'] == z.__version__
