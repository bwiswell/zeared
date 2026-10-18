"""Tests for ``zeared/__init__.py`` — the package root. Pins that the version
it stamps agrees with ``pyproject.toml``'s, and that every class it exports
has annotations ``typing.get_type_hints`` can resolve.
"""

from __future__ import annotations

import inspect
import tomllib
import typing
from pathlib import Path

import pytest

import zeared as z

_ROOT = Path(__file__).parent.parent


def test_the_version_is_stamped_the_same_in_both_places():
    project = tomllib.loads((_ROOT / 'pyproject.toml').read_text())
    # The release workflow tags from one and refuses to run if they differ;
    # consumers read the other. Failing here catches it before the merge.
    assert project['project']['version'] == z.__version__


_CLASSES = sorted(name for name in z.__all__ if inspect.isclass(getattr(z, name)))


@pytest.mark.parametrize('name', _CLASSES)
def test_every_exported_classs_annotations_resolve(name):
    # 0.4.3: on seared 0.4.1 and 0.4.2, `get_type_hints` raised `NameError` on
    # every class built on seared's base (Message, SessionConfig, ZenohMeta)
    # and every field class. An annotation naming a type imported only for
    # type checking, here or in seared, fails this.
    typing.get_type_hints(getattr(z, name))
