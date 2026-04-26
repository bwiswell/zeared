"""Tests for ``zeared/_module_class.py`` — the ``_ZearedModule``
metaclass that intercepts ``zeared.session = sess`` assignments so the
dual-role ``_SessionHandle`` keeps its identity while updating its
default."""
from __future__ import annotations

import sys

import zeared as z
from zeared._module_class import _ZearedModule
from zeared._session import _SessionHandle


class TestModuleClassSwap:
    """Pin: ``zeared.session = sess`` routes through ``session._set_default``,
    NOT through normal attribute replacement."""

    def test_module_is_subclass_of_zeared_module(self):
        # The runtime module class swap is in effect.
        assert isinstance(sys.modules['zeared'], _ZearedModule)

    def test_session_handle_identity_preserved_on_assignment(self):
        # Capture the handle before the assignment.
        handle_before = z.session
        # Open a real peer (close immediately — we just need a session
        # object to assign).
        sess = z.peer()
        try:
            z.session = sess
            handle_after = z.session
            # The handle object is the SAME — assignment updated the
            # default, not the handle itself.
            assert handle_after is handle_before
            assert isinstance(handle_after, _SessionHandle)
            # The default is now the assigned session.
            assert handle_after.current is sess
        finally:
            sess.close()
            # Reset the default to avoid bleeding into other tests.
            handle_before._default = None

    def test_other_attribute_assignments_pass_through(self):
        # Assigning a non-``session`` attribute should hit normal
        # __setattr__ behaviour.
        z.test_marker_xyz = 'hello'
        try:
            assert z.test_marker_xyz == 'hello'
        finally:
            del sys.modules['zeared'].test_marker_xyz
