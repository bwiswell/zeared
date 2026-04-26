"""``_ZearedModule`` — module-class subclass that intercepts
``zeared.session = sess`` assignments so the dual-role
``_SessionHandle`` keeps its identity while updating its default.

Pulled out of ``__init__.py`` so the package init can stay a thin
re-export-and-glue module under the 300-line cap. The actual
``sys.modules[__name__].__class__ = _ZearedModule`` swap lives in
``__init__.py`` (where ``__name__`` is ``zeared``).
"""
from __future__ import annotations

import types


class _ZearedModule(types.ModuleType):
    """Subclass of the module type so ``zeared.session = sess`` can be intercepted."""

    def __setattr__(self, name: str, value) -> None:
        if name == 'session':
            # Late-bind the handle from the package namespace so we don't
            # carry a strong reference here. The handle is constructed in
            # ``__init__.py`` as ``session = _SessionHandle()``; on
            # assignment we route the value through ``_set_default``
            # rather than overwriting the handle object.
            handle = self.__dict__.get('session')
            if handle is not None:
                handle._set_default(value)
                return
        super().__setattr__(name, value)
