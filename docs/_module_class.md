# `_module_class.py` — `_ZearedModule` metaclass

Subclass of `types.ModuleType` that intercepts `zeared.session = sess`
assignments so the dual-role `_SessionHandle` keeps its identity while
updating its default.

## Mechanism

`__init__.py` does:

```python
session: _SessionHandle = _SessionHandle()
sys.modules[__name__].__class__ = _ZearedModule
```

After the swap, `zeared.session = my_session` invokes
`_ZearedModule.__setattr__(self, 'session', my_session)`. The
`__setattr__` retrieves the existing handle (`self.__dict__['session']`)
and calls `handle._set_default(my_session)` — preserving the handle
object identity that other parts of the package hold references to.

Without the swap, `zeared.session = my_session` would replace the
`_SessionHandle` with the user's raw session, breaking the scope-stack
+ thread-local mechanism entirely.

## Why a separate file

Lives in `_module_class.py` rather than inline in `__init__.py` so the
package init stays under the 300-line cap. The actual swap (the
`sys.modules[__name__].__class__ = _ZearedModule` line) still lives in
`__init__.py` because that's where `__name__` is `zeared`.
