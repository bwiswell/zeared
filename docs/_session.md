# `_session.py`

Internals of the dual-role `zeared.session` attribute. See
[`docs/overview/architecture.md`](overview/architecture.md) for the
user-level picture.

## `_SessionHandle`

The object assigned to `zeared.session` at module import time. Never replaced
for the lifetime of the program.

```python
class _SessionHandle:
    current: Optional[zenoh.Session]        # property; reads scope stack → default
    def resolve(explicit) -> zenoh.Session  # kwarg > scope > default > raise
    def __call__(session) -> _SessionScope  # context manager for thread-local override
```

Scope overrides are pushed onto a `threading.local()` stack, so two threads
can independently scope their own sessions.

## `_SessionScope`

Return value of `zeared.session(other)`. `__enter__` pushes; `__exit__` pops.
Works correctly even when an exception escapes the block.

## Why the module-class swap

`zeared.session = sess` would, by default, replace the `_SessionHandle`
instance — breaking `z.session(...)` and `z.session.current`. To preserve
assignment as an intuitive "set the default" gesture, `zeared/__init__.py`
swaps the module's `__class__` for `_ZearedModule`, whose `__setattr__`
intercepts the `session` name and redirects to `handle._set_default(value)`.
Every other attribute assignment is normal.

## Resolution order

Implemented in `_SessionHandle.resolve(explicit)`:

1. `explicit is not None` → return it.
2. Scope stack non-empty → return top of stack.
3. Default is set → return it.
4. Raise `NoSessionError`.
