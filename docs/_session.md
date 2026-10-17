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
    def __call__(session) -> _SessionScope  # context manager for a context-local override
```

Scope overrides are pushed onto a `ContextVar`-backed stack (0.3.8; it was
a `threading.local()` before), the same mechanism `z.batch()` uses. That
makes a scope context-local:

- **Per thread.** A new `threading.Thread` starts with an empty context, so
  two threads still scope their own sessions independently.
- **Into `asyncio.to_thread` workers.** `to_thread` copies the context, so
  `asend` / `aquery` / `afetch_retained` / `aunretain` / `adeclare_presence` —
  which resolve on a worker thread — see the enclosing scope. Before
  0.3.8 they didn't, and fell back to the default.
- **Per asyncio task.** Each task gets its own copy of the context at
  creation: tasks interleaving on one loop don't see each other's scopes,
  and a task created inside a `with z.session(...)` block keeps that scope
  after the block exits.

On a free-threaded build, `sys.flags.thread_inherit_context` defaults to
true, so a new thread *inherits* the scope instead of starting empty.

## `_SessionScope`

Return value of `zeared.session(other)`. `__enter__` pushes (`ContextVar.set`);
`__exit__` restores the previous stack (`reset` of that entry's token).
Works correctly even when an exception escapes the block, and when one
scope object is re-entered. Exiting in a different context from the one it
was entered in (for example a scope held across a `yield` in an async
generator that another task resumes) raises `ValueError`; the scope can
still be exited from the right context afterwards.

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
