"""``presence`` — session-level liveliness + Last-Will-and-Testament.

Pattern B subdir; ``presence.py`` is the primary file holding the shared
constants and the wire-shape ``_WillEnvelope``. Helpers split into:

- ``_presence_session.py`` — per-session state (`_SessionPresence`)
  and registry (`_registry`, `get_presence`, `clear_presence_state`).
- ``_presence_observer.py`` — cross-session observer (`_PresenceObserver`)
  and registry (`_observer_registry`, `get_observer`, `clear_observer`).
- ``_presence_synthesized_sample.py`` — `_SynthesizedSample` shim used
  to fire wills locally through the normal dispatch path.

Public surface (everything previously importable from
``zeared.presence``) is re-exported here; internal callers can keep
using ``from .presence import X`` unchanged.
"""
from ._presence_observer import (
    Dispatcher,
    _PresenceObserver,
    _observer_lock,
    _observer_registry,
    clear_observer,
    get_observer,
)
from ._presence_session import (
    _SessionPresence,
    _registry,
    _registry_lock,
    clear_presence_state,
    get_presence,
)
from ._presence_synthesized_sample import _SynthesizedSample
from .presence import (
    ALIVE_PREFIX,
    WILL_PREFIX,
    _GC_INTERVAL_SECONDS,
    _WillEnvelope,
    _envelope_encoding,
    _resolve_gc_interval,
    _slug,
)


__all__ = [
    'ALIVE_PREFIX',
    'Dispatcher',
    'WILL_PREFIX',
    '_GC_INTERVAL_SECONDS',
    '_PresenceObserver',
    '_SessionPresence',
    '_SynthesizedSample',
    '_WillEnvelope',
    '_envelope_encoding',
    '_observer_lock',
    '_observer_registry',
    '_registry',
    '_registry_lock',
    '_resolve_gc_interval',
    '_slug',
    'clear_observer',
    'clear_presence_state',
    'get_observer',
    'get_presence',
]
