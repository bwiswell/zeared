"""Session-level presence (liveliness tokens) and Last-Will-and-Testament machinery.

Each session that uses presence declares a single Zenoh liveliness token at
``__zeared/alive/<zid>``. Users register wills per-message via
:meth:`Message.register_will`; each will is stored as a retained
``_WillEnvelope`` at ``__zeared/will/<zid>/<slug>`` so late subscribers can
fetch via queryable.

When a peer's liveliness token disappears (graceful close OR crash), any
subscriber on a ``LIVELINESS = True`` class whose declared templates match
the will's target key expression synthesises a sample locally and dispatches
it through the normal decode path — the user's callback fires identically
to a real publish.

This is an honest emulation: the will is never "really" published by a
broker (Zenoh has none). Non-subscribers don't observe the offline event.
Document accordingly.

Primary file of the ``presence`` Pattern B subdir. Holds the shared
constants, helpers, and the wire-shape ``_WillEnvelope`` class that the
session-state and observer helpers both consume. Per-session state lives
in ``_presence_session.py``; the cross-session observer lives in
``_presence_observer.py``; the synthesised-sample shim is in
``_presence_synthesized_sample.py``.
"""
from __future__ import annotations

import hashlib

import seared as s


ALIVE_PREFIX = '__zeared/alive'
WILL_PREFIX = '__zeared/will'

# Default interval for the orphaned-will GC sweep in `_PresenceObserver`.
# Long enough that overhead is irrelevant, short enough that orphaned
# stash entries don't accumulate for hours after a missed liveliness
# DELETE (e.g., during a brief network partition).
_GC_INTERVAL_SECONDS = 60.0


def _resolve_gc_interval(session, observer_override=None) -> float:
    """Resolve the per-iteration GC interval.

    Precedence: observer-instance override (``observer._gc_interval``,
    if non-None — set explicitly by tests / niche runtime tuning) >
    wrapper attribute (``session._gc_interval`` from
    ``z.peer(gc_interval=...)``) > module default
    (``_GC_INTERVAL_SECONDS``). Read on every loop iteration so a
    runtime poke at any layer takes effect on the next cycle.
    """
    if observer_override is not None:
        return observer_override
    return getattr(session, '_gc_interval', _GC_INTERVAL_SECONDS)


def _envelope_encoding() -> str:
    """Pick the wire encoding for the will envelope itself.

    Honors ``zeared.debug`` symmetrically across publish, queryable
    reply, and subscriber decode. The user payload *inside* the
    envelope continues to honor ``cls.ENCODING`` independently.
    """
    # Imported here to avoid a circular import at module load.
    import zeared as z
    return 'json' if z.debug else 'msgpack'


@s.seared
class _WillEnvelope(s.Seared):
    """Reserved wire shape for a registered LWT payload."""
    source_zid:      str   = s.Str(required=True)
    target_key_expr: str   = s.Str(required=True)
    encoding:        str   = s.Str(required=True)
    payload:         bytes = s.Bytes(required=True)


def _slug(cls_qualname: str, concrete_topic: str) -> str:
    """Deterministic short slug for a (cls, concrete_topic) pair."""
    return hashlib.sha1(
        f'{cls_qualname}:{concrete_topic}'.encode()
    ).hexdigest()[:16]
