"""Cross-session presence observer — declares ``__zeared/alive/**`` and
``__zeared/will/**`` subscribers; on a peer's liveliness DELETE, fans
synthesised will-samples out to every registered dispatcher.

Holds ``_PresenceObserver``, the observer-registry helpers, and the
``Dispatcher`` callable type. Materialised once when the first
``LIVELINESS = True`` subscriber on a session registers.
"""
from __future__ import annotations

import logging
import threading
from typing import Callable, Dict, List, Optional

import zenoh

from .. import _codec as codec
from .._managed_session import resolve_raw
from ._presence_synthesized_sample import _SynthesizedSample
from .presence import (
    ALIVE_PREFIX,
    WILL_PREFIX,
    _WillEnvelope,
    _resolve_gc_interval,
)


_log = logging.getLogger('zeared.presence')


# Interested-party callback: invoked with the synthesised sample when a
# matching will fires. The subscriber registers one of these per declared
# class. Returns True iff the sample matched any of the subscriber's templates
# (used only for debug counting, not control flow).
Dispatcher = Callable[[_SynthesizedSample], bool]


class _PresenceObserver:
    """Per-session observer of ``__zeared/alive/**`` + ``__zeared/will/**``.

    Materialised once when the first ``LIVELINESS = True`` subscriber on that
    session registers. Holds:
      - a liveliness subscriber (history=True so we see currently-alive tokens)
      - a will subscriber (retained via our queryable machinery) + initial get
      - a map of peer_zid → wills-stashed-for-that-peer
      - a list of interested parties (dispatchers)

    On liveliness DELETE: iterate stashed wills for that peer; call each
    dispatcher with a synthesised sample. Dispatchers filter by template
    internally.
    """

    __slots__ = (
        'session', '_self_zid',
        '_alive_sub', '_will_sub',
        '_wills_by_zid',        # peer_zid → {slug → _WillEnvelope}
        '_alive_zids',          # set of currently-alive peer zids
        '_parties',             # list[Dispatcher]
        '_lock',
        '_gc_thread', '_gc_cancel', '_gc_interval',
    )

    def __init__(self, session: zenoh.Session):
        self.session = session
        self._self_zid = str(session.zid())
        self._alive_sub: Optional[zenoh.Subscriber] = None
        self._will_sub: Optional[zenoh.Subscriber] = None
        self._wills_by_zid: Dict[str, Dict[str, _WillEnvelope]] = {}
        self._alive_zids: set = set()
        self._parties: List[Dispatcher] = []
        self._lock = threading.Lock()
        self._gc_thread: Optional[threading.Thread] = None
        self._gc_cancel = threading.Event()
        # Observer-level GC-interval override; ``None`` means "defer to
        # session-level (or module default)". Tests / niche runtime
        # tuning can set this directly: ``observer._gc_interval = 0.05``.
        # The factory-level ``z.peer(gc_interval=...)`` flows through
        # via the wrapper's ``session._gc_interval`` attribute and is
        # read on every loop iteration, so post-construction wrapper
        # changes also propagate.
        self._gc_interval: Optional[float] = None

    def start(self) -> None:
        """Declare the two observing subscribers. Idempotent.

        The initial fetch of already-retained wills happens in a background
        thread so ``Subscriber._declare`` stays snappy and doesn't block on
        Zenoh's query roundtrip.
        """
        if self._alive_sub is not None:
            return
        with self._lock:
            if self._alive_sub is not None:
                return
            raw = resolve_raw(self.session)
            # history=True gives us already-declared tokens as initial PUTs.
            self._alive_sub = raw.liveliness().declare_subscriber(
                f'{ALIVE_PREFIX}/**',
                self._on_alive,
                history=True,
            )
            # Regular subscriber on the will stream.
            self._will_sub = raw.declare_subscriber(
                f'{WILL_PREFIX}/**',
                self._on_will,
            )

        # Background initial fetch — best effort, short timeout. Late-joiners
        # get this history via peer queryables.
        def _fetch():
            try:
                for reply in self.session.get(
                    f'{WILL_PREFIX}/**', timeout=1.0,
                ):
                    ok = getattr(reply, 'ok', None)
                    if ok is not None:
                        try:
                            self._on_will(ok)
                        except Exception:  # noqa: BLE001
                            _log.exception('presence: initial will dispatch raised')
            except Exception as exc:  # noqa: BLE001
                _log.warning('presence: initial will-fetch failed: %s', exc)

        threading.Thread(target=_fetch, daemon=True).start()

        # Start the GC daemon — sweeps stash entries whose peer is no
        # longer alive (covers missed-DELETE during partition, etc.).
        self._gc_cancel.clear()
        self._gc_thread = threading.Thread(
            target=self._gc_loop, daemon=True, name='zeared-presence-gc',
        )
        self._gc_thread.start()

    def _gc_loop(self) -> None:
        """Periodically drop stashed wills for peers no longer alive.

        Re-reads ``self._gc_interval`` (and the wrapper-side
        ``session._gc_interval`` if present) on every iteration, so
        runtime mutations propagate without restarting the loop.
        """
        while True:
            interval = _resolve_gc_interval(self.session, self._gc_interval)
            if self._gc_cancel.wait(interval):
                return
            with self._lock:
                stale = [
                    zid for zid in self._wills_by_zid
                    if zid not in self._alive_zids
                ]
                for zid in stale:
                    self._wills_by_zid.pop(zid, None)

    def stop(self) -> None:
        """Undeclare the two subscribers. Idempotent."""
        # Cancel GC first so it does not race against state teardown.
        self._gc_cancel.set()
        gc_thread = self._gc_thread
        self._gc_thread = None
        with self._lock:
            alive, will = self._alive_sub, self._will_sub
            self._alive_sub = None
            self._will_sub = None
            self._parties.clear()
            self._wills_by_zid.clear()
            self._alive_zids.clear()
        if alive is not None:
            try:
                alive.undeclare()
            except Exception:  # noqa: BLE001
                pass
        if will is not None:
            try:
                will.undeclare()
            except Exception:  # noqa: BLE001
                pass
        if gc_thread is not None and gc_thread.is_alive():
            gc_thread.join(timeout=1.0)
        _observer_registry.pop(id(self.session), None)

    def register(self, dispatcher: Dispatcher) -> None:
        with self._lock:
            self._parties.append(dispatcher)

    def unregister(self, dispatcher: Dispatcher) -> None:
        with self._lock:
            try:
                self._parties.remove(dispatcher)
            except ValueError:
                pass

    # -- callbacks from Zenoh ---------------------------------------------

    def _on_alive(self, sample: zenoh.Sample) -> None:
        key = str(sample.key_expr)
        if not key.startswith(f'{ALIVE_PREFIX}/'):
            return
        peer_zid = key[len(ALIVE_PREFIX) + 1:]
        if peer_zid == self._self_zid:
            return   # ignore our own token events
        if sample.kind == zenoh.SampleKind.PUT:
            with self._lock:
                self._alive_zids.add(peer_zid)
        else:  # DELETE
            with self._lock:
                self._alive_zids.discard(peer_zid)
                wills = self._wills_by_zid.pop(peer_zid, {})
                parties = list(self._parties)
            # Fan out synthesised samples for each will.
            for envelope in wills.values():
                mime = codec.MIME.get(envelope.encoding, codec.MIME['msgpack'])
                syn = _SynthesizedSample(
                    key_expr=envelope.target_key_expr,
                    payload=envelope.payload,
                    encoding_mime=mime,
                    source_zid=envelope.source_zid,
                )
                for party in parties:
                    try:
                        party(syn)
                    except Exception:  # noqa: BLE001
                        _log.exception('presence: dispatcher raised')

    def _on_will(self, sample) -> None:
        """Handle a sample on ``__zeared/will/<zid>/<slug>``.

        Bucket is now ``{peer_zid: {slug: _WillEnvelope}}`` so an explicit
        DELETE for a specific will key drops only that entry. A future
        ``unregister_will()`` API can rely on this; until then DELETEs
        only happen if a peer's will publish was overwritten with an
        explicit clear, which is correct.
        """
        key = str(sample.key_expr)
        parts = key.split('/')
        # Expected shape: ['__zeared', 'will', '<zid>', '<slug>']
        if len(parts) < 4:
            return
        peer_zid = parts[2]
        slug = parts[3]
        if peer_zid == self._self_zid:
            return

        if getattr(sample, 'kind', None) == zenoh.SampleKind.DELETE:
            with self._lock:
                bucket = self._wills_by_zid.get(peer_zid)
                if bucket is not None:
                    bucket.pop(slug, None)
                    if not bucket:
                        self._wills_by_zid.pop(peer_zid, None)
            return

        # PUT: stash by slug.
        raw = bytes(sample.payload)
        sample_enc = str(getattr(sample, 'encoding', '') or '')
        env_enc = 'json' if 'json' in sample_enc else 'msgpack'
        try:
            env_dict = codec.unpack(raw, env_enc)
            envelope = _WillEnvelope.load(env_dict)
        except Exception as exc:  # noqa: BLE001
            _log.warning('presence: could not decode will envelope at %s: %s', key, exc)
            return
        with self._lock:
            bucket = self._wills_by_zid.setdefault(peer_zid, {})
            bucket[slug] = envelope


# Observer registry — keyed on id(session), one observer per session.
_observer_registry: Dict[int, _PresenceObserver] = {}
_observer_lock = threading.Lock()


def get_observer(session: zenoh.Session) -> _PresenceObserver:
    sid = id(session)
    obs = _observer_registry.get(sid)
    if obs is not None:
        return obs
    with _observer_lock:
        obs = _observer_registry.get(sid)
        if obs is not None:
            return obs
        obs = _PresenceObserver(session)
        _observer_registry[sid] = obs
        return obs


def clear_observer(session: Optional[zenoh.Session] = None) -> None:
    """Stop observers. Without ``session=``, stops all."""
    with _observer_lock:
        if session is None:
            observers = list(_observer_registry.values())
            _observer_registry.clear()
        else:
            sid = id(session)
            obs = _observer_registry.pop(sid, None)
            observers = [obs] if obs is not None else []
    for obs in observers:
        obs.stop()
