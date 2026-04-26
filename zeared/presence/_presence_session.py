"""Per-session presence state — one liveliness token + N registered wills.

Holds ``_SessionPresence``, the module-level registry, ``get_presence``,
and ``clear_presence_state``. Lazy-declares the liveliness token + will
queryable on the first ``register_will()`` call so publishers that never
register anything pay zero presence overhead.
"""
from __future__ import annotations

import logging
import threading
from typing import Dict, Optional

import zenoh

from .. import _codec as codec
from .._managed_session import resolve_raw
from .._prefix_index import _PrefixIndex
from .presence import (
    ALIVE_PREFIX,
    WILL_PREFIX,
    _WillEnvelope,
    _envelope_encoding,
    _slug,
)


_log = logging.getLogger('zeared.presence')


class _SessionPresence:
    """Per-session presence state: one liveliness token + N registered wills.

    The liveliness token and the wills-queryable are declared lazily on the
    first ``register_will()`` call. Publishers that never register a will
    pay zero presence overhead.
    """

    __slots__ = (
        'session', 'zid', '_token', '_queryable',
        '_wills', '_index', '_lock',
        '_registered',
    )

    def __init__(self, session: zenoh.Session):
        self.session = session
        self.zid = str(session.zid())
        self._token: Optional[zenoh.LivelinessToken] = None
        self._queryable: Optional[zenoh.Queryable] = None
        # full will_key (`__zeared/will/<zid>/<slug>`) → envelope
        self._wills: Dict[str, _WillEnvelope] = {}
        # Trie of will_keys — replaces the iterate-and-intersect loop in
        # `_handle_will_query`.
        self._index = _PrefixIndex()
        self._lock = threading.Lock()
        # (cls_qualname, envelope) pairs — kept so the reconnect machinery
        # can replay every registered will under the post-reconnect zid.
        # Keyed by (cls_qualname, target_key_expr) so re-registers update
        # rather than duplicate.
        self._registered: Dict[tuple, _WillEnvelope] = {}

    def _ensure_declared(self) -> None:
        """Declare the liveliness token and will queryable if not already."""
        if self._token is not None:
            return
        with self._lock:
            if self._token is not None:
                return
            alive_key = f'{ALIVE_PREFIX}/{self.zid}'
            raw = resolve_raw(self.session)
            self._token = raw.liveliness().declare_token(alive_key)
            will_wildcard = f'{WILL_PREFIX}/{self.zid}/**'
            self._queryable = raw.declare_queryable(
                will_wildcard, self._handle_will_query,
            )

    def register_will(
        self,
        cls_qualname: str,
        envelope: _WillEnvelope,
    ) -> None:
        """Register an envelope as the will for its target topic.

        Publishes retained (via session.put) to ``__zeared/will/<zid>/<slug>``
        so live subscribers see it immediately, stashes it for the queryable
        to serve late subscribers, and ensures the liveliness token is
        declared.
        """
        self._ensure_declared()
        slug = _slug(cls_qualname, envelope.target_key_expr)
        will_key = f'{WILL_PREFIX}/{self.zid}/{slug}'
        env_enc = _envelope_encoding()
        raw = codec.pack(_WillEnvelope.dump(envelope), env_enc)
        with self._lock:
            new_entry = will_key not in self._wills
            self._wills[will_key] = envelope
            if new_entry:
                self._index.add(will_key)
            self._registered[(cls_qualname, envelope.target_key_expr)] = envelope
        try:
            self.session.put(
                will_key, raw, encoding=codec.MIME[env_enc],
            )
        except Exception as exc:  # noqa: BLE001
            # If the publish fails, pull the will back out; no stale state.
            with self._lock:
                if self._wills.pop(will_key, None) is not None:
                    self._index.remove(will_key)
            _log.warning(
                'register_will: publish of %s failed: %s', will_key, exc,
            )
            raise

    def _handle_will_query(self, query: zenoh.Query) -> None:
        """Answer a late subscriber's ``session.get(__zeared/will/<zid>/**)``
        with every stashed will whose concrete key intersects the query.

        Uses the trie index — O(query depth × matches) rather than O(N)
        across all stashed wills.
        """
        query_key = str(query.key_expr)
        with self._lock:
            matches = list(self._index.matching(query_key))
            items = [(k, self._wills[k]) for k in matches if k in self._wills]
        env_enc = _envelope_encoding()
        for will_key, envelope in items:
            try:
                raw = codec.pack(_WillEnvelope.dump(envelope), env_enc)
                query.reply(will_key, raw, encoding=codec.MIME[env_enc])
            except Exception as exc:  # noqa: BLE001
                _log.warning(
                    'will-queryable reply failed on %s: %s', will_key, exc,
                )

    def replay_to(self, new_session) -> None:
        """Re-register every previously-registered will against ``new_session``.

        Used by the reconnect machinery: the old raw session is dead, the
        wrapper has swapped in a new raw, and peers will see the OLD zid's
        liveliness token DELETE (firing our wills on their side). We mint
        a fresh ``_SessionPresence`` under the new zid and replay every
        envelope so we come back online with the same wills wired up.

        The caller must have removed the old ``_SessionPresence`` from the
        module registry before calling this.
        """
        with self._lock:
            pairs = list(self._registered.items())
            # Drop our local state — the underlying token + queryable are
            # tied to the dead raw session. Don't try to undeclare; just
            # forget.
            self._wills.clear()
            self._index = _PrefixIndex()
            self._registered.clear()
            self._token = None
            self._queryable = None

        new_state = get_presence(new_session)
        new_zid = str(new_session.zid())
        for (cls_qualname, target), envelope in pairs:
            new_envelope = _WillEnvelope(
                source_zid=new_zid,
                target_key_expr=envelope.target_key_expr,
                encoding=envelope.encoding,
                payload=envelope.payload,
            )
            try:
                new_state.register_will(cls_qualname, new_envelope)
            except Exception:  # noqa: BLE001
                _log.exception(
                    'replay_to: register_will failed for %s @ %s',
                    cls_qualname, target,
                )

    def drop(self) -> None:
        """Undeclare the token + queryable, clear state. Idempotent."""
        with self._lock:
            token = self._token
            queryable = self._queryable
            self._token = None
            self._queryable = None
            self._wills.clear()
            self._index = _PrefixIndex()
        if token is not None:
            try:
                token.undeclare()
            except Exception:  # noqa: BLE001
                pass
        if queryable is not None:
            try:
                queryable.undeclare()
            except Exception:  # noqa: BLE001
                pass
        _registry.pop(id(self.session), None)


# ---------------------------------------------------------------------------
# Module-level registry
# ---------------------------------------------------------------------------


_registry: Dict[int, _SessionPresence] = {}
_registry_lock = threading.Lock()


def get_presence(session: zenoh.Session) -> _SessionPresence:
    """Return (creating if needed) the presence state for this session."""
    sid = id(session)
    state = _registry.get(sid)
    if state is not None:
        return state
    with _registry_lock:
        state = _registry.get(sid)
        if state is not None:
            return state
        state = _SessionPresence(session)
        _registry[sid] = state
        return state


def clear_presence_state(*, session: Optional[zenoh.Session] = None) -> None:
    """Drop per-session presence state. Without ``session=``, clears all."""
    with _registry_lock:
        if session is None:
            states = list(_registry.values())
            _registry.clear()
        else:
            sid = id(session)
            state = _registry.pop(sid, None)
            states = [state] if state is not None else []
    for s_state in states:
        # Undeclare directly; drop() re-pops from the registry but we already
        # did that above (no-op second time).
        try:
            if s_state._token is not None:
                s_state._token.undeclare()
        except Exception:  # noqa: BLE001
            pass
        try:
            if s_state._queryable is not None:
                s_state._queryable.undeclare()
        except Exception:  # noqa: BLE001
            pass
        s_state._token = None
        s_state._queryable = None
        s_state._wills.clear()
        s_state._index = _PrefixIndex()
