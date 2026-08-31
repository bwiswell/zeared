"""Module-level queryable registry — keyed on ``id(session)``. Hard refs
(no weakref) since ``Queryable`` uses ``__slots__`` and we explicitly
deregister on close. ``z.release(session=sess)`` walks this set; the
reconnect machinery walks it to redeclare against a fresh raw.

Sibling helper inside the ``queryable`` Pattern B subdir — mirrors
``subscriber/_subscriber_registry.py``.
"""
from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .._managed_session import SessionLike
    import zenoh

    from .queryable import Queryable


_log = logging.getLogger('zeared.queryable')


_queryables: 'dict[int, set]' = {}
_queryables_lock = threading.Lock()


def _register_queryable(session: 'zenoh.Session', qbl: 'Queryable') -> None:
    sid = id(session)
    with _queryables_lock:
        _queryables.setdefault(sid, set()).add(qbl)


def _deregister_queryable(session, qbl: 'Queryable') -> None:
    if session is None:
        return
    sid = id(session)
    with _queryables_lock:
        bucket = _queryables.get(sid)
        if bucket is None:
            return
        bucket.discard(qbl)
        if not bucket:
            _queryables.pop(sid, None)


def _close_queryables_for(session: 'SessionLike') -> None:
    """Close every queryable registered against this session. Called by
    ``z.release()`` right after subscribers are closed."""
    sid = id(session)
    with _queryables_lock:
        bucket = _queryables.pop(sid, None)
    if bucket is None:
        return
    for qbl in list(bucket):
        try:
            qbl.close()
        except Exception:  # noqa: BLE001
            _log.warning(
                'queryable.close failed during release', exc_info=True,
            )


def clear_queryable_cache(*, session: 'Optional[zenoh.Session]' = None) -> None:
    """Undeclare user queryables and drop them from the registry.

    Without ``session=``, closes every registered queryable. With
    ``session=``, closes only those declared against that session — useful
    just before closing a session in a long-running process, and for test
    isolation. Mirrors :func:`zeared.clear_retention_cache`.
    """
    if session is not None:
        _close_queryables_for(session)
        return
    with _queryables_lock:
        buckets = list(_queryables.values())
        _queryables.clear()
    for bucket in buckets:
        for qbl in list(bucket):
            try:
                qbl.close()
            except Exception:  # noqa: BLE001
                _log.warning(
                    'queryable.close failed during clear', exc_info=True,
                )
