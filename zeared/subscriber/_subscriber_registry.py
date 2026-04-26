"""Module-level subscriber registry — keyed on ``id(session)``. Hard
refs (no weakref) since ``Subscriber`` uses ``__slots__`` and we
explicitly deregister on close. ``z.release(session=sess)`` walks this
set.

Sibling helper inside the ``subscriber`` Pattern B subdir. Pulled out
of ``subscriber.py`` to keep the class file under the 300-line cap.
"""
from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import zenoh

    from .subscriber import Subscriber


_log = logging.getLogger('zeared.subscriber')


# Soft cap for the per-subscriber schema-mismatch warn-once cache. New
# (sender_zid, observed_schema) pairs evict the oldest entry on overflow.
# 1024 covers realistic cluster sizes (peers × schema versions); a misfire
# from an evicted pair just means re-warning that pair, which is fine.
_SCHEMA_MISMATCH_CACHE_MAX = 1024


_subscribers: 'dict[int, set]' = {}
_subscribers_lock = threading.Lock()


def _register_subscriber(session: 'zenoh.Session', sub: 'Subscriber') -> None:
    sid = id(session)
    with _subscribers_lock:
        _subscribers.setdefault(sid, set()).add(sub)


def _deregister_subscriber(session, sub: 'Subscriber') -> None:
    if session is None:
        return
    sid = id(session)
    with _subscribers_lock:
        bucket = _subscribers.get(sid)
        if bucket is None:
            return
        bucket.discard(sub)
        if not bucket:
            _subscribers.pop(sid, None)


def _close_subscribers_for(session: 'zenoh.Session') -> None:
    """Close every subscriber registered against this session. Called by
    ``z.release()`` as the first step of shutdown."""
    sid = id(session)
    with _subscribers_lock:
        bucket = _subscribers.pop(sid, None)
    if bucket is None:
        return
    for sub in list(bucket):
        try:
            sub.close()
        except Exception:  # noqa: BLE001
            _log.warning(
                'subscriber.close failed during release', exc_info=True,
            )
