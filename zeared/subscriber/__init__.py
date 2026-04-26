"""``subscriber`` — Zeared subscription handle and per-session registry.

Pattern B subdir. ``subscriber.py`` holds the ``Subscriber`` class.
``_subscriber_registry.py`` holds the module-level subscriber registry
used by ``z.release(session=)``. ``_subscriber_dispatch.py`` holds the
per-sample dispatch closure builder plus the inspect / encoding /
async-adapter helpers. ``_subscriber_retained_fetch.py`` holds the
retained-fetch helper.

Public surface unchanged: callers continue to write
``from zeared.subscriber import Subscriber``.
"""
from ._subscriber_dispatch import (
    _adapt_async_callback,
    _build_dispatch,
    _make_presence_dispatcher,
    _pick_encoding,
    _wants_meta,
)
from ._subscriber_registry import (
    _SCHEMA_MISMATCH_CACHE_MAX,
    _close_subscribers_for,
    _deregister_subscriber,
    _register_subscriber,
    _subscribers,
    _subscribers_lock,
)
from ._subscriber_retained_fetch import _fetch_retained
from .subscriber import M, Subscriber


__all__ = [
    'M',
    '_SCHEMA_MISMATCH_CACHE_MAX',
    '_adapt_async_callback',
    '_build_dispatch',
    '_close_subscribers_for',
    '_deregister_subscriber',
    '_fetch_retained',
    '_make_presence_dispatcher',
    '_pick_encoding',
    '_register_subscriber',
    '_subscribers',
    '_subscribers_lock',
    '_wants_meta',
    'Subscriber',
]
