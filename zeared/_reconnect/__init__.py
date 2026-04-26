"""``_reconnect`` — reconnect orchestration for ``ManagedSession``.

Pattern B subdir. ``_reconnect.py`` holds the orchestration (probe loop,
reconnect worker, trigger, ``_reconnect`` driver). ``_restore.py`` holds
the post-reopen walks (retention, subscribers, wills) + the cancellable
``_open_with_backoff`` loop.

Public surface unchanged: callers continue to write
``from zeared._reconnect import start_probe`` (and ``_trigger_reconnect``,
``_reconnect`` for tests).
"""
from ._reconnect import (
    _probe_loop,
    _reconnect,
    _reconnect_worker,
    _trigger_reconnect,
    start_probe,
)
from ._restore import (
    _ReconnectAborted,
    _open_with_backoff,
    _restore_retention,
    _restore_subscribers,
    _restore_wills,
)


__all__ = [
    '_ReconnectAborted',
    '_open_with_backoff',
    '_probe_loop',
    '_reconnect',
    '_reconnect_worker',
    '_restore_retention',
    '_restore_subscribers',
    '_restore_wills',
    '_trigger_reconnect',
    'start_probe',
]
