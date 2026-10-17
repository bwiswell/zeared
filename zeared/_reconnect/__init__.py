"""``_reconnect`` — reconnect orchestration for ``ManagedSession``.

Pattern B subdir. ``_reconnect.py`` holds the orchestration (probe loop,
reconnect worker, trigger, ``_reconnect`` driver). ``_restore.py`` holds
the post-reopen walks (publishers, retention, subscribers, queryables,
presence) + the cancellable ``_open_with_backoff`` loop. ``_link.py`` holds
router-link detection, the link signals, the republish and the re-attach
walk (0.4.0).

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
    _open_with_backoff,
    _ReconnectAbortedError,
    _restore_presence,
    _restore_publishers,
    _restore_queryables,
    _restore_retention,
    _restore_subscribers,
)

__all__ = [
    '_ReconnectAbortedError',
    '_open_with_backoff',
    '_probe_loop',
    '_reconnect',
    '_reconnect_worker',
    '_restore_presence',
    '_restore_publishers',
    '_restore_queryables',
    '_restore_retention',
    '_restore_subscribers',
    '_trigger_reconnect',
    'start_probe',
]
