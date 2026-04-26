"""Public surface of the ``zeared`` package.

Imports + re-exports + the module-class swap that turns
``zeared.session = sess`` into ``session._set_default(sess)``.

Heavy lifting lives elsewhere:

- ``_factories.py`` — session-opening factories (``peer`` / ``client`` /
  ``open`` plus retry / config-build helpers).
- ``_release.py`` — session-tearing-down (``release`` / ``release_all``).
- ``_module_class.py`` — the ``_ZearedModule`` metaclass.

Re-exports the entire seared public surface so users can ``import
zeared as z`` and pick up every Field type, the decorator (renamed
``seared`` → ``zeared`` for single-package flavour), the ``Seared``
base class, and the error hierarchy. All seared names are still
reachable as ``seared.*`` for callers who prefer the two-package style.
"""
from __future__ import annotations

import sys

from seared import (
    Bool,
    Bytes,
    Date,
    DateTime,
    Decimal,
    Dict,
    Enum,
    Float,
    Int,
    NDArray,
    PandasFrame,
    Path,
    PolarsFrame,
    SearedError,
    Seared as Zeared,  # base class, renamed for uniform texture
    Str,
    T,
    Time,
    TimeDelta,
    Tuple,
    UUID,
    Union,
    ValidationError,
    seared as zeared,  # decorator, renamed for the same reason
)

from . import _codec as codec
from ._factories import client, open, peer  # noqa: A004 — `open` shadows builtin intentionally
from ._managed_session import ManagedSession, OnReconnectHandle
from ._mode import Mode
from ._module_class import _ZearedModule
from ._release import release, release_all
from ._session import _SessionHandle
from .async_ import abatch, aclient, alisten, aopen, apeer, asend, asend_batch, aunretain
from .batch import batch
from .config import SessionConfig
from .errors import (
    CallbackError, DecodeError, NoSessionError, RetainedFetchError,
    SchemaMismatchError, SessionDeadError, SubscriberError,
    SubscriptionError, TopicError, ZearedError,
)
from .message import Message
from .meta import ZenohMeta
from .presence import clear_observer, clear_presence_state
from .publisher import clear_publisher_cache, published_topics
from .retention import clear_retention_cache
from .subscriber import Subscriber


__version__ = '0.1.0'

# Module-level defaults.
# ``session`` is a dual-role handle (see ``_SessionHandle`` docstring) — the
# module-class swap below intercepts ``zeared.session = sess`` assignments.
# ``debug`` is an ordinary flag; ``True`` forces JSON on the wire everywhere.
session: _SessionHandle = _SessionHandle()
debug: bool = False


# Install the module-class swap so ``zeared.session = sess`` routes through
# ``session._set_default`` rather than overwriting the handle.
sys.modules[__name__].__class__ = _ZearedModule


__all__ = [
    # seared re-exports (decorator renamed to ``zeared`` for flavour)
    'Bool',
    'Bytes',
    'Date',
    'DateTime',
    'Decimal',
    'Dict',
    'Enum',
    'Float',
    'Int',
    'NDArray',
    'PandasFrame',
    'Path',
    'PolarsFrame',
    'SearedError',
    'Zeared',
    'Str',
    'T',
    'Time',
    'TimeDelta',
    'Tuple',
    'UUID',
    'Union',
    'ValidationError',
    'zeared',
    # zeared surface
    'CallbackError',
    'DecodeError',
    'ManagedSession',
    'Message',
    'Mode',
    'NoSessionError',
    'OnReconnectHandle',
    'RetainedFetchError',
    'SchemaMismatchError',
    'SessionDeadError',
    'Subscriber',
    'SubscriberError',
    'SubscriptionError',
    'TopicError',
    'ZearedError',
    'ZenohMeta',
    'abatch',
    'aclient',
    'alisten',
    'aopen',
    'apeer',
    'asend',
    'asend_batch',
    'aunretain',
    'SessionConfig',
    'batch',
    'clear_observer',
    'clear_presence_state',
    'clear_publisher_cache',
    'clear_retention_cache',
    'client',
    'codec',
    'debug',
    'open',
    'peer',
    'published_topics',
    'release',
    'release_all',
    'session',
]
