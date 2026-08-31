"""Public surface of the ``queryable`` subpackage.

Re-exports the ``Queryable`` handle and ``QueryContext`` plus the
registry helpers that ``z.release`` / ``release_all`` / the reconnect
machinery walk.
"""

from __future__ import annotations

from ._query_context import QueryContext
from ._queryable_registry import (
    _close_queryables_for,
    _deregister_queryable,
    _queryables,
    _queryables_lock,
    _register_queryable,
    clear_queryable_cache,
)
from .queryable import Queryable

__all__ = [
    'QueryContext',
    'Queryable',
    '_close_queryables_for',
    '_deregister_queryable',
    '_queryables',
    '_queryables_lock',
    '_register_queryable',
    'clear_queryable_cache',
]
