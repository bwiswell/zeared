"""Trie-based index of path-segmented topics.

Used by the retention queryable handler to avoid the O(N)
iterate-and-intersect loop on every incoming query.

Cached topics are concrete (no wildcards). A query may carry any of Zenoh's
wildcards: ``*`` (one chunk), ``**`` (any number of chunks, none included)
and ``$*`` within a chunk. The trie's walk only narrows: it may collect a
topic the query doesn't reach, never miss one that it does, and each
candidate is then confirmed with ``zeared.keyexpr.intersects`` (0.4.2). So
the answer is exactly the topics Zenoh would say the query intersects.
Until 0.4.2 the walk was the answer: a ``$*`` chunk matched nothing, and a
trailing ``**`` missed the key at its own prefix.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .keyexpr import intersects

if TYPE_CHECKING:
    from collections.abc import Iterator

# Sentinel for the leaf-set inside each trie node — using a string with
# leading/trailing underscores avoids any collision with real path segments.
_LEAF_KEY = '__topics__'


class _PrefixIndex:
    """Trie of path-segments. Insert concrete topics; query with wildcards.

    Implementation note: each node is a plain ``dict``. A child entry maps
    segment → child-node, except the special key ``__topics__`` which maps
    to a ``set[str]`` of concrete topics whose path ends at this node.
    """

    __slots__ = ('_root', '_size')

    def __init__(self) -> None:
        self._root: dict = {}
        self._size = 0

    def __len__(self) -> int:
        return self._size

    def add(self, topic: str) -> None:
        """Insert a concrete topic."""
        segs = topic.split('/')
        node = self._root
        for seg in segs:
            node = node.setdefault(seg, {})
        leaves = node.setdefault(_LEAF_KEY, set())
        if topic not in leaves:
            leaves.add(topic)
            self._size += 1

    def remove(self, topic: str) -> None:
        """Remove a concrete topic. Idempotent (silently ignores misses)."""
        segs = topic.split('/')
        path: list[dict] = [self._root]
        node = self._root
        for seg in segs:
            child = node.get(seg)
            if child is None:
                return  # not present
            path.append(child)
            node = child
        leaves = node.get(_LEAF_KEY)
        if leaves is None or topic not in leaves:
            return
        leaves.discard(topic)
        self._size -= 1
        if not leaves:
            node.pop(_LEAF_KEY, None)
        # Prune empty interior branches walking back up.
        for i in range(len(segs) - 1, -1, -1):
            child = path[i + 1]
            if child:  # still has descendants or leaves
                break
            path[i].pop(segs[i], None)

    def matching(self, query_key: str) -> Iterator[str]:
        """Yield each cached topic that ``query_key`` intersects, as Zenoh decides it, once.

        A concrete query is an O(depth) walk; wildcards fan out over the
        branches they could reach, and every candidate is confirmed.
        """
        segs = query_key.split('/')
        seen: set[str] = set()
        for topic in self._walk(self._root, segs, 0):
            if topic not in seen:
                seen.add(topic)
                if intersects(query_key, topic):
                    yield topic

    def _walk(self, node: dict, segs: list[str], i: int) -> Iterator[str]:
        """Collect the topics under ``node`` that ``segs[i:]`` might reach: a superset, confirmed by ``matching``."""
        if i == len(segs):
            leaves = node.get(_LEAF_KEY)
            if leaves:
                yield from leaves
            return
        seg = segs[i]
        if seg == '**':
            # Zero chunks or more, here or anywhere below.
            yield from self._walk_starstar(node, segs[i + 1 :])
        elif seg == '*' or '$*' in seg:
            # One chunk: any child, which ``matching`` then confirms for ``$*``.
            for k, child in node.items():
                if k == _LEAF_KEY:
                    continue
                yield from self._walk(child, segs, i + 1)
        else:
            child = node.get(seg)
            if child is not None:
                yield from self._walk(child, segs, i + 1)

    def _walk_starstar(self, node: dict, remaining: list[str]) -> Iterator[str]:
        """Handle ``**``, trailing or not: zero chunks here, or one or more below, then ``remaining``."""
        # First option: ** matched zero segments here — try matching
        # `remaining` from this node directly.
        yield from self._walk(node, remaining, 0)
        # Otherwise: ** matched at least one segment here. Recurse into
        # each child and try `**`+remaining again.
        for k, child in node.items():
            if k == _LEAF_KEY:
                continue
            # Equivalent to: try `**`+remaining at the child level.
            yield from self._walk_starstar(child, remaining)
