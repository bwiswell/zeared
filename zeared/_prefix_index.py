"""Trie-based index of path-segmented topics.

Used by the retention and presence queryable handlers to avoid the
O(N) iterate-and-intersect loop on every incoming query.

Cached topics are concrete (no wildcards). Queries may carry single-segment
``*`` and trailing multi-segment ``**`` wildcards. The trie's ``matching``
walk handles both. Output is a set of concrete topics that intersect the
query — semantically identical to the prior linear search, just pruned
by the trie structure.
"""
from __future__ import annotations

from typing import Iterable, Iterator, List


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
        path: List[dict] = [self._root]
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
        """Yield concrete topics that intersect ``query_key``.

        Handles ``*`` (single-segment) and ``**`` (multi-segment) wildcards
        anywhere in the query. Concrete queries (no wildcards) are an O(depth)
        walk; wildcards may fan out across multiple branches.
        """
        segs = query_key.split('/')
        yield from self._walk(self._root, segs, 0)

    def _walk(self, node: dict, segs: List[str], i: int) -> Iterator[str]:
        if i == len(segs):
            leaves = node.get(_LEAF_KEY)
            if leaves:
                yield from leaves
            return
        seg = segs[i]
        if seg == '**':
            # Match zero or more remaining segments. ``**`` may be the LAST
            # segment of the query (the only form zeared currently produces),
            # but handle anywhere for completeness.
            remaining = segs[i + 1:]
            if not remaining:
                # Trailing `**` matches one-or-more remaining segments.
                # Emit only descendants — leaves at the current node
                # would correspond to a zero-segment match.
                for k, child in node.items():
                    if k == _LEAF_KEY:
                        continue
                    yield from self._all_leaves(child)
            else:
                # `a/**/b/c` — ** matches some number of segments before
                # `b/c` lines up. We delegate to a recursive walk that
                # tries every depth.
                yield from self._walk_starstar(node, remaining)
        elif seg == '*':
            for k, child in node.items():
                if k == _LEAF_KEY:
                    continue
                yield from self._walk(child, segs, i + 1)
        else:
            child = node.get(seg)
            if child is not None:
                yield from self._walk(child, segs, i + 1)

    def _walk_starstar(self, node: dict, remaining: List[str]) -> Iterator[str]:
        """Handle non-trailing ``**``: try each depth where ``remaining``
        could line up against the trie structure."""
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

    def _all_leaves(self, node: dict) -> Iterator[str]:
        leaves = node.get(_LEAF_KEY)
        if leaves:
            yield from leaves
        for k, child in node.items():
            if k == _LEAF_KEY:
                continue
            yield from self._all_leaves(child)
