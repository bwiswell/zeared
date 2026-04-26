# `_prefix_index.py`

Internal trie-based index of path-segmented topics. Used by
[`retention`](retention.md) and [`presence`](presence.md) queryable
handlers to avoid the O(N) iterate-and-intersect loop on every incoming
query.

## Why it exists

Cached concrete topics (retention) and stashed will keys (presence) are
both indexed by full key — and in both cases, every incoming query has
to find which cached entries intersect the query's key expression.
Pre-0.0.10 this was a linear scan with `query_ke.intersects(...)` per
entry, fine at small N but quadratic when both the cache and the query
rate grow.

A trie of path segments turns this into O(query depth × matches): a
concrete query is one walk down the trie, a `*` fans out only across the
direct children, and a trailing `**` enumerates the subtree.

## Surface

```python
class _PrefixIndex:
    def add(self, topic: str) -> None: ...        # insert concrete
    def remove(self, topic: str) -> None: ...     # idempotent
    def matching(self, query_key: str) -> Iterator[str]: ...
    def __len__(self) -> int: ...
```

`add` and `remove` operate on **concrete** topics (no wildcards).
`matching` accepts a query key that may carry single-segment `*` and
multi-segment `**` wildcards — single-segment `*` matches one path
segment (no slash crossing), `**` inside the query matches zero or more
segments, trailing `**` matches one or more.

## Wildcard semantics

These match the regex-equivalent reference implementation in
`tests/test_prefix_index.py::_linear_match`:

| Query | Equivalent regex |
|-------|------------------|
| `a/b/c`     | `^a/b/c$`         |
| `a/*/c`     | `^a/[^/]+/c$`     |
| `a/**`      | `^a/.+$` (one or more trailing) |
| `a/**/c`    | `^a/.*?/c$` (zero or more inside) |

Output is identical to the prior linear scan; pinned by parity tests
across a randomised topic-and-query corpus.

## Implementation notes

Each trie node is a plain `dict`. Child entries map segment → child-node;
the special key `__topics__` maps to a `set[str]` of concrete topics
whose path ends at that node. `remove` walks back up after a leaf
removal to prune empty interior branches so the trie stays compact.
