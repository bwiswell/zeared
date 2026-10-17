# `_prefix_index.py`

Internal trie-based index of path-segmented topics. Used by the
[`retention`](retention.md) queryable handler to avoid the O(N)
iterate-and-intersect loop on every incoming query.

## Why it exists

Cached concrete topics (retention) are indexed by full key, and every
incoming query has to find which cached entries intersect the query's key
expression. (Presence's stashed will keys used it too, until 0.4.0.)
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

What `matching` returns, as an equivalent regex:

| Query | Equivalent regex |
|-------|------------------|
| `a/b/c`     | `^a/b/c$`         |
| `a/*/c`     | `^a/[^/]+/c$`     |
| `a/**`      | `^a/.+$` (one or more trailing) |
| `a/**/c`    | `^a(/.+)?/c$` (zero or more inside: `a/c`, `a/b/c`, `a/b/x/c`) |

For concrete, `*` and trailing-`**` queries the output is identical to
the regex reference `tests/test__prefix_index.py::_linear_match`, pinned
by parity tests across a randomised topic-and-query corpus. The corpus
never puts `**` inside a query (zeared's own selectors only end in one),
and the reference's `.*?` there wouldn't match `a/c`;
`test_starstar_inside_pattern` pins the trie's zero-or-more behaviour
instead.

A trailing `**` here needs at least one segment, where Zenoh's own `**`
also matches none (`a/**` intersects `a`), so a query `a/**` doesn't
return a cached topic that is exactly `a`.

## Implementation notes

Each trie node is a plain `dict`. Child entries map segment → child-node;
the special key `__topics__` maps to a `set[str]` of concrete topics
whose path ends at that node. `remove` walks back up after a leaf
removal to prune empty interior branches so the trie stays compact.
