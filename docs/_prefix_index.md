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
direct children, and a `**` enumerates the subtree.

## Surface

```python
class _PrefixIndex:
    def add(self, topic: str) -> None: ...        # insert concrete
    def remove(self, topic: str) -> None: ...     # idempotent
    def matching(self, query_key: str) -> Iterator[str]: ...
    def __len__(self) -> int: ...
```

`add` and `remove` operate on **concrete** topics (no wildcards).
`matching` yields each cached topic the query key intersects, once, as
Zenoh decides it: `*` is one chunk, `**` any number of chunks (none
included), and `$*` any run within one chunk.

## How it decides (0.4.2)

The walk only narrows. It may collect a topic the query doesn't reach,
but never misses one that it does:
- a chunk holding `$*` walks every child, as `*` does;
- `**` takes zero chunks here, or one or more below.

Each candidate is then confirmed with
[`zeared.keyexpr.intersects`](keyexpr.md), Zenoh's own answer, so the
index returns exactly what a linear scan with `intersects` would.
`tests/test__prefix_index.py` pins that parity across a randomised corpus
of queries, with `*`, `$*` and `**` anywhere in them.

Until 0.4.2 the walk was the answer, and it differed from Zenoh twice:
- **A `$*` chunk was looked up literally,** so it matched nothing:
  `Status.query(robot='r-$*')` got no retained values.
- **A trailing `**` needed at least one chunk,** so a getter on `a/**` missed
  a value retained at `a`.

A query with two `**` could also yield a topic more than once, and
retention replied once per yield.

## Implementation notes

Each trie node is a plain `dict`. Child entries map segment → child-node;
the special key `__topics__` maps to a `set[str]` of concrete topics
whose path ends at that node. `remove` walks back up after a leaf
removal to prune empty interior branches so the trie stays compact.
