# `keyexpr.py` — matching keys as Zenoh does

```python
from zeared.keyexpr import includes, intersects, is_concrete, shape

includes('rio/**/rtls/**', 'rio/config/geometry/rtls/r1')   # True: the rule reaches the key
intersects('fleet/*/status', 'fleet/r1/**')                 # True: one key could match both
shape('fleet/{robot}/log/{rest**}')                         # 'fleet/*/log/*/**'
is_concrete('r-$*')                                         # False: a wildcard chunk
```

New in 0.4.2. Anything that matches or shapes keys by hand, such as an
access rule, a route or a gateway's grant, should ask here rather than split on
`/` itself. The answers are Zenoh's own, from the `zenoh.KeyExpr` the mesh
routes by, so they can't drift from what a subscriber, a queryable or a
getter actually reaches. zeared already depends on Zenoh, so this costs
nothing new, and it's quicker than matching in Python: about 0.4 µs a call,
both keys built.

## What the wildcards mean

| Chunk | Matches |
|---|---|
| `*` | exactly one chunk |
| `**` | any number of chunks, none included: `fleet/**` matches `fleet` |
| `$*` within a chunk | any run of characters in that chunk, none included: `r-$*` matches `r-7` and `r-` |

A key is canonized before it's compared, so `a/**/**` reads as `a/**`. A
`*` anywhere else, as in `read-*`, isn't a key expression: Zenoh writes
that wildcard `read-$*`.

## The functions

| Function | Answers |
|---|---|
| `intersects(a, b)` | Could one key match both? A subscription on one receives a put on the other, and a getter on one reaches a queryable on the other |
| `includes(a, b)` | Does `a` match every key `b` matches? With a concrete `b`: does `a` match that key? |
| `shape(topic)` | The key expression of every key a `TOPIC` template can render or decode |
| `is_concrete(key)` | Does `key` name one key, with no `*`, `**` or `$*`? |

Each takes key expressions as strings, and raises
[`KeyExprError`](errors.md) for one that isn't, with Zenoh's reason (and a
hint for `read-*`). `shape` raises `TopicError` for a template that isn't
one.

### `shape(topic)`

```python
shape('fleet/{robot}/status')          # 'fleet/*/status'
shape('files/{path**}')                # 'files/*/**'
shape('fleet/**')                      # 'fleet/*/**'
```

`{name}` becomes `*`. A trailing `{name**}` becomes `*/**`, one chunk or
more, because a tail slot never renders empty. A trailing anonymous `**`
becomes the same, because it never decodes empty. That's what makes
`shape` exact for reasoning about a class's keys. A subscriber declares
the template's `wildcard` instead, which keeps `**`: subscribing a little
wider costs nothing.

A wildcard in a template's literal text stays a wildcard.

### `is_concrete(key)`

A queryable's `ctx.captures` keeps a wildcard chunk as written: a query on
`fleet/position/*` gives `{'robot': '*'}`. That's how a handler learns the
query was wide:

```python
def on_position(ctx):
    robot = ctx.captures.get('robot', '*')
    if is_concrete(robot):
        return [positions[robot]]
    return [p for name, p in positions.items() if includes(f'fleet/position/{robot}', f'fleet/position/{name}')]
```

## Access checks

An access check is the functions in combination. Each line is one call:

| Question | Call |
|---|---|
| May this rule reach this key? | `includes(rule, key)` |
| Could this route ever deliver something the rule allows? | `intersects(rule, shape(topic))` |
| Does the rule cover everything the route can carry? | `includes(rule, shape(topic))` |

These are the questions rio-gateway's `auth/scope.py` (`match_key`,
`can_match`) and zemqtt's `acl.py` (`match_key`, `can_match`, `subsumes`)
answer by hand. The policy around them stays with each consumer: rio's
operations and roles, and zemqtt's allow and deny by direction. Only the
matching is shared.

Moving one of them over changes one answer: both read `read-*` as "`read-`
then anything", which Zenoh refuses as a key expression. Write `read-$*`.
