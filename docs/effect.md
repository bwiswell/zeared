# `effect.py`

`EFFECT` declares what acting on a contract does (0.3.12). It is
documentation:
- zeared never reads it at runtime, so no send, subscribe, query or serve
  path changes;
- it never rides the wire.

`zeared.doc` shows it on generated pages and in TypeScript.
`z.effect_of(cls)` reads and validates it for a policy layer, which decides
what to do about it. A gateway, an MCP bridge, an MQTT bridge, or a console
that confirms before sending are all policy layers in this sense.

## Declaring

```python
@z.zeared
class SetMode(z.Message):
    TOPIC  = 'device/{id}/mode'
    EFFECT = z.Effect.WRITE                 # or by name: EFFECT = 'write'
    id:   int = z.Int(required=True)
    mode: str = z.Str(required=True)
```

`ty` checks a name against `'read' | 'write' | 'action'`. `effect_of` and
`zeared.doc` hand back the `Effect` member either way.

## The vocabulary

| Effect | Means | Retry-safe | HTTP analogue | Examples |
|---|---|---|---|---|
| `READ` | Acting on it changes nothing a caller could observe | Yes | `GET` (safe) | a lookup, a status or snapshot query, a search, a computed reply |
| `WRITE` | It changes state, but doing it twice leaves the same state as doing it once | Yes | `PUT` / `DELETE` (idempotent) | set a config value, upsert a record, clear an alarm, put a device into a named mode |
| `ACTION` | Anything else: repeating it repeats the effect, or the effect leaves the system | No | `POST` | start or stop a job, trigger a cycle, increment, append, send a notification, move an actuator, reboot |

**Incidental effects don't count.** Logging, metrics, caches and audit
records don't make a request a `WRITE`. As with HTTP's *safe*, what counts
is the effect the caller asks for.

## Order

`READ < WRITE < ACTION`, ranked by how much care a caller owes before
sending:
- `READ`: none;
- `WRITE`: authorization, but a retry is harmless;
- `ACTION`: authorization, no blind retry, and often a confirmation.

`Effect` is a `StrEnum`, so its values are the plain strings `read`,
`write` and `action`. Its comparisons use the rank, not string order.

```python
z.Effect.READ < z.Effect.WRITE < z.Effect.ACTION     # True
max(effects)                                         # the strongest
z.Effect.ACTION > 'read'                             # True: a name compares as its member
z.Effect.READ < 'banana'                             # TypeError
```

An operand that's neither an `Effect` nor an effect name raises
`TypeError`. It doesn't fall back to `str`'s lexical order, in which
`'action' < 'read'`. Equality and hashing stay `str`'s:
`z.Effect.READ == 'read'`.

## What it describes

`EFFECT` describes **acting on a message of the contract**: what happens
when the mesh does what the message asks.
- **A queryable:** the effect of serving a query. Declare it on the class
  with the `TOPIC`, the one you call `on_query` / `query` on. It doesn't
  go on the `REQUEST` payload class.
- **A command that is published rather than queried:** the effect of
  delivering it to the subscribers that act on it. An MQTT-style command
  topic is the usual case.

**Some operations are reads by construction:** subscribing, fetching
retained values, and watching presence. They need no declaration.

**A state or telemetry stream normally leaves `EFFECT` undeclared.**
Publishing on it is a producer reporting state, not a request. Who may
publish is a policy question, not a protocol fact.

## Contracts whose requests differ

Some contracts take requests with different effects; the usual case is a
command envelope with an action field. **Declare the strongest effect any
of its requests can have.**

The declaration is then an upper bound. A consumer that wants per-request
detail keeps its own table, which may refine a request *below* the
declared effect, never above it.

## Undeclared is unknown

`EFFECT` defaults to `None`, and zeared never infers a value. That holds
even for a `RETAINED` class or a class without a `REQUEST`.

A policy layer chooses its own default for an undeclared contract. The
conservative choice is to treat it as `ACTION`.

## Inheritance

`EFFECT` is an ordinary class attribute:
- subclasses inherit it;
- a subclass may override it, including with `None`;
- a shared base can declare it for a whole family of contracts.

## `effect_of(cls)`

Returns the effect `cls` declares, or `None` when it declares none. It
reads `cls.EFFECT`, so it works on any class, not only a `Message`.

| `EFFECT` | `effect_of` |
|---|---|
| `None`, or not set | `None` |
| an `Effect` | that member |
| an effect name (`'write'`) | its member (`Effect.WRITE`) |
| any other string | raises `ValueError`, naming the class and the three values |
| anything else | raises `TypeError` |

## Where it shows up

- **`MessageDoc.effect`**, from `zeared.doc.introspect_message`.
- **The generated page's wire line**, last:
  ``Encoding `msgpack` · … · Effect `write` ``.
- **The TypeScript interface's JSDoc**: `@effect write`, after `@encoding`.

Both renderings appear only when a class declares `EFFECT`. An undeclared
class renders exactly as it did before 0.3.12, so `EFFECT` alone doesn't
break existing `zeared.doc --check` and TypeScript `--check` guards. The
0.4.0 upgrade does, once: generated TypeScript writes `| null` only for
`nullable=True` fields, and generated docs show seared 0.4.0's `None`
defaults, so regenerate committed docs and types after upgrading.

## Validation

`effect_of` validates, and `introspect_message` calls it. So a bad value
fails doc generation, the TypeScript emit and their `--check` hooks: the
first place a protocol package's own tooling looks at its contracts.

Nothing validates `EFFECT` at class creation or on the messaging paths. A
typo in documentation shouldn't stop a service from starting.

## Mapping it onto a policy

One consumer's mapping, not zeared's:

| Effect | HTTP | MCP tool annotations |
|---|---|---|
| `READ` | safe | `readOnlyHint: true` |
| `WRITE` | idempotent | `readOnlyHint: false`, `idempotentHint: true` |
| `ACTION` | neither | `readOnlyHint: false`, `idempotentHint: false` |

`EFFECT` doesn't determine MCP's `destructiveHint` (destructive versus
additive) or `openWorldHint`; the consumer sets those.
