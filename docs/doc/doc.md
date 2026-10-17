# `doc/`

Generated documentation for `@zeared` message classes, in two forms:

- **Markdown wire docs** — one page per class. A `Message` page adds the
  wire contract (topic, slots, schema version, encoding, retention,
  liveliness, effect) to `seared.doc`'s field / enum / variant rendering;
  any other `@zeared` class (a nested payload, a `REQUEST` body) gets
  seared's own page.
- **TypeScript types** — one `.ts` module of the JSON shapes a browser
  consumer sees after a bridge decodes zeared messages. Types only: the
  mesh→websocket bridge (auth, identity scope, transport) is the
  consumer's to build.

Both run on `seared.doc`'s pipeline: discovery, the reference closure,
output paths, cross-links, the index and `--check`. zeared adds a
Message-aware page renderer and the TypeScript emitter.

## Command line

```sh
python -m zeared.doc fleet_msgs -o docs             # a page per class + index.md
python -m zeared.doc fleet_msgs -o docs --check     # CI guard: exit 1 on drift, writes nothing
zeared-doc fleet_msgs -o docs                       # the same, as a console script

python -m zeared.doc.typescript fleet_msgs -o fleet.ts           # one .ts module
python -m zeared.doc.typescript fleet_msgs -o fleet.ts --check   # CI guard
```

| `python -m zeared.doc` / `zeared-doc` | |
|---------------------------------------|---|
| `TARGET` | module or package to scan (dotted path); a package's submodules are imported too |
| `-o` / `--output DIR` | output directory, default `./docs` |
| `--check` | compare with `DIR` without writing; exit 1 and list each missing or stale page |

| `python -m zeared.doc.typescript` | |
|-----------------------------------|---|
| `TARGET` | module or package to introspect |
| `-o` / `--output FILE` | the `.ts` file to write; stdout without it |
| `--check` | exit 1 if `FILE` is missing or out of date; requires `-o` |

Discovery takes every `Seared` subclass with declared fields (`z.Message`,
`z.Zeared`) in the target's modules, plus every class they reference
(`T` targets, `Union` variants). A class whose name starts with `_` is
left out unless a public one references it. Pages land under directories
mirroring the target's subpackages; a referenced class from outside the
target lands at the root.

## Public API

From `zeared.doc` (`__init__.py`):

| Name | |
|------|---|
| `document(cls, *, link_for=...)` | Markdown for one class: the wire-aware page for a Message, seared's page otherwise |
| `render_one(cls, link_for)` | the same, in the shape `seared.doc`'s pipeline calls |
| `build_docs(target)` | `{path: markdown}` for every class under `target`, `index.md` included; no disk I/O |
| `main(argv=None)` | the `python -m zeared.doc` / `zeared-doc` CLI; returns the exit code |
| `introspect(cls)` | a `MessageDoc` for a Message, else seared's `SchemaDoc` |
| `introspect_message(cls)` | the `MessageDoc` for a Message class |
| `is_message_class(obj)` | `True` for a concrete `Message` subclass with declared fields (not the base) |
| `MessageDoc` | seared's `SchemaDoc` (`schema`) plus `topic`, `extra_topics`, `slots`, `category`, `schema_version`, `encoding`, `retained`, `liveliness`, `dedupe`, `retention_ttl`, `request` and `effect` |
| `SlotDoc` | one topic slot: `name`, and `is_field` (also a declared payload field) |
| `render_message(cls, link_for=...)` | the full Message page |
| `render_topic(md)` / `render_wire(md, link_for=...)` | the page's Topic section / wire line |
| `generate_ts(target)` | the `.ts` source for every class under `target` |
| `emit_ts(classes)` | the `.ts` source for the given classes and everything they reference |
| `render_interface(cls)` | one `export interface`, with its JSDoc |
| `ts_type(field)` | the TypeScript type expression for one seared field |

`introspect_message` reads `EFFECT` through `z.effect_of`, so a bad
declaration fails doc generation, the TypeScript emit and both `--check`
guards (see [`effect.md`](../effect.md)).

## The Message page

`render_message` builds the page from `introspect_message`:

- **Header** — `# Name`; a badge line with the topic's second segment when
  it is a literal (`fleet/status/{robot}` → `status`) and `schema <SCHEMA>`
  when set; then the class docstring.
- **Topic** — the canonical `TOPIC`, each `EXTRA_TOPICS` entry marked
  `(extra)`, and a table of the canonical topic's `{name}` slots saying
  whether each is also a payload field (`yes`) or capture-only (`no`). A
  trailing `{name**}` slot isn't listed.
- **Wire line** — `Encoding`, `Retained`, `Liveliness`, `Dedupe`,
  `Retention TTL` (`∞` when unset), then `Request` (a link to the
  `REQUEST` class) when set and `Effect` when declared.
- Then seared's fields table, enumerations and variants, and a
  *do not edit by hand* footer.

For this module:

```python
import datetime as dt
import enum

import zeared


class Mode(enum.Enum):
    IDLE = 'idle'
    DRIVING = 'driving'


@zeared.zeared
class Pose(zeared.Zeared):
    """A planar pose."""

    x: float = zeared.Float(required=True, doc='metres')
    y: float = zeared.Float(required=True, doc='metres')


@zeared.zeared
class RobotStatus(zeared.Message):
    """Periodic status from one robot."""

    TOPIC = 'fleet/status/{robot}'
    SCHEMA = '1'
    RETAINED = True
    EFFECT = zeared.Effect.READ

    robot: str = zeared.Str(required=True)
    mode: Mode = zeared.Enum(enum=Mode, required=True)
    pose: Pose = zeared.T(Pose, required=True)
    battery: float | None = zeared.Float(default=None, doc='state of charge, 0 to 1')
    fault: str | None = zeared.Str(nullable=True, default=None)
    uptime: dt.timedelta = zeared.TimeDelta(data_key='up_s', default=dt.timedelta(0))
    tags: list[str] = zeared.Str(many=True, default_factory=list)
```

`python -m zeared.doc fleet_msgs -o docs` writes `Pose.md`, `index.md` and
this `RobotStatus.md`:

````markdown
# RobotStatus

`status` · schema `1`

Periodic status from one robot.

## Topic

`fleet/status/{robot}`

| slot | payload field? |
|------|----------------|
| `robot` | yes |

Encoding `msgpack` · Retained `yes` · Liveliness `no` · Dedupe `yes` · Retention TTL `∞` · Effect `read`

## Fields

| field | wire key | type | required | default | notes |
|-------|----------|------|----------|---------|-------|
| `robot` | — | `str` | ✓ | — |  |
| `mode` | — | `Mode` | ✓ | — |  |
| `pose` | — | [`Pose`](Pose.md) | ✓ | — |  |
| `battery` | — | `float \| None` |  | `None` | state of charge, 0 to 1 |
| `fault` | — | `str \| None` |  | `None` | nullable: `null` loads as `None` |
| `uptime` | `up_s` | `timedelta` |  | `datetime.timedelta(0)` |  |
| `tags` | — | `list[str]` |  | `<factory: list>` |  |

## Enumerations

### `mode` → `Mode`

| member | wire value |
|--------|------------|
| `IDLE` | `'idle'` |
| `DRIVING` | `'driving'` |

_Generated by `zeared.doc` — do not edit by hand._
````

The fields table is seared's: since 0.4.0 (on seared 0.4.0) it shows a
`None` default as `None` and notes a `nullable=True` field.

## TypeScript

`generate_ts` discovers classes as the Markdown pipeline does, then adds
each class's references and its `REQUEST` class, transitively. The module
it emits holds a header comment stating the wire-shape contract, the
enums as type aliases, any `TS_PRELUDE` aliases, then one
`export interface` per class, sorted by module and name so the output
doesn't depend on discovery order.

`ts_type` maps each seared field:

| Field | TypeScript |
|-------|------------|
| `Bool` | `boolean` |
| `Int`, `Float` | `number` |
| `Str`, `UUID`, `Path`, `Date`, `DateTime`, `Time` | `string` |
| `TimeDelta` | `number`, its seconds |
| `Bytes` | `string`: base64, or hex under `encoding='hex'` |
| `Decimal` | `string`; `number` with `as_number=True` |
| `Dict` | `Record<string, unknown>` |
| `NDArray`, `PandasFrame`, `PolarsFrame` | `unknown`: decode frames in the bridge |
| `Enum(enum=E)` | `E`, emitted as `export type E = "a" \| "b";` over the wire values (number literals for int values) |
| `T(Cls)` | `Cls`, a reference to its interface |
| `Union` | a union discriminated on `tag_key`: `(A & { type: "a" }) \| …` when flat, `{ type: "a"; payload: A } \| …` with `payload_key="payload"` |
| `Tuple(f1, f2, …)` | `[t1, t2, …]` |
| a field class with `TS_TYPE` | that type; its `TS_PRELUDE`, if any, is emitted once before the interfaces |
| anything else | `unknown` |

`keyed=True` wraps the type as `Record<string, T>`, and `many=True` as
`T[]` (parenthesised for a union).

Each member:

- is keyed by its **wire key** (`data_key` when set), quoted when it isn't
  a JS identifier;
- takes `?` unless the field is `required`;
- takes `| null` only for a `nullable=True` field (0.4.0), the only kind
  whose wire value may be `null`: an ordinary optional field
  (`X | None`, `default=None`) is `opt?: T`, absent when `None`, and a
  required nullable one is `req: T | null`;
- carries its field's `doc=` as a `/** … */` comment.

`dump=False` (load-only) fields are left out. A template slot that is a
declared field is a member; a capture-only slot isn't.

Each interface's JSDoc holds the docstring's first line (Sphinx roles
stripped) and, for a Message, `@topic`, an `@extraTopic` per extra topic,
`@schemaVersion` when `SCHEMA` is set, `@encoding`, and `@effect` when
`EFFECT` is declared.

For the module above, after the header comment:

```ts
export type Mode = "idle" | "driving";

/**
 * A planar pose.
 */
export interface Pose {
  /** metres */
  x: number;
  /** metres */
  y: number;
}

/**
 * Periodic status from one robot.
 * @topic fleet/status/{robot}
 * @schemaVersion 1
 * @encoding msgpack
 * @effect read
 */
export interface RobotStatus {
  robot: string;
  mode: Mode;
  pose: Pose;
  /** state of charge, 0 to 1 */
  battery?: number;
  fault?: string | null;
  up_s?: number;
  tags?: string[];
}
```

## What lives where

```
doc/
├── __init__.py      # the public surface above
├── __main__.py      # `python -m zeared.doc`
├── generate.py      # render_one / document / introspect / build_docs / main, over seared.doc's pipeline
├── introspect.py    # MessageDoc, SlotDoc, introspect_message, is_message_class
├── render.py        # render_message / render_topic / render_wire
└── typescript.py    # ts_type / render_interface / emit / generate + `python -m zeared.doc.typescript`
```
