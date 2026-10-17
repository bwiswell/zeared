# Changelog

Each version of zeared, newest first.

A **Breaking** line means code written for the version before may fail, or behave differently. A **Wire change**
line means what a message puts on the wire (its key, payload or attachments) differs from the version before, which
nodes on other versions, retained state and access rules can see.

The top entry, while it reads *unreleased*, collects what has merged for the next version; each change adds its own
lines as it lands, and the entry is dated when that version is released.

## 0.4.0 — unreleased

**seared 0.4.0, TypeScript `| null` only where the wire can carry one, single-segment slots that can't split their
key, and the changelog in the repository.**

### Added

- **`CHANGELOG.md`**, this file: every release from 0.1.0 on, newest first. It is kept in the repository and grows
  with each change as it merges, where it had been reconstructed after each release.

### Changed

- **Breaking:** generated TypeScript writes `| null` only for `nullable=True` fields, the only kind whose wire value
  may be `null`; an ordinary optional field is `opt?: T`, and a nullable required one `req: T | null`. Generated
  docs show seared 0.4.0's `None` defaults and nullable notes. Regenerate committed docs and types.
- **Wire change:** under msgpack, the default encoding, a `Bytes` slot inside a `z.Tuple` field is written as raw
  bytes, as a top-level `Bytes` field is, where it was base64 text. This comes from seared 0.4.0. `Bytes` reads
  either form, so nodes on 0.3.x and 0.4.0 still read each other's payloads; the JSON wire is unchanged.
- The README gains *Upgrading to 0.4.0*, which lists each break with who it reaches and what to do.
- **Dependencies:** seared follows its `version/0.4.0` branch (`>=0.4.0,<0.5.0`), with the lockfile pinning a
  commit, until seared 0.4.0 is tagged. seared 0.4.0 reads a wire `null` on a field that isn't nullable as absent,
  refuses lossy numeric coercion (a fractional float into an `Int`, a `2` into a `Bool`), prefixes validation errors
  with their location and renames `Union(default=)` to `fallback=`; see its changelog.
- **Breaking:** a single-segment slot bound to `Path`, or to `Bytes` in its default base64 form, raises
  `TopicError` at first use, since either can render a `/` and split the key. Use `Bytes(encoding='hex')` or a
  `Str` slot, or a trailing `{name**}` slot for a path.
- **Breaking:** publishing with a `/` inside a single-segment slot value raises `TopicError`, from `send`,
  `unretain`, `concrete_topic`, `register_will` and a queryable's replies, where the message had gone out on a key
  no template matched. The query getters' selectors don't check, since they embed wildcards deliberately.

## 0.3.13 — 2026-10-16

**Releases are tagged, and seared is pinned by tag.** No library code changes. uv doesn't check a version range
against a git source, so until now nothing kept a consumer, or zeared itself, from taking the next breaking release
of seared or zeared on its next upgrade. From this release, each merge to `master` is tagged, and zeared pins seared
to a tag. The benchmark numbers are re-recorded on seared 0.3.4.

### Added

- **Release tags.** A version merged to `master` is tagged `v<version>` with a GitHub release, by a workflow that
  refuses to run if `pyproject.toml` and `zeared/__init__.py` disagree on the version. This release is the first,
  `v0.3.13`.
- **A test keeps the two version stamps in step,** so a mismatch fails on the branch rather than at the merge.
- **A `prek` hook refuses commits on `master`,** which moves only by merging a release branch.

### Changed

- **The README's *Setup* installs a release by its tag** and explains why: the tag is the pin. It also says what a
  project that declares its own seared source must match. Its URLs take the form every source in the family uses,
  `https://github.com/bwiswell/zeared`; uv treats `https://www.github.com/bwiswell/zeared.git` as a different
  source.
- *Development* lists the sixth check and how a release is cut.
- **Every benchmark number is re-recorded,** as the median of 3 runs, on zeared 0.3.13, seared 0.3.4, `rusted`
  0.3.1 and eclipse-zenoh 1.10.1. seared 0.3.4 restores pure-Python speed that 0.3.2 and 0.3.3 had lost, and the
  pure-Python rows gain 7 to 18% end to end; the accelerated rows don't move. Against pydantic over MQTT, default
  zeared comes out 0.70× at QoS 0 and 2.29× at QoS 1; with `rusted`, 1.28× and 4.20×.
- **The benchmarks prose drops a claim the runs don't support:** publisher caching's gain on the static-`TOPIC`
  benchmark, which sits within run-to-run noise. The note that MQTT publish rates overstate what the subscriber saw
  now gives a generated figure per QoS level, about 2x at QoS 0 and 6x at QoS 1, where it had said 2x for both.
- **Dependencies:** seared's source is pinned to `tag = "v0.3.4"`, and the lockfile moves from seared 0.3.3 to
  0.3.4, which changes nothing `load` or `dump` does. The `>=0.3.3,<0.4.0` range is unchanged. A project that
  declares its own seared source must pin the same tag: while seared's `master` is still that commit an untagged
  source agrees with it, and once `master` moves on, uv reports conflicting URLs instead of resolving.

### Fixed

- Seventeen links between doc pages that pointed at the wrong path.

## 0.3.12 — 2026-10-16

**Contracts can declare their effect.** A new class attribute, `EFFECT`, says what acting on a contract does, for a
policy layer such as a gateway or an MCP or MQTT bridge to read. It is documentation only: zeared never reads it at
runtime, and nothing about it rides the wire.

### Added

- **`EFFECT`**, a class attribute that says what acting on a contract does. `READ` changes nothing a caller could
  observe; `WRITE` changes state, but repeating it leaves the same state; `ACTION` is anything else. For a queryable
  it describes serving a query, and for a command that is published, delivering it. A contract whose requests differ
  declares the strongest, and subclasses inherit it. It defaults to `None`, meaning undeclared: zeared never infers
  an effect.
- **`z.Effect`**, a `StrEnum` ordered `READ < WRITE < ACTION`. Comparisons use that order rather than string order,
  so `max()` of several effects is the strongest. An effect name compares as its member, and any other operand
  raises `TypeError`.
- **`z.effect_of(cls)`** returns the effect any class declares, normalising a name to its member, or `None`. An
  unknown name raises `ValueError`, and any other type `TypeError`.
- **Generated docs show a declared effect.** `MessageDoc` gains `effect`, the doc page's wire line ends with it,
  and the TypeScript interface's JSDoc gains `@effect` after `@encoding`. A bad declaration fails doc generation and
  its `--check`. A class that declares no effect generates exactly what it did before.

### Changed

- The docs gain a page on the effect vocabulary, and the class attributes table in `docs/message/message.md` gains
  the `DEDUPE_MAX_SKEW`, `RETENTION_TTL` and `REQUEST` rows it lacked.

## 0.3.11 — 2026-10-16

**Benchmark numbers generated from their artifact.** Bench and docs only; no library code changes. Every benchmark
number in the README and `docs/overview/benchmarks.md` now comes from `bench/results.json`, as the median of its
runs, and a check fails when a doc drifts from it. The artifact is re-recorded on the current stack.

### Added

- **Generated benchmark numbers.** `bench/tables.py` renders every table, and every number the prose leans on,
  between `<!-- bench:NAME -->` markers in the README and `docs/overview/benchmarks.md`; the prose outside them
  stays hand-written. `python -m bench` re-renders them after recording to the committed artifact, and
  `python -m bench.tables --check` exits 1 when a doc has drifted from it, also as a new `prek` hook.
- **`python -m bench --runs N`** runs every suite N times and tags each measurement with its run. The docs quote
  the median, every ratio is a ratio of medians, and the *Run-to-run variance* table shows how far each stacks row
  moved between runs. The committed artifact is recorded with `--runs 3`; one recorded before 0.3.11 still loads, as
  a single run.
- **A *Throughput suite* table.** The suite's results were recorded but shown nowhere.

### Changed

- **Every number is re-recorded**, as the median of 3 runs, on zeared 0.3.11, seared 0.3.3, `rusted` 0.3.1 and
  eclipse-zenoh 1.10.1. Against pydantic over MQTT, default zeared comes out 0.73× at QoS 0 and 2.23× at QoS 1; with
  `rusted`, 1.55× and 4.74×. The variance and verdict tables, which quoted medians of runs the artifact didn't
  store, now come from it.
- **The benchmarks prose says what the runs show.** The variance text cites the largest run-to-run spread in the
  artifact (28%) instead of claiming the stacks rows move most, which the runs didn't bear out; numbers the prose
  only repeated from the table above are gone; and the async headline's JSON row no longer claims the same wire as
  msgpack.
- The README's *Development* section documents how the artifact is recorded, with `rusted` installed for the run,
  and the fifth `prek` hook.

## 0.3.10 — 2026-10-16

**Hub restarts, characterised.** Tests and docs only; no library code changes. Ahead of 0.4.0's restart work, the
suite gains a hub that restarts in-process and tests that pin how zeared behaves across a restart today, and the
docs say plainly that auto-reconnect doesn't cover a hub outage.

### Added

- **Restart characterisation tests** (`tests/test_hub_restart.py`, marked `restart`): a real hub restart,
  in-process, with a client whose re-dial is slowed so the restarted hub's boot publish lands while the client is
  detached. They pin that `on_reconnect` doesn't fire, that a value retained during the outage isn't replayed though
  `fetch_retained` recovers it, that the restarted publisher's death goes unannounced, and that Zenoh's liveliness
  history and its transport and link events report the restart. `-m "not restart"` deselects them. Another test pins
  that a bounded `alisten` drops what arrives while its queue is full, each drop a `QueueFull` raised inside a loop
  callback.

### Changed

- **The docs say that a lost router link is not a dead session.** Across a hub restart, auto-reconnect sees nothing
  and `on_reconnect` doesn't fire, and a value retained or a will registered by a publisher that restarted while the
  link was down never reaches the client; `Cls.fetch_retained()` recovers retained state. In the README's
  *Auto-reconnect* and *Limits*, and `docs/_reconnect/_reconnect.md`.

## 0.3.9 — 2026-10-15

**Config files that mean what they say, and docs that say what's so.** A patch release. A hub keeps the listeners
its config names, the hub daemon is a router whatever its config file says, a client can take its routers from its
config, and an uncached send raises the error the docs promise.

### Added

- **A client can open on endpoints from its config.** With no `router` or `connect` (as arguments or
  `SessionConfig` fields), `z.client()` uses the connect endpoints its Zenoh config already names: a raw
  `zenoh_config=`, a `zenoh_config_file` or `zenoh_overrides`. It raised `TypeError` regardless; it still does when
  no endpoints are named anywhere, and the message now lists those options.

### Changed

- **Breaking:** a hub started from a Zenoh config that names its own listen endpoints binds those when no listen is
  given (`listen=`, `SessionConfig.listen` or the daemon's `--listen`), instead of `tcp/0.0.0.0:7447`. That covers
  `z.hub(zenoh_config=…)`, `z.open(SessionConfig(mode=ROUTER, …))` with a `zenoh_config_file` or `zenoh_overrides`,
  and `python -m zeared.hubd --config`. The default applies only when nothing names any. See Fixed.
- **Breaking:** `python -m zeared.hubd --config FILE` applies zeared's settings over the file, as over a
  `SessionConfig.zenoh_config_file`: router mode, timestamping unless `--no-timestamping`, and shared memory off,
  which it already forced. A file that sets `mode: peer`, or no mode, now runs a router, and one that turns
  timestamping off gets it on unless `--no-timestamping` is passed. `z.hub(zenoh_config=…)` still takes a raw config
  as given. See Fixed.
- **Breaking:** the TypeScript generator types a `TimeDelta` field as `number`, its seconds, not `string`.
  TypeScript that treated one as a string stops compiling, against values that were always numbers. See Fixed.
- **One endpoint rule in every mode:** an endpoint field (`router`, `connect`, `listen`) is written over the Zenoh
  config when it is set, and when it isn't, the endpoints the config already names stand. Peers, and a hub's
  `connect`, already worked this way; the hub's `listen` and the client now do too. `docs/config.md` documents the
  rule and its place in the precedence (after `zenoh_overrides`), says that `zenoh_config=` replaces the config
  zeared would build rather than layering over it, and notes that a `mode` override changes the mode Zenoh runs in
  while `z.open` still picks the factory from `SessionConfig.mode`.
- **The hub daemon logs to stdout**, which systemd hands to journald, as its docs said; it logged to stderr. Its
  startup line names the endpoints the hub actually binds.
- **Docs that had drifted** now say what is so:
  - the benchmarks page's prose matches the committed artifact: the async percentages, the `rusted` ratios (1.76×
    and 2.00×), which tables are medians of three runs it doesn't store, and that MQTT has its own retained
    messages, Last Will and (in MQTT 5) request/response;
  - four 0.2.9 features labelled "(0.3.0)" in the README, `docs/async_.md`, `docs/watchdog.md` and
    `docs/presence/presence.md` now say 0.2.9;
  - the README's `TS_TYPE` paragraph no longer interrupts the sentence before the `generate_ts` example it
    introduces;
  - the README's hub example listens and links on TLS, beside its TLS config.

### Fixed

- **An uncached send raises `SessionDeadError` while the managed session is reconnecting or dead**, as the README
  documents and the cached path has since 0.3.1. A `PUBLISHER = False` class, or any class past its publisher cap,
  wrapped it in a plain `ZearedError`, so the documented retry handler never caught it. `SessionDeadError`
  subclasses `ZearedError`, so existing handlers still catch it, and both send paths now choose the error the same
  way.
- **A hub binds its config's own listeners.** Every hub path wrote `listen/endpoints` over the config, so a config
  file listening only on TLS gave a hub listening on plaintext `tcp/0.0.0.0:7447`, and the TLS endpoint was never
  bound; a `listen/endpoints` entry in `zenoh_overrides` was overwritten the same way. A hub given an explicit
  listen was unaffected.
- **The hub daemon runs a router under any `--config`.** A file without `mode` opened a peer, and
  `--no-timestamping` did nothing with `--config`.
- **TypeScript types for `TimeDelta` match the wire**, where seared sends a float of seconds.

## 0.3.8 — 2026-10-15

**One way to render a topic, and session scopes the async wrappers can see.** A patch release. Every change fixes
behaviour that disagreed with itself or with the docs, or adds surface.

### Added

- **`concrete_topic`**, the key a message publishes on, without sending it. `msg.concrete_topic()` renders the
  instance on its canonical template, or on a declared `topic=`. `Cls.concrete_topic(**slots)` renders from slot
  values alone: keyword arguments that aren't slots are ignored, so a whole field dict can be passed, and a missing
  or `None` slot raises `TopicError` rather than widening to a wildcard. It replaces rendering a template by hand
  through `_templates()`, so a computed key always matches the one zeared uses.

### Changed

- **One canonical render builds every concrete key**: `send`, `register_will`, a queryable's `ctx.reply`,
  `unretain` in both forms, the query getters' selectors, and `concrete_topic`. A slot value of its field's own type
  renders as `str(field.serialize(value))`, the field's JSON-carrier string form, whatever the class `ENCODING`:
  exactly what a subscriber's capture decodes back. Any other value renders as `str(value)`, as before, so wildcard
  and pre-rendered strings pass through untouched. The type check is exact, so a `bool` is not an `int`. A new
  README section, *How a slot value becomes a key segment*, documents the rule. `Template.render` and
  `Template.render_selector` are unchanged.
- **Wire change:** `unretain` and the query getters now render `Enum` slots other than `StrEnum` and `IntEnum`,
  `DateTime` slots and `TimeDelta` slots the way `send` always has. See Fixed.
- **Wire change:** a `Bytes` topic slot under msgpack, the default encoding, renders as base64 instead of the
  Python repr of the bytes. Neither form round-trips, because base64 can contain `/`; the README's *Limits* says so,
  and recommends against `Bytes` and `Path` slots.
- **Breaking:** inside a `with z.session(other):` block, `asend`, `aquery`, `aquery_one`, `aquery_iter`,
  `afetch_retained`, `aunretain` and `aregister_will` now use `other`. They resolve on an `asyncio.to_thread`
  worker, which the scope never reached, so they used the default session or raised `NoSessionError`. See Fixed.
- **Session scopes are context-local**, not thread-local. They follow `asyncio.to_thread` into its worker, and each
  asyncio task has its own: tasks interleaving on one loop no longer see each other's scopes, and a task created
  inside a scope keeps it after the block ends. A plain new thread still starts with no scope, except on a
  free-threaded build, where it inherits the scope. Exiting a scope in a different context from the one it was
  entered in raises `ValueError`.
- `coerce_captures` and the subscriber's own decode share one capture coercion, so the two can't drift.
- `zeared.doc.typescript` decides whether an optional field is `| null` from the field's `default`, falling back to
  seared's deprecated `missing` only for a field still declared with it. seared 0.4.0 removes `missing`, which would
  otherwise make every defaulted optional field nullable. Generated output is unchanged.
- **The `alisten(maxsize=…)` docs were wrong.** They said a full queue makes delivery block. It drops the message
  instead, and asyncio logs each drop as `QueueFull`. The docstring, `docs/async_.md` and the README's *Limits* now
  say so. Behaviour is unchanged.
- **Dependencies:** seared `>=0.3.3` (was `>=0.3.0`), and the lockfile moves to seared 0.3.3.

### Fixed

- **`unretain` and query selectors reach the key `send` published on.** A `(str, Enum)` or plain `Enum` member
  rendered as `Zone.NORTH` instead of its value, a `DateTime` with a space instead of ISO's `T`, and a `TimeDelta`
  as `0:00:05` instead of seconds. A tombstone went to a key nothing was published on, and a query came back empty
  with no error.
- **A slot field with a `data_key` works.** Publishing raised `TopicError: missing field`, and receiving raised
  `… is required`: the key rendered from wire keys, and captures were put back under attribute names. Keys now
  render from the attribute, captures decode under the wire key, and the slot is stripped from the payload by its
  wire key. `register_will`, `ctx.reply` and `fetch_retained` follow.
- **A queryable's reply renders through the same path**, so a reply to a typed selector reaches the getter.
- **The async wrappers honour `with z.session(…)`.** See Changed.

## 0.3.7 — 2026-10-03

**Zenoh's shared memory, off.** Large messages between sessions on one host had been silently dropped once a
long-lived node, a hub above all, had seen enough sessions come and go.

### Changed

- **Shared memory is off in every config zeared builds**: by `peer`, `client` and `hub` (and so `open` and the
  async factories), from keyword arguments or a `SessionConfig`, and by the hub daemon. Each sets
  `transport/shared_memory/enabled=false` beside the mode and timestamping it already set, so large same-host
  messages travel over the link like everything else. Only links within one host ever used shared memory, and it is
  agreed per link, so a hub with it off keeps it off every link through the hub, clients on Zenoh's defaults
  included. A session opts back in with a `zenoh_overrides` entry, which lands last. A raw `zenoh_config=` is still
  taken as given, Zenoh's default included, so a hub embedded with `z.hub(zenoh_config=…)` should turn it off there.
  See Fixed.
- **Breaking:** a `zenoh_config_file` that turns shared memory on no longer does: zeared's setting lands on top of
  the file, as its mode and timestamping already did. Use `zenoh_overrides` instead. The hub daemon's `--config`
  file can't turn it on at all: `python -m zeared.hubd` turns it off over the file, and the rest of the file stands.
- **The docs say so:** `docs/config.md` gains a *Shared memory off* section, and its precedence now reads the file,
  then zeared's own settings (mode, timestamping, shared memory off), then `zenoh_overrides`. `docs/hubd.md`,
  `docs/_factories.md`, the daemon's `--config` help and the README's feature list follow.
- `zeared.doc.introspect_message` drops a topic's repeated slots more simply. Its output is the same.
- **Dependencies:** the lockfile is refreshed (eclipse-zenoh 1.10.1, msgpack 1.2.3, seared 0.3.2, ruff, ty and
  prek). No dependency ranges changed.

### Fixed

- **Large same-host messages no longer stop arriving.** With Zenoh's shared-memory transport on, as it is by
  default, a put, query or reply over 3 KB between two sessions on one host went through a shared-memory pool, 16 MB
  a session, instead of over the link. The receiver locks the pages it maps and keeps every closed session's segment
  mapped, so a long-lived node with sessions coming and going climbed toward `RLIMIT_MEMLOCK` (64 MB on a stock
  Linux box). Once there, Zenoh failed to open the next segment and dropped the message, logging it only at DEBUG:
  large messages stopped arriving while small ones kept working.

## 0.3.6 — 2026-09-04

**TypeScript names for custom fields.**

### Added

- **`TS_TYPE` and `TS_PRELUDE` on a field class**, read by the TypeScript generator
  (`python -m zeared.doc.typescript`, `generate_ts`). The generator maps fields by class name, so a downstream
  subclass with a name of its own (a branded string, say) came out as `unknown`. Setting `TS_TYPE = 'SecretRef'` on
  the subclass gives its members that type, with `many`, `keyed` and `| null` applied as for any field. An optional
  `TS_PRELUDE = 'export type SecretRef = string & {...};'` is the alias that defines it, emitted once however many
  fields use it, after the enum aliases and before the interfaces. `Enum`, `Union`, `T`, `Tuple` and `Decimal`
  fields keep their own mapping, and fields without `TS_TYPE` map as before. The README's TypeScript section
  describes both.

### Changed

- **Dependencies:** the lockfile is refreshed (eclipse-zenoh 1.10.0, msgpack 1.2.2, seared 0.3.1, ruff, ty, prek,
  pygments and packaging). No dependency ranges changed.

## 0.3.5 — 2026-09-04

**Typed async getters.** Typing only; nothing changes at runtime.

### Changed

- **The async query getters and `afetch_retained` are generic in the message class**, so a result is typed as the
  class passed in, and callers need no `cast`:
  - `aquery` returns `list[Cls]`, where it returned a bare `list`;
  - `aquery_one` returns `Cls | None`, where it returned `Message | None`;
  - `aquery_iter` returns `AsyncIterator[Cls]`, where it returned a bare `AsyncIterator`;
  - `afetch_retained` returns `list[Cls]`, where it returned a bare `list`.

  They use PEP 695 type parameters to carry through what the sync methods they wrap already declare. `alisten` is
  unchanged: its `meta=` flag changes what it yields, which needs overloads rather than a type parameter.

## 0.3.4 — 2026-09-04

**Streaming query getters.** The serving side already streamed through generator handlers; now a caller can act on
each reply as it lands, instead of waiting out the `timeout` window.

### Added

- **`Cls.iter_query(...)`**, which yields each decoded reply as it arrives instead of collecting them. It takes the
  same arguments as `query`, and decodes and routes errors the same way: `query` is now `list()` over it, so the two
  can't drift. Paired with a generator handler, neither end builds the whole result set, and the caller gets its
  first answer without waiting out `timeout`. Three things to know:
  - the `session.get` is issued when `iter_query` is called, not on the first `next()`, so `timeout` counts from
    the call; it is the query's total duration, not a gap between replies;
  - `on_error` fires as the caller consumes, so an iterator nobody iterates reports nothing;
  - breaking out stops only the caller's consumption: the serving queryable runs to completion. Zenoh's
    cancellation is client-side only, which is why no cancel parameter is offered.
- **`z.aquery_iter(Cls, ...)`**, its async sibling: `async for row in z.aquery_iter(Cls, id='x'): ...`. The
  blocking loop runs on a worker thread and hands each reply to the event loop, and an exception raised there is
  re-raised in the consumer. Breaking out returns promptly, and the worker stops at its next reply or the timeout.
  There is no `maxsize`: replies are already in flight, so bounding the queue would move the buffering, not push
  back on the server.

### Changed

- **Breaking:** `query_one`, and `aquery_one` with it, returns at the first decoded reply instead of collecting
  every reply until `timeout` and taking the first. The reply it returns is the same, and it still returns `None`
  when nobody answers. But `on_error` now sees only the failures that arrive before that reply, where it saw every
  failure in the window; use `query` for the whole picture.
- `docs/queryable/queryable.md` describes the streaming getters and their three caveats, and the README lists them.
  The README's *Limits* drops "there is no streaming iterator surface" for the caveat that breaking out doesn't stop
  the server.

## 0.3.3 — 2026-09-04

**Raw Zenoh settings from `SessionConfig`, publisher attribution, and a dedupe watermark one bad clock can't
poison.** It also fixes `meta.issued_at`, which was `None` on every real sample.

### Added

- **`SessionConfig.zenoh_config_file` and `zenoh_overrides`**, for the Zenoh settings zeared doesn't model (TLS and
  mTLS, access control, scouting, transport limits). Until now a security posture meant leaving `SessionConfig` for
  a hand-built `zenoh_config=`.
  - `zenoh_config_file` is the path to a JSON5 Zenoh config, and `zenoh_overrides` maps config paths to values,
    such as `{'scouting/multicast/enabled': False}`. Left unset, the config is built as before.
  - `peer`, `client` and `hub` layer the file first, then the config's `mode` with HLC timestamping on, then the
    overrides, so a `mode` in the overrides beats `SessionConfig.mode`. As with `zenoh_config=`, the factory's
    `timestamping=` doesn't apply. Endpoints from `connect`, `listen` and `router`, where set, still go on top.
  - An explicit `zenoh_config=` still wins over both fields.
  - `SessionConfig.from_env` reads the path from `ZEARED_SESSION_ZENOH_CONFIG_FILE`. The overrides have no
    environment form, since a nested mapping doesn't survive a flat string; they go in the TOML or JSON the config
    is loaded from.
- **`meta.origin_zid`**: the Zenoh session id from the sample's HLC timestamp, normally the publisher's, or `None`
  when the sample has none. `source_info` beside it is almost always `None`, since Zenoh fills it only when a
  publisher passes `source_info=`, so this is the one origin signal ordinarily on the wire. It is attribution, not
  authentication: `put` takes a caller-supplied `timestamp=`, so a publisher can claim any zid. Use it for logging
  and forensics, and enforce origin at the router, with Zenoh access control on a TLS certificate.
- **`DEDUPE_MAX_SKEW`** on `Message`: how far ahead of the local clock, in seconds, a sample's HLC may be and still
  advance the dedupe watermark. The default is `300.0`; `None` disables the check. See Changed.

### Changed

- **Breaking:** a subscriber with `DEDUPE = True` no longer deduplicates samples whose HLC is more than
  `DEDUPE_MAX_SKEW` seconds ahead of local time. They are delivered without advancing the watermark, so a repeat of
  one is delivered again, and a warning naming the skew is logged once per key. A publisher with a legitimately
  loose clock wants a higher ceiling; `DEDUPE_MAX_SKEW = None` restores the old behaviour. See Fixed.
- The docs cover the new fields in `docs/config.md` and `docs/meta.md`, and the README's *Limits* adds `origin_zid`
  and the skew ceiling.

### Fixed

- **One far-future timestamp no longer blinds a subscriber.** Dedupe drops a sample whose HLC isn't newer than the
  last one seen on its key, so a single sample stamped far ahead, by a broken clock or a hostile publisher, parked
  the watermark there, and every genuine sample on that key was dropped until restart. A sample past the ceiling is
  still delivered, so a skewed publisher still gets through, but it can't move the watermark.
- **`meta.issued_at` is set on real samples.** It was `None` on every one: the parser expected the HLC's time as 16
  hex digits, and `zenoh.Timestamp` renders it in decimal. It now reads `Timestamp.get_time()`, and parses the
  string form only for a value without that method, taking whichever base gives a date from 2000 to 2100.
- **A subscriber's `on_error` that raises no longer hides the error it was handed.** It escaped into Zenoh's
  callback thread, which kept the subscription alive but logged a bare callback traceback, with the decode, schema
  or callback error behind it nowhere in the log. zeared now catches it and logs both.

## 0.3.2 — 2026-09-02

**Generator handlers for queryables**, replying as they yield, `async def` generators included.

### Added

- **`async def` generator handlers** for `on_query` and `aon_query`. Each yielded instance is replied as it is
  produced, so a handler serving a large result set never holds all of it. Like an `async def` handler, it needs a
  running event loop when it is declared. A sync generator already streamed this way, as an iterable return value,
  and the README and `docs/queryable/queryable.md` now describe both. The getter still collects: `Cls.query` returns
  a list and `z.aquery` wraps the same blocking collect, so streaming saves the server's memory, not the caller's
  wait for a first reply.

### Changed

- **Breaking:** declaring an `async def` generator handler with no running event loop raises `QueryableError`, as
  an `async def` handler always has. It used to declare, and then answer every query with nothing. See Fixed.

### Fixed

- **An `async def` generator handler replied nothing.** `inspect.iscoroutinefunction` is false for one, so it took
  the sync path, where what it returns is neither awaitable nor iterable: the server logged a warning, and the
  getter got no rows and no error.
- **A generator handler that raises part-way is reported.** The raise comes as the stream is advanced, after
  `handler(ctx)` has returned, so it escaped into Zenoh's callback: `on_error` never fired, and the getter took the
  truncated stream for a whole one. Replies already sent still land; the error now goes to the queryable's
  `on_error` as a `QueryableError`, or to the log, and an error reply reaches the getter, whose `on_error` gets a
  `QueryError`. Sync and `async def` generators behave alike.

## 0.3.1 — 2026-09-01

**Sends survive a reconnect.** On a managed session, the first send per class after an automatic reconnect was lost
on the default cached-publisher path.

### Changed

- **A failed send on a cached publisher raises `SessionDeadError`** when the managed session is `RECONNECTING` or
  `DEAD` by then, as the README documents for the reconnect window; it raised a plain `ZearedError`.
  `SessionDeadError` subclasses `ZearedError`, so existing handlers still catch it. On a raw `zenoh.Session`, with
  no state to consult, it stays a `ZearedError`.
- The README and `docs/_reconnect/_reconnect.md` list what a reconnect restores, in order: publisher caches,
  retention queryables, subscribers, user queryables (restored before, but not listed), then presence wills.

### Fixed

- **The first send after a reconnect is delivered.** Cached `zenoh.Publisher` handles stayed bound to the session
  the reconnect had just closed, so with `PUBLISHER = True`, the default, the first `send()` per class and session
  after a successful reconnect raised instead of publishing. A reconnect now drops those handles before restoring
  anything else, since the later steps can run code that publishes (retained-fetch replay callbacks, `on_reconnect`
  hooks), and the next send re-declares. The caches stay registered, so `published_topics()` keeps its history.
- **A failed send on a cached publisher drives reconnect detection.** A cached publisher bypasses the managed
  session, so its failures never reached it, and a publisher-only daemon with `probe_interval=0` never noticed a
  dead session. The failure is now reported to the session, as a `PUBLISHER = False` send's already was.

## 0.3.0 — 2026-08-31

**seared 0.3.** The minor bump follows seared's, and zeared's own code is unchanged. The benchmarks are re-recorded
on seared 0.3.0 and `rusted` 0.2.0, and the docs catch up with current idioms.

### Changed

- **Breaking:** seared 0.2 is no longer accepted, so a project that pins it can't install this release. No zeared
  code changed for seared 0.3.
- **The README's benchmarks answer "pydantic + MQTT, or zeared?"**, as two separate axes: pydantic's compiled codec
  beats pure-Python seared by about 3.6×, and Zenoh beats a broker-mediated MQTT hop by about 2.5×. At default
  settings against MQTT QoS 0, pydantic + MQTT comes out about 1.2× ahead. Against QoS 1, the at-least-once delivery
  most fleets run, zeared is about 2.3× ahead; with `rusted`, about 1.6× against QoS 0 and 4.4× against QoS 1.
  Against marshmallow + Zenoh, zeared is about 89% faster (was 80%) and still a third smaller on the wire.
- **`docs/overview/benchmarks.md` revises its verdict.** It called default zeared against pydantic + MQTT QoS 0 a
  dead heat (0.96×); it is now 0.82×. The ratios are medians of three runs, and a new table gives the run-to-run
  spread of six rows, from 0% to 19%, so anything under about 1.2× reads as comparable. Every number is re-recorded,
  on zeared 0.3.0, seared 0.3.0, `rusted` 0.2.0, `eclipse-zenoh` 1.9.0 and CPython 3.14.3.
- **The README uses current seared idioms**: `default=` and `default_factory=` rather than `missing=`, and
  `str | None` with `default=None` rather than `Optional[str]`. It gives the right Python floor, 3.14 (it said
  3.11), and documents the four `prek` hooks (`ruff check`, `ruff format`, `deptry`, `ty`), installed once per clone
  with `uv run prek install`, and the `bench` extra.
- `docs/overview/architecture.md` describes the source layout: a module that outgrows one file becomes a
  subdirectory, with a primary file named after it, stateless mixins in sibling modules, and an `__init__.py` that
  keeps the import path unchanged. Docstrings that pointed at local working notes now say what they meant.
- **Dependencies:** seared `>=0.3.0,<0.4.0` (was `>=0.2.9,<0.3.0`), and the lockfile resolves seared 0.3.0.

## 0.2.14 — 2026-08-31

**Pydantic over MQTT, or zeared?** A benchmark suite that answers it one axis at a time. Bench and docs only; the
library's code is unchanged.

### Added

- **`suite_stacks`**, the stack comparison, in `python -m bench`: 5,000 messages per row, with pydantic or seared
  as the codec and MQTT (QoS 0 and 1) or Zenoh as the transport, raw and through zeared, with and without `rusted`.
  Every row is JSON on the wire except two labelled msgpack rows, and the Zenoh rows go through a router, a hop
  comparable to the MQTT broker, unless marked `peer`. The bench starts its own `mosquitto` and Zenoh router, and
  skips the suite when `mosquitto` isn't on `PATH`.
- **The answer, in `docs/overview/benchmarks.md`**, read one axis at a time rather than as one headline, which
  would mix codec and transport:
  - codec: pydantic is 3.45× faster than seared over the same raw Zenoh transport, and 1.64× over MQTT at QoS 0;
  - transport: with pydantic as the codec, Zenoh is 3.15× faster than MQTT;
  - zeared's `Message` layer costs 9% over raw seared on Zenoh, a peer topology is 5% faster than a router, and
    `rusted` is 1.73× on the native stack;
  - against pydantic over MQTT, zeared's defaults (msgpack, peer) are a dead heat at QoS 0 (0.96×) and 2.81× ahead
    at QoS 1; with `rusted`, 1.66× and 4.86×.

  Rows are compared on end-to-end rate: `paho`'s `publish()` returns once a message is queued, so an MQTT publish
  rate measures enqueueing, not delivery. The caveats are stated: one payload shape, one machine, loopback only, and
  throughput alone, which is one axis of a stack choice.

### Changed

- **Dependencies:** the `bench` extra adds `pydantic` (`>=2.11,<3.0`) and `paho-mqtt` (`>=2.1,<3.0`) beside
  `marshmallow`, and the lockfile resolves pydantic 2.13.5 and paho-mqtt 2.1.0. None of them is a runtime
  dependency.

## 0.2.13 — 2026-08-31

**The bench as a package**, `python -m bench`, as in seared, recording to a committed artifact, with a suite for the
`rusted` accelerator.

### Added

- **`python -m bench`** replaces the three loose scripts (`bench/bench_wire.py`, `bench_throughput.py` and
  `bench_async.py`) with a shared harness and a suite per question:
  - `suite_wire`, 5,000 messages: what one message costs, and its size on the wire;
  - `suite_rusted`, 5,000 messages: what the accelerator buys;
  - `suite_throughput`, a 5 s window: the sustained sync rate;
  - `suite_async`, a 5 s window: what each combination of sync and async publishing and delivery costs.

  A run prints a table per suite and writes `bench/results.json`, which `docs/overview/benchmarks.md` quotes;
  `--no-write` only prints, and `-n` and `-d` set the message count and the window. The artifact's `Measurement` and
  `Report` are `@z.zeared` classes, so the bench writes its results with zeared itself. A suite whose package isn't
  installed (`marshmallow`, behind the `bench` extra, or `rusted`) skips with a note.
- **An accelerated suite.** `rusted` is declared nowhere, as in seared: where it is installed, zeared's wire path
  picks it up through seared. Against `rusted` 0.1.2 that is about 1.7× on JSON and 1.9× on msgpack end to end, with
  the same wire sizes. The suite skips, giving seared's reason, unless its classes really did accelerate, and every
  other strategy is pinned to `accel=False`, so an installed accelerator can't turn the pure-Python rows into
  compiled numbers.

### Changed

- **The numbers are re-measured**, over a 5 s window rather than 10 s. Against the default sync msgpack path,
  `await asend()` is about 62% slower (was 55%), `alisten` with a sync `send` about 23% (was 12%), an `async def`
  callback through `on_message` about 34% (was 20%), and marshmallow over Zenoh about 52% (was 44%). msgpack is
  still a third smaller on the wire than JSON.
- `docs/overview/benchmarks.md` is rewritten around the suites. `docs/async_.md` points to it, where it pointed to
  a `tests/bench_async.py` that didn't exist.
- `bench/` is left out of the sdist, as seared's is; it was never in the wheel. Its `ruff` waivers narrow to the
  one seared's bench has, `T201`, now that it is a documented and annotated package.
- **Dependencies:** seared `>=0.2.9,<0.3.0`, from `>=0.2.9`, so zeared can't pick up seared 0.3.0 before it has
  been tested against it. The lockfile stays at seared 0.2.9.

### Fixed

- **`zeared.__version__` reports the release.** It stayed at `'0.2.9'` through 0.2.10, 0.2.11 and 0.2.12 while
  `pyproject.toml` moved on, and now reads `'0.2.13'`. The bench reads versions from the installed package metadata
  instead, so a stale constant can't mislabel its results.
- **An `async def` callback to `on_message` type-checks.** `cb` was typed as a sync callable taking one or two
  arguments, though the dispatch layer has always scheduled coroutine functions, so passing one was flagged. It now
  takes all four forms: one or two arguments, sync or `async def`. Behaviour is unchanged.

## 0.2.12 — 2026-08-31

**`deptry`.** Tooling only.

### Added

- **`deptry`** in the dev dependencies and as a `prek` hook, after `ruff` and before `ty`. `marshmallow`, the bench
  comparator behind the `bench` extra, is mapped to its module, so `bench/` stays in the check.

## 0.2.11 — 2026-08-31

**`ruff`**, linting and formatting, as `prek` hooks. The largest churn of the 0.2.x line, almost all of it
mechanical.

### Added

- **`ruff`** in the dev dependencies and as `prek` hooks for checking and then formatting: `select = ["ALL"]`, line
  length 120, target `py314`, google docstrings, single quotes. Two ignores go beyond the house list, each with its
  reason: `TID252`, because the subpackages reach their siblings through parent-relative imports, and `ANN401`,
  because `Any` is the wire boundary type, as in seared. Markdown is left unformatted, since the docs align
  declarations by hand and show JSON in Python blocks.

### Changed

- Source, tests and the benches are reformatted to 120 columns, `__all__` lists are sorted, and
  `try`/`except`/`pass` becomes `contextlib.suppress`. The hand-aligned field blocks in `config.py`, `meta.py` and
  `presence.py` keep their columns, to match the docs. Behaviour is unchanged, but for one exception type, below.
- **`Subscriber` and `Queryable` declare their type parameter in PEP 695 syntax** (`class Subscriber[M: Message]`).
  `Subscriber[MyMsg]` still works, and `with MyMsg.on_message(cb) as sub:` now types `sub` as `Subscriber[MyMsg]`,
  since `__enter__` returns `Self`. `ManagedSession.__enter__` does the same.
- **The module-level async functions are annotated in full:** `asend_batch`, `aunretain`, `afetch_retained`,
  `alisten`, `aquery`, `aquery_one` (returning `Message | None`), `aon_query` (returning a `Queryable`) and
  `abatch`, along with `config=` on `apeer` and `aclient`, `router=` on `aclient`, and `aopen`'s `cfg`.
- **A payload that doesn't decode to a map** raises `TypeError` inside zeared, not `ValueError`. It still reaches
  `on_error` wrapped in `DecodeError`, `QueryError` or `RetainedFetchError`, with the same message; only code that
  inspects the wrapper's `__cause__` sees the difference.

## 0.2.10 — 2026-08-31

**A `ty` gate**, the typing fixes it called for, and docs brought back in line with the code.

### Added

- **`ty`**, as a `prek` hook: `.pre-commit-config.yaml` runs `uv run ty check` on commits that touch Python.
  Install it once per clone with `uv run prek install`. `ty` skips `tests` and the three bench scripts, which import
  a comparator a default dev sync doesn't install.
- **A `bench` extra** with that comparator, `marshmallow` (`>=3.26,<5.0`), for the benches alone, never a runtime
  dependency.

### Changed

- **More functions are typed to take a `ManagedSession`:** `clear_retention_cache`, `clear_publisher_cache`,
  `clear_presence_state` and `clear_observer` were typed for a raw `zenoh.Session` only, though they took either.
  `release` already said so.
- **`zeared.doc` takes seared classes**, as `seared.doc` does: `document`, `introspect` and `render_one` take
  `type[Seared]`, `introspect_message` and `render_message` take `type[Message]`, and `is_message_class` is a
  `TypeGuard`, so a branch on it narrows. `emit_ts` and `render_interface` are typed the same way.
- **Docs that had drifted** now say what the code does:
  - `docs/retention.md`: retained-fetch dedupe compares timestamps, not payloads. A subscription keeps the highest
    HLC timestamp it has seen per concrete topic and drops any sample at or below it, live samples included, while a
    sample with no timestamp always passes, which is how synthesised wills get through. With HLC timestamping off,
    dedupe does nothing (`z.peer()` and `z.client()` turn it on by default), and `dedupe=False` opts out per
    subscription;
  - the same page no longer says there is no TTL, which `RETENTION_TTL` had made untrue. The real limit is that
    retained values live in the publishing process and die with it;
  - `docs/presence/presence.md`: a dead peer's retained values vanish with it, so a subscriber joining later gets
    nothing for those topics. The page had said late subscribers still saw the last value, and listed a subscriber
    watchdog as missing, which zeared has;
  - `docs/config.md` and `docs/meta.md` show the declarations as the code now writes them (see Fixed), and
    `config.md` no longer shows the deprecated `missing=`.
- **Dependencies:** seared `>=0.2.9` (was `>=0.2.3`), the release that declares `format=` on `Seared.dump` and
  `load`, as zeared calls them, and types `seared.doc` for seared classes. The lockfile moves from seared 0.2.3 to
  0.2.9. `ty` and `prek` join the dev dependencies.

### Fixed

- **`SessionConfig(mode=…)` and `ZenohMeta(key_expr=…)` type-check.** `SessionConfig`'s `router` and
  `max_attempts`, and `ZenohMeta`'s `timestamp`, `issued_at`, `encoding`, `source_info`, `attachment` and `schema`,
  had no default, which under PEP 681 reads as a required argument, so any construction leaving them out was
  flagged. They are now `X | None` with `default=None`. At runtime they already defaulted to `None`.

## 0.2.9 — 2026-08-30

**Telling a replay from a live publish.** `meta.origin` says how each sample reached a subscriber, a subscription
can skip the retained replay entirely, and the watchdog now counts live traffic only.

### Added

- **`meta.origin`**, the sample's provenance, as the new `z.Origin` `StrEnum`:
  - `Origin.LIVE` (`'live'`): a publish, delivered by the live subscriber;
  - `Origin.REPLAY` (`'replay'`): a cached value from a retained fetch, at subscribe time or after a reconnect;
  - `Origin.WILL` (`'will'`): a will the presence observer synthesised when a peer's liveliness token went away.

  It is decided by the local delivery path, never read from or written to the wire, so nodes on other versions are
  unaffected. A 2-arg `on_message` callback sees it, and so does `on_remove`; `ZenohMeta` defaults it to `LIVE`. It
  is the reliable way to recognise a will: `meta.issued_at is None` is also true of any sample from a session
  without timestamping.
- **`retained_fetch=False`** on `on_message`, for a live-only subscription to a `RETAINED` class: no cached values
  replay at subscribe time or after a reconnect, and the blocking `session.get` each fetch issues is skipped. The
  default is `True`, as before.
- **`alisten(meta=True)`**, on `Cls.alisten` and `z.alisten`, yields `(msg, meta)` tuples carrying the same
  `ZenohMeta` a 2-arg callback gets, so the async-iterator path can read captures, the schema and `origin`.

### Changed

- **Breaking:** only live samples feed a subscriber's watchdog (`expected_interval=`). A replay no longer starts an
  optimistic watchdog or counts as the first message in a `startup_grace` window, and a will no longer resets the
  quiet timer or fires `on_active`. See Fixed.
- The README and the `meta`, `retention`, `subscriber`, `presence`, `watchdog` and `async_` docs cover `origin`.
  They also state that the subscribe-time fetch has drained by the time `on_message` returns, so everything after it
  is live or marked, and that dedupe is separate from `origin`: a replay that survives dedupe still arrives as
  `REPLAY`.

### Fixed

- **The watchdog measures the live stream.** Replays and wills pinged it like live publishes. A subscribe-time
  replay gave it a cadence from cached data, so an optimistic watchdog started, and went on to fire `on_quiet`, for
  a producer that never published while the subscriber listened. A will, which arrives because the producer has
  died, reset the quiet timer, and on a subscription already quiet fired `on_active`.

## 0.2.8 — 2026-08-29

**Python 3.14**, in step with seared 0.2.3.

### Changed

- **Breaking:** the Python floor rises from 3.11 to 3.14 (`requires-python = ">=3.14,<4.0"`). No source changes.
- **Dependencies:** seared `>=0.2.3` (was `>=0.2.2`). The lockfile resolves seared 0.2.3 and drops the msgpack
  wheels only older Pythons needed.

## 0.2.7 — 2026-08-29

**A relay hub** for nodes that can't reach each other directly: an in-process Zenoh router, and a daemon around it.

### Added

- **`z.hub()`** opens a router-mode session that relays pub/sub, queries and liveliness, so retention, queryables,
  presence and wills all work through it. Two nodes behind NAT, both outbound-only, each connect out to it with
  `z.client(router=...)`. The routing runs in Zenoh's core, and no `zenohd` binary is needed.
  - `listen=` defaults to `['tcp/0.0.0.0:7447']`, and `connect=` links it to other hubs, for a mesh of several;
  - `config=` supplies the endpoints and the retry knobs (`retry`, `initial_backoff`, `max_backoff`,
    `max_attempts`), and explicit arguments override it; HLC timestamping is on unless `timestamping=False`;
  - `zenoh_config=` replaces the base config, as on `peer` and `client`: zeared then sets neither router mode nor
    timestamping, so the config must, and its listen endpoints give way to `listen=` or the default;
  - it returns a raw `zenoh.Session`, not a `ManagedSession`: a hub holds no zeared resources to supervise, so
    there is no `auto_reconnect`.
- **`Mode.ROUTER`** (`'router'`): `z.open(cfg)` opens a `SessionConfig` with that mode as a hub.
- **`python -m zeared.hubd`**, the hub as a daemon, to run as a systemd service. `-l/--listen` and `-c/--connect`
  each take an endpoint and repeat; `--config FILE` loads a JSON5 Zenoh config as `zenoh_config=`, for TLS and
  access control; and there are `--no-timestamping` and `--log-level`. It routes samples without decoding them and
  holds no message state, so it needs no message classes. It logs through stdlib `logging` and stops cleanly on
  SIGINT or SIGTERM.
- **`zeared.hubd.run(...)`**, the daemon's loop as a call, for embedding and tests: it opens the hub, calls
  `on_ready(session)` once it is up, and blocks until a `stop` event is set (or, without one, until a signal).
- **Docs:** the README's *NAT traversal: the relay hub*, and `docs/hubd.md`, with a sample systemd unit. Both warn
  that an open hub relays for anyone who can reach its port, so a public one needs TLS and access control, and that
  a single hub is a single point of failure.

## 0.2.6 — 2026-08-29

**TypeScript types** for browser consumers, generated from message classes.

### Added

- **`python -m zeared.doc.typescript <module-or-package> [-o out.ts] [--check]`** writes one `.ts` module of
  `export interface` and `export type` declarations: every seared or zeared class under the target, and every class
  they reference, `REQUEST` payloads included. Without `-o` it prints to stdout. `--check`, which needs `-o`, exits
  non-zero when the file is missing or stale, for CI. Declarations are sorted by module and name, so the output
  doesn't depend on discovery order. `zeared.doc` exports `generate_ts(target)`, which returns the source as a
  string, and `emit_ts(classes)`, `render_interface` and `ts_type`.
- **The types describe the JSON a bridge hands the browser** after decoding a message. zeared ships only the types;
  the mesh-to-websocket bridge, with its auth and transport, is the consumer's to build. The mapping:
  - keys are wire keys (`data_key` when set), and `dump=False` fields are left out;
  - a template slot that is also a declared field is a member; a capture-only slot is not;
  - `Bool`, `Int`, `Float` and `Str` become `boolean`, `number`, `number` and `string`; `UUID`, `Path` and the date
    and time types become `string`;
  - `Decimal` is a `string`, or a `number` with `as_number=True`; `Bytes` is a base64 `string`; `Dict` is
    `Record<string, unknown>`; `NDArray` and the frames are `unknown`, for the bridge to decode;
  - an `Enum` becomes a type alias over its values as literals (`0 | 1`, `"fast" | "slow"`);
  - `T(Nested)` refers to the nested class's interface, a `Union` becomes a union discriminated on its tag (flat,
    or with the variant under its payload key), and a `Tuple` becomes a TS tuple;
  - `many` gives `T[]` and `keyed` gives `Record<string, T>`;
  - a `required` field has no `?` and every other field does; one with no default, or a `None` default, is also
    `| null`.

  A message's interface opens with a JSDoc block: its docstring's first line, then `@topic`, `@extraTopic`,
  `@schemaVersion` and `@encoding`. A field's `doc=` becomes its JSDoc.
- The README's *TypeScript types for browser consumers* section.

## 0.2.5 — 2026-08-28

**Schema docs for messages**, built on seared's generator, with each message's wire contract on its page.

### Added

- **`zeared.doc`**, a wire-aware layer over `seared.doc`:
  - `document(cls)` renders one class's Markdown page. A message gets the wire-aware page; any other seared class
    (a config dataclass, a `REQUEST` payload) gets seared's;
  - `introspect(cls)` returns a `MessageDoc` for a message, or seared's `SchemaDoc` for anything else;
  - `build_docs(target)` returns `{path: markdown}` for every public class in a module or package, with seared's
    `index.md`;
  - the pieces are public too: `introspect_message`, `is_message_class`, `render_message`, `render_topic`,
    `render_wire` and `render_one`, with `MessageDoc` and `SlotDoc`.
- **A message's page** shows, in order:
  - its topic's second segment, when literal, as a category, and its `SCHEMA`;
  - the `TOPIC` template and any `EXTRA_TOPICS`, with a table of which slots are also payload fields;
  - a line for `ENCODING`, `RETAINED`, `LIVELINESS`, `DEDUPE` and `RETENTION_TTL` (`∞` when unset), linking the
    `REQUEST` payload's page;
  - seared's sections for fields, enums and variants.
- **The `zeared-doc` command**, or `python -m zeared.doc <module-or-package> [-o docs] [--check]`, as `seared-doc`
  is for seared. `--check` fails when the docs are stale, for CI.

### Changed

- **Dependencies:** seared `>=0.2.2` (was `>=0.2.1`), for `seared.doc`. The lockfile resolves seared 0.2.2.

## 0.2.4 — 2026-08-27

**Wills keep their `SCHEMA`**, as retained snapshots did in 0.2.3.

### Changed

- **Wire change:** the will envelope zeared keeps on `__zeared/will/<zid>/<slug>` gains a `schema` key, holding the
  registering class's `SCHEMA`. It is left out when the class sets none, so those envelopes are unchanged. Older
  nodes ignore the key, and their envelopes, which lack it, still decode. See Fixed.

### Fixed

- **A subscriber whose class sets `SCHEMA` receives wills.** A will is synthesised on each subscriber when the
  producer's session disappears, and the synthesised sample carried no attachment, so a class with a `SCHEMA` read
  its schema as `None`, treated it as a mismatch and dropped the will, reporting a `SchemaMismatchError` the first
  time. The envelope now carries the schema, through re-registration after a reconnect too, and the synthesised
  sample is stamped with the attachment a live publish carries, so `meta.schema` reads it as well. A will from a
  class with a different `SCHEMA` is still dropped. The fix needs 0.2.4 on both the node registering the will and
  the node receiving it. `docs/presence/presence.md` documents the new field.

## 0.2.3 — 2026-08-27

**Retained snapshots keep their `SCHEMA`.**

### Changed

- **Wire change:** a retention queryable's replies carry the `SCHEMA` attachment of the publish each one caches, as
  the live sample did; before, they carried none. A subscriber's `meta.schema` and `meta.attachment` for a snapshot
  now read what they read for the live sample. See Fixed.

### Fixed

- **A late subscriber receives retained snapshots of a class with a `SCHEMA`.** The retention cache kept each
  payload and its encoding but not its attachment, so its replies arrived with no schema. A subscriber whose class
  sets `SCHEMA` treated that as a mismatch and dropped every snapshot, reporting a `SchemaMismatchError` the first
  time, both at subscribe time and on the re-fetch after a reconnect. Live samples were unaffected, and so was
  `fetch_retained`, which doesn't check the schema. The cache now keeps the attachment, from `send` and from a
  `z.batch()` flush alike, and each reply re-stamps it. The fix is on the publishing side: replies from a 0.2.3
  publisher pass the check on any subscriber, and an older publisher's still don't. `docs/retention.md` describes
  the cached value.

## 0.2.2 — 2026-08-26

**The version number, corrected.** 0.2.1 was stamped `0.3.0` by mistake; this release returns the line to 0.2.x. The
code is unchanged from 0.2.1.

### Changed

- `zeared.__version__` and the package metadata read `0.2.2`. Under 0.2.1 they read `0.3.0`, so an install of that
  commit reported itself as newer than the 0.2.x releases after it.

## 0.2.1 — 2026-08-26

**Removals, observable.** `on_message`'s callback never sees a DELETE sample, so a subscriber couldn't tell that a
peer had called `unretain()`. This release adds a feed of removals and a snapshot to reconcile against. It shipped
stamped `0.3.0` in its package metadata and `zeared.__version__`; 0.2.2 corrects the number.

### Added

- **`on_remove=` on `on_message`:** the tombstone feed. It fires on each DELETE sample on a matching key (a peer's
  `unretain()`, or any `session.delete`) with a `ZenohMeta` whose `captures` hold the removed key's slots, as raw
  strings. A tombstone has no payload, so there is no typed instance to hand over. Without `on_remove`, DELETEs are
  still dropped silently.
  - An `async def` callback is scheduled on the loop running at subscribe time, as an async `cb` is.
  - It survives reconnect.
  - If it raises, `on_error` receives a `CallbackError` with empty payload bytes.
- **`Cls.coerce_captures(captures)`** runs raw captures through their declared fields, strictly, so an `{id}` slot
  bound to `z.Int` comes back as an `int`. Capture-only slots pass through as strings.
- **`Cls.fetch_retained()`**, a one-shot typed snapshot of a retained class's current set, with
  `await z.afetch_retained(Cls)` as its async form. It issues one `session.get` per declared template and returns
  the decoded instances. It is the reliable reconcile path: a subscriber offline when a DELETE lands never sees it,
  since nothing replays a tombstone, so `on_remove` gives latency and a periodic `fetch_retained` gives correctness.
  - It raises `TopicError` on a class without `RETAINED = True`.
  - Decode failures go to `on_error=` as `RetainedFetchError`, or to the log. Error replies are skipped, and a
    failed get is logged and skipped.
  - It doesn't deduplicate: where several publishers retain the same key, dedupe at the call site.

### Changed

- The README documents both in a new *Removals* section, and its *Limits* gains two entries: removals aren't
  durably delivered, and `on_remove` delivers the removed key, not the removed value.
  `docs/subscriber/subscriber.md` follows.

## 0.2.0 — 2026-08-25

**Queries and queryables:** request/response for `@z.zeared` classes. Where a `RETAINED` class serves a cached
value, `Cls.on_query` serves a computed one, on the class's own topic templates, with its fields as the reply body.

### Added

- **Serving, `Cls.on_query(handler)`:** declares a queryable on each of the class's templates (`TOPIC` and
  `EXTRA_TOPICS`) and returns a `z.Queryable` handle, closed with `.close()` or as a context manager. The handler
  takes a `z.QueryContext` and either returns an instance, an iterable of them or `None`, each instance sent as a
  reply, or answers itself through `ctx.reply`, `ctx.reply_err` and `ctx.reply_del`, for several replies, streaming
  or errors.
  - `QueryContext` exposes the request without Zenoh types: `key_expr`, `selector`, `parameters` and their parsed
    `params` (`{str: str}`), the template `captures` (empty when a wildcard query key can't bind them), and
    `request`.
  - `ctx.reply(instance)` gives the reply the wire shape of a `send()`: a key rendered on the template the query
    matched, the slot fields stripped from the body, the instance's encoding and the class's `SCHEMA` attachment.
  - A handler that raises goes to `on_error=` as a `QueryableError`, or to the log, and the getter receives an
    error reply carrying the exception's message. A failed reply goes to `on_error=` the same way. Nothing
    propagates into Zenoh.
  - An `async def` handler needs an event loop running when `on_query` is called (it raises `QueryableError`
    otherwise), and runs on that loop; `await z.aon_query(Cls, handler)` registers one from async code. The query
    stays open until the coroutine resolves.
  - `on_query` on a `RETAINED = True` class raises `TopicError`: retention already serves a queryable on the same
    keys, and the two would answer one get with competing replies.
- **Getting, `Cls.query(**slots)`:** issues the get and returns the decoded replies as a list of instances.
  `Cls.query_one(...)` returns the first in arrival order, or `None`. Both block up to `timeout=`.
  - Omitted slots widen to `*`, or `**` for a trailing `{name**}` slot, so `query()` with none asks the whole
    template; a `*` inside a value makes a partial wildcard (`epc='E280*'`). `Template.render_selector` builds the
    key.
  - `params=` appends `?k=v` to the selector. `target=` and `consolidation=` pass through to Zenoh.
  - **No consolidation by default.** A query is a fan-out in which any queryable may reply more than once, and
    Zenoh's default would collapse replies that share a key. Pass `consolidation=` to deduplicate.
  - Only decoded OK replies are returned. Error replies and replies that fail to decode go to `on_error=`, or to
    the log, as `QueryError`, and the query keeps collecting.
  - `z.aquery` and `z.aquery_one` run the get on a worker thread.
- **Typed requests:** with `REQUEST = SomeClass` on the class, `query(request=SomeClass(...))` sends the instance
  as the query payload, and the handler's `ctx.request` is a decoded `SomeClass`. Without `REQUEST`, `ctx.request`
  is the raw payload bytes, or `None`. A request that fails to decode is logged and left as bytes.
- **Lifecycle:**
  - a managed session re-declares its queryables after a reconnect, after subscribers and before wills, unless
    `on_query(..., auto_reconnect=False)`; one that fails to re-declare is logged and closed;
  - `z.release()` closes a session's queryables just after its subscribers, and `z.release_all()` also reaches
    sessions that hold only queryables;
  - `z.clear_queryable_cache()` closes every queryable, or with `session=` one session's.
- **Errors:** `z.QueryableError`, raised when a queryable can't be declared and passed to `on_error=` when serving
  fails, and `z.QueryError`, per reply on the get side, a subclass of `SubscriberError`.
- **Docs:** `docs/queryable/queryable.md`, a README section and a note in `docs/overview/architecture.md`.

### Changed

- `SessionConfig` and `ZenohMeta` declare their defaults with `default=` and `default_factory=`, not seared's
  deprecated `missing=`, so importing zeared no longer triggers seared's `DeprecationWarning`. The defaults are the
  same.
- The README's *Limits* drops "no queries or queryables" for four entries about them: a queryable computes and
  doesn't cache, `query()` blocks up to its timeout, an async handler holds its query until it resolves, and
  `params=` maps `str` to `str`.

## 0.1.1 — 2026-08-25

**Typing support**: a `py.typed` marker, and seared 0.2.1.

### Added

- **A `py.typed` marker**, so type checkers read zeared's inline annotations instead of treating the package as
  untyped.

### Changed

- **Dependencies:** seared `>=0.2.1` (was `>=0.2.0`), and the lockfile moves to seared 0.2.1. Its PEP 681 support
  reaches message classes, since `@z.zeared` is seared's decorator under another name: type checkers see each field
  as a dataclass field, so constructors and attributes are typed, and fields take `default=` and `default_factory=`,
  the names checkers use to tell required from optional. zeared's own `SessionConfig` and `ZenohMeta` still declare
  defaults with `missing=`, which seared 0.2.1 deprecates, so importing zeared emits `DeprecationWarning`s; default
  warning filters hide them.

## 0.1.0 — 2026-04-26

**Typed Zenoh pub/sub, built on seared**, and the first release. Declare a message class once, with a format-string
topic, and it publishes, subscribes and routes itself. The examples here use `import zeared as z`, as the README
does.

### Added

- **Message classes:** subclass `z.Message`, decorate it with `@z.zeared`, and declare seared fields and a `TOPIC`.
  `msg.send()` publishes. `Cls.on_message(cb)` subscribes to every template the class declares and returns a
  `Subscriber`, which closes with `close()` or as a context manager. The callback's arity is read once: `cb(msg)`,
  or `cb(msg, meta)` for the sample's metadata too. An `async def` callback is scheduled on the event loop running
  when it subscribed. seared's field types and errors import from `zeared` too, with the decorator and base class
  renamed `z.zeared` and `z.Zeared` (they are `seared.seared` and `seared.Seared`).
- **Topic templates**, parsed once per class:
  - `{name}` captures one segment, at any position, and subscribers listen on the derived wildcard:
    `robot/{id}/telemetry` becomes `robot/*/telemetry`;
  - `{name**}` captures a slash-containing tail and can still be published. It must be the last segment, and there
    can be one per template; an empty value raises `TopicError`, and a field bound to it must be a plain `z.Str`;
  - an anonymous trailing `**` matches one or more segments, and makes its template subscribe-only;
  - format specs and conversions (`{x:03d}`, `{x!r}`) raise `TopicError`, as does any template whose first segment
    is `__zeared`, which zeared reserves for its own keys. The bare catch-alls `__zeared/**` and `**` are allowed;
  - a slot named after a declared field is rendered from the instance on publish, left out of the payload, and
    decoded from the key back onto the instance on receive. Any other slot is capture-only: its raw string lands in
    `meta.captures`, and a template with one can't be published on;
  - `EXTRA_TOPICS` lists further templates the class receives from, each with its own slots. `TOPIC` stays the
    publish default; `send(topic=…)` picks another declared template, and any other string raises `TopicError`.
- **The wire format:** the payload is a map of the fields not carried in the key, in msgpack, declared to Zenoh as
  `application/msgpack`. `ENCODING = 'json'` switches a class to JSON, and `z.debug = True` forces JSON everywhere.
  Under msgpack, zeared passes `format='msgpack'` to seared, so `Bytes` and `NDArray` go as native binary rather
  than base64. A subscriber decodes by the encoding each sample declares, so classes with different settings
  interoperate.
- **`SCHEMA` stamping:** a class's `SCHEMA`, an opaque string, rides on each publish as a msgpack attachment,
  `{'schema': …}`. A subscriber with its own `SCHEMA` drops samples whose stamp differs or is missing. The first
  from each sender and stamp goes to `on_error` as `SchemaMismatchError`, and the rest drop silently.
- **`z.ZenohMeta`**, a seared class with no Zenoh types in it: `key_expr`, the raw HLC `timestamp`, `issued_at`
  (parsed from it, in UTC), `encoding`, `source_info`, `attachment`, `schema` and `captures`.
- **A watchdog per subscription:** `on_message(…, expected_interval=N, on_quiet=…, on_active=…)` fires `on_quiet`
  when the stream goes silent for `N` seconds and `on_active` when it resumes. It waits for the first message unless
  `startup_grace=` is given. Its callbacks, sync or async, run on a thread of their own.
- **Retained messages:** with `RETAINED = True`, each publish updates a cache of the last payload per concrete key,
  which a queryable on each template's wildcard serves, declared on the first retained publish. A subscriber on a
  retained class queries for those values when it subscribes.
  - `send(retain=False)` publishes live only; `retain=True` on a class that isn't retained raises `TopicError`;
  - `msg.unretain()`, or `Cls.unretain(**slots)`, drops the cached value and publishes a Zenoh `DELETE`, which
    subscribers skip;
  - `RETENTION_TTL = N`, or `retention_ttl=N` on a factory with `auto_reconnect=True`, expires entries after `N`
    seconds, checked when a query arrives;
  - `DEDUPE = True`, the default, drops any sample whose HLC timestamp is no newer than the last one seen on its
    key, so a retained reply doesn't repeat a live sample. `on_message(…, dedupe=False)` opts one subscriber out.
- **Last wills:** with `LIVELINESS = True`, `msg.register_will()` stages a message for subscribers to receive when
  the session goes away, by graceful close or crash alike. The session holds one liveliness token, at
  `__zeared/alive/<zid>`, and each will is served from `__zeared/will/<zid>/<slug>`; registering again for the same
  class and key replaces the will. When the token disappears, each subscriber on a `LIVELINESS` class whose
  templates match synthesises the will and delivers it as an ordinary message. Nodes that don't subscribe never see
  it, and registering a will doesn't touch retained state.
- **A publisher cache:** `PUBLISHER = True`, the default, keeps a `zenoh.Publisher` per concrete key, up to 256 per
  class and session; an int sets the cap, and `False` sends with `session.put`. Past the cap, sends fall back to
  `put` with a one-time warning.
- **Batching:** `with z.batch():` holds sends until the outermost block exits, and discards them all if it raises.
  `Cls.send_batch(items)` sends a list of one class. The buffer lives in a `ContextVar`, so each asyncio task has
  its own.
- **Sessions:** `z.peer()`, `z.client(router)` and `z.open(config)`. A call resolves its session from a `session=`
  argument, then the innermost `with z.session(other):` block on its thread, then the default set by
  `z.session = sess`, and raises `NoSessionError` if there is none.
  - The factories enable HLC timestamping, which dedupe needs, in the Zenoh config they build (`timestamping=False`
    opts out), but not in one passed as `zenoh_config=`.
  - `retry=True` retries the open with exponential backoff (`initial_backoff`, `max_backoff`, `max_attempts`).
  - `z.SessionConfig` is a seared class for the whole spec (`mode`, a `z.Mode`; `router`, `connect` and `listen`;
    the retry settings), with `replace`, `with_retry`, `with_connect`, `with_listen` and `set_router` builders and
    `from_env`, which reads `ZEARED_SESSION_*` variables. Explicit arguments override `config=`.
- **Auto-reconnect:** `auto_reconnect=True` returns a `ManagedSession`, which stands in for the `zenoh.Session` and
  adds `raw()`, `state` and `on_reconnect(cb)`. A probe every `probe_interval` seconds (10 by default) and failed
  sends detect a dead session, which is reopened with backoff: subscribers and retention queryables are redeclared,
  retained values fetched again, and wills registered again under the new zid. Until then, and for good if reopening
  fails, operations raise `SessionDeadError`. `on_message(…, auto_reconnect=False)` keeps one subscription on the
  original session.
- **One-call shutdown:** `z.release(session=sess)` closes subscribers, publishers, retention queryables and the
  presence observer, removes the liveliness token so peers' wills fire, then closes the session; a second call does
  nothing. `z.release_all()` does it for every session zeared knows of. A `ManagedSession` used as a context manager
  releases on exit, and `z.apeer()`, `z.aclient()` and `z.aopen()` are async context managers that open off the
  event loop and release on exit. The `clear_*` functions do each step alone.
- **An async surface:** `asend`, `asend_batch`, `aunretain` and `aregister_will` on messages,
  `async for msg in Cls.alisten()`, and `async with z.abatch()`. The first three and `alisten` are module functions
  too, taking the message or class. Zenoh's Python bindings are synchronous, so these run the sync calls through
  `asyncio.to_thread`, and `alisten` feeds an `asyncio.Queue`.
- **Introspection:** `Cls.published_topics()` returns every concrete key the class has published on in this
  process, and `z.published_topics()` maps each class and session to its keys.
- **Errors**, all under `ZearedError`: `NoSessionError`, `TopicError`, `SessionDeadError`, `SubscriptionError`
  (declaring a subscriber failed) and `SubscriberError`. Under the last, `DecodeError` (with `SchemaMismatchError`
  under it), `CallbackError` and `RetainedFetchError` cover failures while delivering a sample: they go to
  `on_error(exc, raw_bytes)` if given and are logged otherwise, with the original as `__cause__`, and the
  subscription carries on.
- **Benchmarks:** three scripts under `bench/`. Against marshmallow over the same Zenoh session, the default
  msgpack path with cached publishers publishes about 1.8× as fast (7,064 messages a second against 3,950), and is a
  third smaller on the wire (533 bytes against 796).
- **Docs:** the README, and a `docs/` tree with a page per module and overviews of the architecture, the benchmarks
  and the session lifecycle.
- **Packaging:** built with `hatchling`, for Python 3.11 and up, depending on `eclipse-zenoh` (1.9.0 and up),
  `msgpack` (1.1.2 and up) and seared (0.2.0 and up, from GitHub). It installs from git. The tests mirror the
  source, a file per module, and run real in-process Zenoh sessions.
