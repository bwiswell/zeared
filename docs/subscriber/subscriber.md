# `subscriber.py`

`Subscriber` is the handle returned by `Message.on_message(...)`. It wraps
one `zenoh.Subscriber` per declared template (plus the presence feed's
liveliness subscribers when `on_online` / `on_offline` are given) and
handles the decode + dispatch loop.

The class is generic in the message type, declared with PEP 695 syntax
(`class Subscriber[M: Message]`) —
`Cls.on_message(cb)` is typed as returning `Subscriber[Cls]`, giving IDE
+ type-checker visibility into which kind of subscriber a given handle
represents. Runtime is identical with or without the parameter; bare
`from zeared import Subscriber` continues to work.

## Lifecycle

```python
sub = Telemetry.on_message(cb)
...
sub.close()                     # idempotent
```

Or as a context manager:

```python
with Telemetry.on_message(cb) as sub:
    ...
# closed automatically
```

## Arity-inspected dispatch

`_wants_meta(cb)` inspects `cb`'s signature once at subscribe time. If `cb`
accepts two or more positional parameters — or a `*args` catch-all — the
subscriber invokes it with `(msg, meta)`; otherwise with `(msg,)`.

Bound methods are handled correctly (`self` is excluded from the count).

## Coroutine callback scheduling

If `cb` is an `async def` function, `Subscriber._declare` captures the
running event loop at subscribe time and wraps the callback in a sync
shim that schedules the coroutine via `asyncio.run_coroutine_threadsafe`
on each sample. If there's no running loop, `SubscriptionError` is
raised — subscribe from inside a running loop, or pass a sync callback.

## Auto-reconnect interaction

When the resolved session is a `ManagedSession`
(see [`_managed_session.md`](../_managed_session/_managed_session.md)), the reconnect
machinery walks the per-session subscriber registry on every successful
swap and calls `Subscriber._redeclare(new_raw, managed)` on each. The
user's `Subscriber` handle stays valid across the boundary. `_redeclare`:

1. clears the schema-mismatch warn-once cache (peer zids may have changed);
2. rebuilds the `_zenoh_subs` tuple against the new raw, on the same
   dispatch closure;
3. re-runs the retained fetch (`RETAINED` classes, unless
   `retained_fetch=False`), dedupe-safe;
4. re-declares the presence watch when there is one: `_watch_presence`
   declares fresh liveliness subscribers (and their `history=True`
   keepers) and replays the keys online now as `REPLAY`.

A redeclare that fails is logged and closes the handle.

### The re-attach walk (0.4.0)

A hub restart can drop a managed session's router link without killing
the session, so nothing is redeclared: the Zenoh subscribers survive the
outage. When the link comes back, the session's re-attach walk calls
`_replay_retained(raw)` on every subscriber whose `_wants_replay()`
holds (open, `RETAINED`, `retained_fetch=True` and
`auto_reconnect=True`). That re-runs the retained fetch through the same
dispatch, as `REPLAY` and dedupe-safe. See
[`_reconnect.md`](../_reconnect/_reconnect.md#link-re-attach-040).

### Opting out

```python
sub = MyMessage.on_message(cb, auto_reconnect=False)
```

A subscriber opted out is skipped by both the restoration walk and the
re-attach walk; after a reconnect its `_zenoh_subs` stay bound to the
dead raw. Power users who need to manage subscriber lifecycle explicitly
across reconnects keep that control. `retained_fetch=False` keeps the
redeclare but skips every replay.

## Multi-topic subscriptions

When a class declares `EXTRA_TOPICS`, `Subscriber._declare` registers one
`zenoh.Subscriber` per wildcard — all feeding the same handler.
`Subscriber.close()` undeclares all of them; partial-declaration failures
during setup roll back any subs already created.

## Retained-value fetch on `RETAINED` classes

When `msg_cls.RETAINED` is `True`, `_declare` issues a `session.get()` per
declared template wildcard after bringing up the live subscribers. Reply
samples — cached payloads from peer `_RetentionCache` queryables — flow
through the same dispatch function used for live samples, marked
`meta.origin = Origin.REPLAY` so 2-arg callbacks can distinguish cached
state from live traffic (see [`../meta.md`](../meta.md)). Decode failures
route through the unified `on_error` hook. See [`retention.md`](../retention.md).

A publisher's republish after its link comes back (0.4.0) arrives
through the live subscriber, but its attachment carries the `replay`
marker, so the dispatch delivers it as `REPLAY` too.

The fetch drains synchronously inside `_declare`, so by the time
`on_message` returns every subscribe-time replay has been delivered (or
dedupe-suppressed) — the initial-sync boundary. Live samples that
interleave during the fetch are labeled `LIVE`.

Per-subscription opt-out — a **live-only** subscriber:

```python
sub = Telemetry.on_message(cb, retained_fetch=False)
```

No cached values replay at subscribe time, after a reconnect or on the
re-attach walk, and the blocking `session.get()` those fetches issue is
skipped entirely. The flag is stored on the handle so `_redeclare` and
the walk honour it too.

`Cls.fetch_retained()` exposes the same `session.get()` sweep as a
standalone, one-shot call that **returns** the decoded instances instead of
dispatching them (backed by `_collect_retained`). It's the reliable
reconcile path — poll it periodically and reconcile against the returned
set, since `on_remove` (below) can miss a DELETE that lands while a
subscriber is offline. Requires `RETAINED = True`; async via
`z.afetch_retained(Cls)`.

**`timeout=`** (seconds, 0.4.4) bounds the whole fetch. Every template's
get is issued before any is read, each with the timeout, so a class with
`EXTRA_TOPICS` fetches in the time of its slowest get rather than their
sum, and a queryable that doesn't answer costs one timeout, not one per
template. `None` keeps Zenoh's default query timeout (10 s, unless the
session's config sets another). The result keeps template order, then
arrival.

**Its error replies reach `on_error`** (0.4.4), as `query`'s do: a
`RetainedFetchError` with no `__cause__`, the reply's payload as the raw
bytes, or a WARNING log without an `on_error`. A reply that won't decode
comes the same way, with its cause. One error reply is Zenoh's own,
payload `b'Timeout'`, for a queryable that didn't answer in time. The
returned set is then partial, so **a reconcile should skip its removals
for a fetch whose `on_error` heard one**. Before 0.4.4 error replies were
dropped, and a partial set looked whole. The subscribe-time replay still
drops them: its `on_error` is the subscriber's, for samples.

## DELETE samples (tombstones)

The dispatch closure detects `sample.kind == zenoh.SampleKind.DELETE` before
the decode path. Behaviour depends on whether the subscriber registered an
`on_remove` callback:

- **No `on_remove` (default):** the DELETE is dropped silently — no callback
  fires. This is how `unretain()` propagates "forget this key" without
  surfacing a nil message to `cb`.
- **`on_remove=` registered:** the removal is delivered as a `ZenohMeta`
  (via `_dispatch_remove`). `meta.captures` carries the removed key's
  template slots (raw strings — `Cls.coerce_captures(meta.captures)` runs
  them through declared fields). A tombstone has no payload, so no typed
  instance is built. `on_remove` is baked into the same dispatch closure as
  `cb`, so it survives reconnect (redeclare reuses `self._dispatch`), is
  async-adapted like `cb`, and a raised `on_remove` routes through
  `on_error` as `CallbackError` (with empty payload bytes).

## Presence feed (`on_online` / `on_offline`, LIVELINESS classes)

When `on_online` or `on_offline` is given (0.4.0; `TopicError` unless
the class has `LIVELINESS = True`), `Subscriber._declare` additionally:
1. Builds the presence handler (`_build_presence_handler`): a token PUT
   calls `on_online(meta)`, a DELETE `on_offline(meta)`. `meta` is
   identity-only — the key's template captures — with `origin` `LIVE`
   (online) or `WILL` (offline).
2. Declares one liveliness subscriber per template wildcard, then queries
   the tokens already live and reports them as online with
   `origin = REPLAY`, then declares a `history=True` keeper per template
   (`_watch_presence`).

`cb` never sees presence events, and `on_remove` keeps its tombstone
meaning. `_redeclare` re-runs `_watch_presence` after a reconnect
(replaying the keys online then), and `Subscriber.close()` undeclares
them all. See [`presence.md`](../presence/presence.md).

## Encoding resolution (inbound)

Each sample is decoded based on:

1. The sample's Zenoh `encoding` attribute, if it parses as msgpack or JSON.
2. Otherwise, the effective class encoding — `'json'` under `z.debug = True`,
   else `cls.ENCODING`.

A sender using JSON and a sender using msgpack can publish to the same topic
and a single subscriber will decode both correctly.

## Per-subscriber dedupe override

Class-level `DEDUPE` (default `True`) turns on, for `RETAINED` classes,
a per-key timestamp watermark that drops a replay of a value already
held, and any re-delivered or out-of-order sample (see
[`retention.md`](../retention.md#deduplication)). A specific subscriber
can override:

```python
sub = Telemetry.on_message(cb, dedupe=False)   # this sub sees every replay
sub = Telemetry.on_message(cb, dedupe=True)    # force on, even if class disabled
sub = Telemetry.on_message(cb, dedupe=None)    # class default (the real default)
```

Useful for diagnostic / audit subscribers that want every retained
sample, separate from production subscribers that want clean
single-fire semantics.

## Schema check + warn-once

When the class has `SCHEMA = '<value>'`, the subscriber compares each
sample's wire-attached schema against the local class value. Mismatches
drop the sample and route via `on_error` as `SchemaMismatchError`
(subclass of `DecodeError`). Per-subscriber state caches
`(sender_zid, observed_schema)` pairs — the first mismatch from any
given pair fires `on_error`; subsequent samples from the same pair
drop silently to avoid log spam from a misaligned peer publishing
continuously. The sender is the zid in the sample's HLC timestamp (0.4.0;
`''` for an unstamped sample). The cache is a bounded LRU (1024 pairs; an
evicted pair warns again), cleared only by `_redeclare` after a reconnect,
since peer zids may have changed.

Subscriber classes with `SCHEMA = None` (default) skip the check;
`meta.schema` still populates from the wire if present.

## Error handling

Decode failures and callback exceptions are routed to:

- The `on_error=cb(exc, raw_bytes)` kwarg if provided.
- Otherwise a `logging.Logger` named `zeared.subscriber` — `warning` for
  decode failures, `exception` for callback crashes.

A bad sample never takes the subscriber down.

## Attributes (internal)

- `_zenoh_subs: tuple` — one `zenoh.Subscriber` per declared template;
  rebuilt by `_redeclare`.
- `_presence_subs: tuple` — the presence feed's liveliness subscribers and
  their keepers (empty without `on_online` / `on_offline`).
- `_presence_handler` — the presence handler, or `None`.
- `_session` — the session it was declared on (its registry key).
- `_watchdog` — the `_SubscriberWatchdog`, or `None`.
- `_closed: bool`
- `_msg_cls`, `_dispatch`, `_on_error`, `_auto_reconnect`,
  `_retained_fetch` — redeclaration state, set by `_declare`.
- `_seen_mismatches` — the dispatch closure's schema-mismatch cache (an
  `OrderedDict`), held so `_redeclare` can clear it.
