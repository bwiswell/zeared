# `presence.py`

Session-level Zenoh liveliness tokens + per-session Last-Will-Testament
(LWT) machinery. A producer calls `msg.register_will()` to stage a
payload; when the producer's session disappears (graceful close or
crash), subscribers on `LIVELINESS = True` classes synthesise the will
locally and dispatch it through the normal decode path.

## Opt-in

```python
@z.zeared
class PeerStatus(z.Message):
    TOPIC = 'peer/{name}/status'
    LIVELINESS = True                 # opt in on BOTH producer and subscriber sides
    name:  str = z.Str(required=True)
    state: str = z.Str(required=True)
```

Classes without `LIVELINESS = True` pay zero presence overhead. Attempting
`msg.register_will()` on a non-LIVELINESS class raises `TopicError`.

## Honest-synthesis semantics

Zenoh has no broker. "The will fires" happens **inside each subscriber**
by materialising the stashed payload when the peer's liveliness token
disappears. Non-subscribers never observe the event.

This is intentional. Pretending otherwise (faking a broker-side publication)
would lie about the transport. MQTT migrants who expect "the broker catches
the will and logs it" will need to rewire — anything that cares about the
offline signal in zeared must subscribe.

## New zid on auto-reconnect

A `ManagedSession` (see [`_managed_session.md`](_managed_session.md))
gets a fresh Zenoh `zid` on each underlying `zenoh.open()` — this is
how Zenoh works, not a bug. On reconnect:

1. Peers see the OLD zid's liveliness token DELETE — they synthesise our
   wills locally (the offline event fires).
2. We replay every registered will under the NEW zid via
   `_SessionPresence.replay_to`.
3. Peers see the NEW zid come online with fresh wills.

Net effect from a peer's perspective: we genuinely WERE offline for the
reconnect window. Anyone watching for liveliness changes sees a clean
offline → online transition. Don't try to suppress this — it's correct
semantics.

## Graceful close vs crash

Zenoh liveliness drops on both `session.close()` and process crash. zeared
doesn't distinguish — the will fires either way. If you need different wire
values for graceful vs crashed shutdown, publish the graceful value
explicitly before calling `close()`.

This is the right behaviour for our use cases: the publisher author writes
the will payload, so "crashed offline" and "graceful offline" converge on
the same content by design.

## Architecture

### Key-expression namespace

```
__zeared/alive/<zid>                  liveliness token — one per session
__zeared/will/<zid>/<slug>            retained will envelope — one per will
```

`<zid>` is the session's own Zenoh ID. `<slug>` is a deterministic SHA1
prefix of `f'{cls_qualname}:{concrete_topic}'`, 16 hex chars — stable
across re-registrations of the same will.

### `_WillEnvelope` — reserved wire shape

```python
@s.seared
class _WillEnvelope(s.Seared):
    source_zid:      str   = s.Str(required=True)
    target_key_expr: str   = s.Str(required=True)
    encoding:        str   = s.Str(required=True)
    payload:         bytes = s.Bytes(required=True)
```

Serialised on `__zeared/will/<zid>/<slug>`. The envelope wire encoding
follows `zeared.debug` symmetrically: msgpack normally, JSON when the
debug flag is set. Publisher, queryable replies, and subscriber-side
decode all read the chosen encoding off the sample's encoding hint, so
the three sides always agree. The user payload *inside* the envelope
continues to honor `cls.ENCODING` independently. Subscribers decode the
envelope, match `target_key_expr` against their class templates, and (on
liveliness DELETE for `<source_zid>`) synthesise a sample with the payload.

### `_SessionPresence` — per-session state

- Holds one liveliness token + one queryable + a dict of stashed will
  envelopes plus a parallel `_PrefixIndex` trie keyed on full will keys.
- Both the token and queryable are declared lazily on the first
  `register_will()` call. Sessions that never register wills pay nothing.
- The queryable serves late-joining subscribers: they issue
  `session.get(__zeared/will/**)` at observer startup and fetch already-
  registered wills across the network. Query-time matching uses the trie
  rather than an O(N) iterate-and-intersect loop. See
  [`_prefix_index.md`](_prefix_index.md).

### `_PresenceObserver` — per-session subscriber-side machinery

Declared on the first `LIVELINESS = True` subscription on a session:

- One liveliness subscriber on `__zeared/alive/**` with `history=True`
  (delivers currently-alive tokens as initial PUTs).
- One regular subscriber on `__zeared/will/**` for live will updates.
- A background thread running the initial `session.get(__zeared/will/**)`
  so `Subscriber._declare` stays snappy.
- Stashes wills indexed by `peer_zid`; on liveliness DELETE for a peer,
  fans each stashed envelope out to every interested-party dispatcher.
- Runs a daemon GC thread that sweeps the stash every `gc_interval`
  seconds (60s default; configurable via `z.peer(gc_interval=...)` or
  `z.client(gc_interval=...)` — stashed on the `ManagedSession` wrapper
  and read by `_PresenceObserver.__init__`). The sweep drops any
  bucket whose `peer_zid` is no longer in `_alive_zids`. This guards
  against a missed liveliness DELETE (e.g. during a brief network
  partition) leaking the entry forever; the periodic reconciliation is
  cheap and self-healing. The thread starts in `start()` and is
  cancelled in `stop()`. Raw `zenoh.Session` callers fall back to the
  module default — `ManagedSession` is the strategic path for
  per-session tuning.

### Synthesised sample shim

`_SynthesizedSample` is a minimal stand-in for `zenoh.Sample` — exposes
`key_expr`, `payload`, `kind` (= `PUT`), `encoding`, `timestamp` (None),
`source_info` (= the peer's zid), `attachment` (None). The subscriber's
normal `dispatch` function treats it identically to a real sample.

## User API

### `msg.register_will(*, session=None, topic=None)`

Requires `LIVELINESS = True` on the class. Renders the target concrete
topic the same way `send()` does (supports the restrictive `topic=`
override for multi-template classes). Publishes the envelope retained.

Re-registration with the same `(cls, concrete_topic)` overwrites — slug
is deterministic.

### `await msg.aregister_will(*, session=None, topic=None)`

Async counterpart. Dispatches the sync call on a thread pool worker.

### No `unregister_will` in MVP

Re-register with a different payload to change the will; there's no
explicit removal. A future API can add this when a use case surfaces.

## What's NOT here (on purpose)

- **Retention is NOT auto-transitioned on peer death.** Registering a will
  does not touch the retention cache. Late subscribers joining after a
  peer's graceful shutdown still see the last retained value — often what
  diagnostic tooling wants.
- **No subscriber watchdog.** "Is MY receive path healthy?" is a different
  question from "is the peer alive?". Deferred to 0.0.9+.
- **No `Cls.on_presence(on_online, on_offline)` API.** Synthesised wills
  flowing through `on_message` / `alisten` are the primary surface. An
  explicit presence-change API can land later if consumers actually need
  the distinct signal.

## Module helpers

- `get_presence(session) -> _SessionPresence` — idempotent per-session state.
- `get_observer(session) -> _PresenceObserver` — idempotent observer.
- `clear_presence_state(*, session=None)` — drop state + undeclare tokens + queryables.
- `clear_observer(*, session=None)` — stop observers.
