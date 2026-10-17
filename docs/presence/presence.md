# `presence.py`

Presence as a Zenoh liveliness token at each contract key (0.4.0). A
publisher declares that one concrete key of a `LIVELINESS = True` class is
available; subscribers hear the key come online and go offline. This
replaced the will payloads of earlier releases, which a liveliness token
can't carry.

## Opt-in

```python
@z.zeared
class ServicePresence(z.Message):
    TOPIC = 'svc/presence/{owner}/{component}/{instance}'
    RETAINED = True                   # the typed snapshot, if you want one
    LIVELINESS = True                 # opt in to presence
    owner:     str  = z.Str(required=True)
    component: str  = z.Str(required=True)
    instance:  str  = z.Str(required=True)
    alive:     bool = z.Bool(default=True)
```

## Publishing: `declare_presence`

| Call | |
|------|---|
| `msg.declare_presence(*, session=None, topic=None)` | holds the instance's own key |
| `Cls.declare_presence(*, session=None, topic=None, **slots)` | holds the key the slots name; every slot required |
| `await msg.adeclare_presence(...)` / `await Cls.adeclare_presence(...)` | the same, on a worker thread |

Each declares a Zenoh liveliness token at the concrete key, rendered by the
same canonical render `send` uses (so a `/` in a single-segment value
raises `TopicError`), and returns a `z.PresenceToken`:

| `PresenceToken` | |
|-----------------|---|
| `key` | the concrete key held |
| `active` | whether the session still holds it through this handle |
| `undeclare()` | withdraws the token while the session lives on; idempotent |
| `with token:` | withdraws on exit |

- **One token per key per session.** Declaring a key the session already
  holds returns the same `PresenceToken`, so one `undeclare()` withdraws it.
- **The handle survives a reconnect.** It names the key, not the Zenoh
  token; the restore walk re-declares the key on the new session, and a
  later `undeclare()` withdraws that one.
- **Closing, crashing or `z.release` withdraws every token** the session
  holds; peers see the keys go offline.
- `LIVELINESS = True` is required (`TopicError` otherwise).

`register_will` / `aregister_will` were removed in 0.4.0. Reading either
raises `AttributeError` with a pointer to `declare_presence`, so
`hasattr(Cls, 'register_will')` reports them gone.

## Subscribing: `on_online` / `on_offline`

```python
def online(meta: z.ZenohMeta):  ...   # meta.captures: the key's slots
def offline(meta: z.ZenohMeta): ...

ServicePresence.on_message(handle_record, on_online=online, on_offline=offline)
```

| Event | Callback | `meta.origin` |
|-------|----------|---------------|
| a key already online when the subscriber started (or after its reconnect) | `on_online` | `Origin.REPLAY` |
| a key coming online afterwards | `on_online` | `Origin.LIVE` |
| a key going offline | `on_offline` | `Origin.WILL` |

- **Identity only.** A token carries no payload, so the event is a
  `ZenohMeta` whose `captures` hold the key's template slots (coerce them
  with `Cls.coerce_captures`).
- **Separate feeds.** `cb` never sees presence events; the typed "up" is
  your retained record. `on_remove` stays the tombstone feed: a deletion
  (`unretain`) and an offline are different facts.
- **`LIVELINESS = True` classes only** (`TopicError` otherwise). `async def`
  callbacks are supported.
- **How it works:** the subscriber declares a liveliness subscriber per
  template wildcard, then queries the tokens already live (dispatched as
  `REPLAY`). A token that appears in between is reported twice, which is
  harmless. A session's own tokens are delivered to its own subscribers.
- `alisten` yields typed messages only; it never yields presence events.

## Semantics

**Tokens aggregate by key.** To an observer that holds none itself, a key
reads online when its first token appears and offline when its last one
goes, from whichever sessions held them (checked on `eclipse-zenoh` 1.9.0
and 1.10.1). A second holder therefore never causes a false offline. One
exception: a session that holds a key *and* watches it sees the key go
offline when another holder leaves, though it still holds it. That only
arises when two sessions claim one identity.

**Only subscribers see it.** Zenoh has no broker to archive an offline
event; anything that needs it subscribes.

**Graceful close and crash look the same.** Both withdraw the tokens.
Publish a final record before closing if you want them to differ.

**A reconnect reads as offline then online** for the keys a session holds:
the old session's tokens went with it, and the restore walk re-declares
them after the swap. (If the old session was still alive when the new
token arrived, aggregation hides the flap.)

**A hub restart reads as offline then online for every remote key.** When
the link to the hub drops, Zenoh reports every token it learned through
the hub as gone, and as back once the link returns. That's true — the
session can't see those peers — and zeared doesn't suppress it.
`on_link_lost` fires first, so a consumer that wants "unknown" rather than
"down" can hold its rendering until `on_link_restored`.

**ACL.** A session that holds presence needs `liveliness_token` on its
contract keys. The `__zeared/alive/**` and `__zeared/will/**` keys of
earlier releases are gone; the `__zeared` prefix stays reserved for
zeared's internals.

**Trust.** The token model makes availability ACL-expressible per key, not
authenticated: until the hub enforces an ACL, anyone can declare a token
on any key.

## Architecture

### `PresenceToken` (`presence.py`)

The handle `declare_presence` returns. It holds the `_SessionPresence` and
the key, never the Zenoh token, so a reconnect can swap the token under it.

### `_SessionPresence` (`_presence_session.py`)

Per-session state: `concrete key → (zenoh.LivelinessToken, PresenceToken)`,
created on the first `declare_presence`. `declare`, `undeclare`, `holds`,
`keys`, `replay_to(new_session)` (the reconnect walk's re-declare; handles
stay valid) and `drop()`. The module registry is keyed on `id(session)`:
`get_presence(session)` and `clear_presence_state(*, session=None)`.

### The subscriber side (`subscriber/`)

`Subscriber._declare` builds the handler (`_build_presence_handler`) and its
liveliness subscribers when `on_online` / `on_offline` are given;
`_redeclare` rebuilds them after a reconnect, and `close()` undeclares
them. There is no cross-session observer.

## Module helpers

| Name | |
|------|---|
| `get_presence(session)` | the session's `_SessionPresence`, created on demand |
| `clear_presence_state(*, session=None)` | withdraws a session's tokens (all sessions without `session=`); `z.release` calls it before closing the session |
