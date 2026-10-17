# `errors.py`

Exception hierarchy. All zeared exceptions extend `ZearedError` which extends
`Exception`.

```
ZearedError
├── NoSessionError
├── TopicError
├── SessionDeadError
├── SubscriptionError              # declare-time, fatal to one subscription
├── QueryableError                 # declare-time, fatal to one queryable; serving-side on_error
└── SubscriberError                # dispatch-time, routed to on_error
    ├── DecodeError
    │   └── SchemaMismatchError
    ├── CallbackError
    ├── RetainedFetchError
    └── QueryError                 # get side, per reply
```

| Exception | Raised by | When |
|-----------|-----------|------|
| `ZearedError` | — | base; not raised directly |
| `NoSessionError` | `_SessionHandle.resolve` | no session resolvable (no kwarg, no scope, no default) |
| `TopicError` | `Template.parse`, `Message._templates`, `Templates.resolve_publish_topic`, the publish render, the retained / presence / `on_query` entry points | a key that can't be built or used: no `TOPIC`; template syntax (bad or duplicate slot name, format spec, `**` anywhere but last, the reserved `__zeared` prefix); a `{name**}` slot not bound to a plain `z.Str`; a single-segment slot bound to `Path` or base64 `Bytes` (0.4.0); a `/` in a single-segment value on publish (0.4.0); a missing slot; a `topic=` that names no declared template, or a subscribe-only one; `retain=True`, `unretain` or `fetch_retained` without `RETAINED`; `on_query` on a `RETAINED` class; presence (`declare_presence`, `on_online` / `on_offline`) without `LIVELINESS` |
| `SessionDeadError` | `ManagedSession.{put,get,delete,...}` | session is mid-reconnect or has terminally failed reconnect |
| `SubscriptionError` | `Subscriber._declare` | underlying `zenoh.Session.declare_subscriber` (or a presence liveliness subscriber) failed at declare-time, or an `async def` callback came without a running loop |
| `QueryableError` | `Queryable._declare`, the query dispatch | `declare_queryable` failed at declare-time, or an `async def` handler came without a running loop; handed to the serving side's `on_error` when a query's context can't be built, a reply fails, or the handler raises |

## `SubscriberError` family — dispatch-time errors

Distinct from `SubscriptionError`: a `SubscriberError` is per-message,
routed to the user's `on_error=` callback if registered (else logged).
The subscription continues. Four direct subclasses:

| Exception | Raised by | When |
|-----------|-----------|------|
| `DecodeError` | `Subscriber._declare` dispatch | `codec.unpack` / `Message.load` raised |
| `SchemaMismatchError` (`< DecodeError`) | dispatch, schema check | sample's wire `schema` ≠ class's `SCHEMA`. Sample dropped; warn-once per `(sender_zid, observed_schema)` pair, the sender being the zid in the sample's HLC timestamp (0.4.0; `''` without one) |
| `CallbackError` | dispatch | user's `cb(msg)` itself raised, or `on_remove`, `on_online` or `on_offline` did |
| `RetainedFetchError` | `_fetch_retained` | a retained-fetch reply failed to dispatch |
| `QueryError` | `Cls.query` / `iter_query` / `query_one` | a remote queryable sent an error reply, or a reply failed to decode; the call keeps collecting the rest |

Each wraps the original exception via `__cause__`, so user code that
catches generically (`except Exception` or `except SubscriberError`)
works unchanged. Callers wanting to discriminate use `isinstance`:

```python
def on_err(exc, raw):
    if isinstance(exc, z.SchemaMismatchError):
        log_schema_drift(exc)
    elif isinstance(exc, z.DecodeError):
        log_corrupt_payload(exc, raw)
    elif isinstance(exc, z.CallbackError):
        # user code blew up — surface to the caller
        log_app_bug(exc)
    else:
        # RetainedFetchError, QueryError or future subclass
        log.warning('subscriber error: %s', exc)
```

Subscriber callback exceptions (raised by user code during message
delivery) are routed to the `on_error` callback wrapped in
`CallbackError`. They do not propagate out of the subscriber and do not
stop the subscription.
