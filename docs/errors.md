# `errors.py`

Exception hierarchy. All zeared exceptions extend `ZearedError` which extends
`Exception`.

```
ZearedError
├── NoSessionError
├── TopicError
├── SessionDeadError
├── SubscriptionError              # declare-time, fatal to one subscription
└── SubscriberError                # dispatch-time, routed to on_error
    ├── DecodeError
    │   └── SchemaMismatchError
    ├── CallbackError
    └── RetainedFetchError
```

| Exception | Raised by | When |
|-----------|-----------|------|
| `ZearedError` | — | base; not raised directly |
| `NoSessionError` | `_SessionHandle.resolve` | no session resolvable (no kwarg, no scope, no default) |
| `TopicError` | `Template.parse`, `Message._template` | TOPIC missing, malformed, referencing undeclared fields, or the renderer is missing a field at send time |
| `SessionDeadError` | `ManagedSession.{put,get,delete,...}` | session is mid-reconnect or has terminally failed reconnect |
| `SubscriptionError` | `Subscriber._declare` | underlying `zenoh.Session.declare_subscriber` failed at declare-time |

## `SubscriberError` family — dispatch-time errors

Distinct from `SubscriptionError`: a `SubscriberError` is per-message,
routed to the user's `on_error=` callback if registered (else logged).
The subscription continues. Three subclasses:

| Exception | Raised by | When |
|-----------|-----------|------|
| `DecodeError` | `Subscriber._declare` dispatch | `codec.unpack` / `Message.load` raised |
| `SchemaMismatchError` (`< DecodeError`) | dispatch, schema check | sample's wire `schema` ≠ class's `SCHEMA`. Sample dropped; warn-once per `(sender_zid, observed_schema)` pair |
| `CallbackError` | dispatch | user's `cb(msg)` itself raised |
| `RetainedFetchError` | `_fetch_retained` | a retained-fetch reply failed to dispatch |

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
        # RetainedFetchError or future subclass
        log.warning('subscriber error: %s', exc)
```

Subscriber callback exceptions (raised by user code during message
delivery) are routed to the `on_error` callback wrapped in
`CallbackError`. They do not propagate out of the subscriber and do not
stop the subscription.
