class ZearedError(Exception):
    """Base exception for all zeared errors."""


class NoSessionError(ZearedError):
    """Raised when a zeared operation needs a session but none is resolvable."""


class SubscriptionError(ZearedError):
    """Raised when a subscription cannot be declared or maintained."""


class TopicError(ZearedError):
    """Raised when a Message's TOPIC template is malformed or refers to unknown fields."""


class SubscriberError(ZearedError):
    """Parent for errors raised inside ``Subscriber`` dispatch / fetch
    paths. Distinct from :class:`SubscriptionError`, which is fatal at
    declare-time. ``SubscriberError`` is per-message and routed to
    ``on_error=`` if the user registered one; the subscription continues.
    """


class DecodeError(SubscriberError):
    """Raised when a sample's payload fails to decode (codec.unpack or
    ``Message.load``). Original exception available via ``__cause__``.
    """


class SchemaMismatchError(DecodeError):
    """Raised when a sample's wire-attached ``schema`` value does not
    match the receiving class's ``SCHEMA`` attribute. Subclass of
    :class:`DecodeError` so callers handling decode failures generically
    catch it for free; callers wanting to discriminate use ``isinstance``.
    """


class CallbackError(SubscriberError):
    """Raised when the user's ``cb(msg)`` (or ``cb(msg, meta)``) callback
    itself raises. Original exception available via ``__cause__``.
    """


class RetainedFetchError(SubscriberError):
    """Raised when a retained-fetch reply (issued by the subscriber's
    initial ``session.get(wildcard)``) fails to dispatch. Original
    exception available via ``__cause__``.
    """


class SessionDeadError(ZearedError):
    """Raised when a publish targets a session that is mid-reconnect or
    has terminally failed reconnect (auto-reconnect exhausted ``max_attempts``).

    Catchable distinct from generic transport errors — callers wanting to
    queue, retry, or drop can branch on this without losing reach to other
    exceptions.
    """
