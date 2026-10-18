class ZearedError(Exception):
    """Base exception for all zeared errors."""


class NoSessionError(ZearedError):
    """Raised when a zeared operation needs a session but none is resolvable."""


class SubscriptionError(ZearedError):
    """Raised when a subscription cannot be declared or maintained."""


class TopicError(ZearedError):
    """Raised for a key that can't be built or used.

    A malformed ``TOPIC`` template; a slot field that can't render as its
    segment, or a value that would split one (0.4.0); a missing slot; a
    ``topic=`` override that matches no declared template; or an operation
    the class doesn't support (retained calls without ``RETAINED``,
    presence without ``LIVELINESS``, ``on_query`` on a ``RETAINED`` class).
    """


class KeyExprError(ZearedError, ValueError):
    """Raised by ``zeared.keyexpr`` for a string that isn't a Zenoh key expression (0.4.2).

    The message is Zenoh's reason (``read-*``: ``*`` may only be preceded by
    ``/`` or ``$``), with a hint where one helps. A ``ValueError`` too, so
    code that checks its own rules' syntax catches it as one.
    """


class SubscriberError(ZearedError):
    """Parent for errors raised inside ``Subscriber`` dispatch / fetch paths.

    Distinct from :class:`SubscriptionError`, which is fatal at declare-time.
    ``SubscriberError`` is per-message and routed to ``on_error=`` if the user
    registered one; the subscription continues.
    """


class DecodeError(SubscriberError):
    """Raised when a sample's payload fails to decode (codec.unpack or ``Message.load``).

    Original exception available via ``__cause__``.
    """


class SchemaMismatchError(DecodeError):
    """Raised when a sample's wire-attached ``schema`` value does not match the receiving class's ``SCHEMA`` attribute.

    Subclass of :class:`DecodeError` so callers handling decode failures generically
    catch it for free; callers wanting to discriminate use ``isinstance``.
    """


class CallbackError(SubscriberError):
    """Raised when the user's ``cb(msg)`` (or ``cb(msg, meta)``) callback itself raises.

    Original exception available via ``__cause__``.
    """


class RetainedFetchError(SubscriberError):
    """Raised when a retained-fetch reply fails to dispatch.

    Raised when a retained-fetch reply fails to dispatch: one from the
    subscriber's subscribe-time ``session.get(wildcard)``, or from the same
    fetch re-run after a reconnect or by the re-attach walk. Original
    exception available via ``__cause__``.
    """


class QueryableError(ZearedError):
    """Raised when a queryable cannot be declared or maintained.

    Fatal at declare-time (mirrors :class:`SubscriptionError` on the
    subscribe side): a failed ``declare_queryable``. It is also what the
    serving side hands ``on_error`` when a query's context can't be built,
    a reply fails, or the handler raises. Distinct from :class:`QueryError`,
    which the getting side gets per reply.
    """


class QueryError(SubscriberError):
    """Parent for per-reply errors on the get side of ``Cls.query``.

    Covers two modes: a remote queryable answered with an *error* reply
    (``query.reply_err``), and a reply sample that failed to decode. Both
    are per-reply and routed to ``on_error=`` when the caller supplied one;
    the ``query()`` call keeps collecting the remaining replies. Subclass
    of :class:`SubscriberError` so generic ``except SubscriberError``
    handlers catch it for free. Original exception (for decode failures)
    available via ``__cause__``.
    """


class SessionDeadError(ZearedError):
    """Raised when publishing to a reconnecting or dead session.

    Raised when a publish targets a session that is mid-reconnect or has terminally
    failed reconnect (auto-reconnect exhausted ``max_attempts``).

    Catchable distinct from generic transport errors — callers wanting to
    queue, retry, or drop can branch on this without losing reach to other
    exceptions.
    """
