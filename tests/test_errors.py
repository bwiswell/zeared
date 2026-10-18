from zeared.errors import (
    KeyExprError,
    NoSessionError,
    SubscriptionError,
    TopicError,
    ZearedError,
)


def test_error_hierarchy():
    assert issubclass(NoSessionError, ZearedError)
    assert issubclass(SubscriptionError, ZearedError)
    assert issubclass(TopicError, ZearedError)
    # A ValueError too, so a caller checking its own rules catches it as one (0.4.2).
    assert issubclass(KeyExprError, ZearedError)
    assert issubclass(KeyExprError, ValueError)
    assert issubclass(ZearedError, Exception)


def test_error_messages():
    e = NoSessionError('no session')
    assert str(e) == 'no session'
