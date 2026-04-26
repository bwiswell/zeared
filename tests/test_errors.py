from zeared.errors import (
    NoSessionError,
    SubscriptionError,
    TopicError,
    ZearedError,
)


def test_error_hierarchy():
    assert issubclass(NoSessionError, ZearedError)
    assert issubclass(SubscriptionError, ZearedError)
    assert issubclass(TopicError, ZearedError)
    assert issubclass(ZearedError, Exception)


def test_error_messages():
    e = NoSessionError('no session')
    assert str(e) == 'no session'
