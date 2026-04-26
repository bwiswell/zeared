"""``message`` — the zeared message base class.

Pattern B subdir using the mixin-extract variant. ``message.py`` holds
the ``Message`` class itself with its class attributes, the ``unretain``
descriptor, and the ``_decode`` classmethod. Method clusters extracted
into mixins:

- ``_message_topic.py`` — ``_MessageTopicMixin`` (template parsing,
  schema attachment cache, multi-segment field-binding validation).
- ``_message_send.py`` — ``_MessageSendMixin`` (``send`` / ``send_batch``).
- ``_message_async.py`` — ``_MessageAsyncMixin`` (``asend`` /
  ``asend_batch`` / ``aunretain`` / ``alisten``).
- ``_message_will.py`` — ``_MessageWillMixin`` (``register_will`` /
  ``aregister_will``).
- ``_message_subscribe.py`` — ``_MessageSubscribeMixin`` (``on_message`` /
  ``published_topics``).

Plus ``_message_unretain.py`` for the ``_UnretainDescriptor`` + the
shared ``_unretain_impl`` (instance-vs-class ``unretain`` dispatch).

Public surface unchanged: callers continue to write
``from zeared.message import Message``.
"""
from .message import Encoding, Message


__all__ = ['Encoding', 'Message']
