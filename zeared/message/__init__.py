"""``message`` — the zeared message base class.

Pattern B subdir using the mixin-extract variant. ``message.py`` holds
the ``Message`` class itself with its class attributes, the ``unretain``
and ``concrete_topic`` descriptors, and the ``_decode`` classmethod.
Method clusters extracted into mixins:

- ``_message_topic.py`` — ``_MessageTopicMixin`` (template parsing,
  schema attachment cache, slot field-binding validation).
- ``_message_send.py`` — ``_MessageSendMixin`` (``send`` / ``send_batch``).
- ``_message_async.py`` — ``_MessageAsyncMixin`` (``asend`` /
  ``asend_batch`` / ``aunretain`` / ``alisten``).
- ``_message_presence.py`` — ``_MessagePresenceMixin`` (``declare_presence`` /
  ``adeclare_presence``; ``register_will`` / ``aregister_will`` raise a pointer).
- ``_message_subscribe.py`` — ``_MessageSubscribeMixin`` (``on_message`` /
  ``published_topics``).
- ``_message_query.py`` — ``_MessageQueryMixin`` (``on_query`` / ``query`` /
  ``query_one`` / ``iter_query``).

Plus ``_message_unretain.py`` for the ``_UnretainDescriptor`` + the
shared ``_unretain_impl`` (instance-vs-class ``unretain`` dispatch),
``_message_concrete_topic.py`` for the ``_ConcreteTopicDescriptor``
(instance-vs-class ``concrete_topic``), and ``_message_render.py`` for the
canonical topic render + capture coercion shared by every key-building
path.

Public surface unchanged: callers continue to write
``from zeared.message import Message``.
"""

from .message import Encoding, Message

__all__ = ['Encoding', 'Message']
