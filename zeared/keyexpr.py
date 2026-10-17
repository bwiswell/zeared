"""Key expressions, matched as Zenoh matches them (0.4.2).

Anything that matches or shapes keys by hand, an access rule or a route,
should ask here rather than split on ``/`` itself. The answers are Zenoh's
own, from the ``zenoh.KeyExpr`` the mesh routes by, so they can't drift from
what a subscriber, a queryable or a getter actually reaches:

- ``*`` is one chunk, ``**`` any number of chunks (none included), and ``$*``
  any run of characters within one chunk (``r-$*`` matches ``r-7``);
- a key is canonized first, so ``a/**/**`` reads as ``a/**``;
- a string that isn't a key expression raises :class:`~zeared.KeyExprError`.

:func:`intersects` and :func:`includes` compare two key expressions.
:func:`shape` turns a ``TOPIC`` template into the key expression of the keys
it can render, and :func:`is_concrete` says whether a key names one key
rather than many. An access check is these in combination:

- may this rule reach this key? ``includes(rule, key)``;
- could this route ever deliver something the rule allows?
  ``intersects(rule, shape(topic))``;
- does the rule cover everything the route can carry?
  ``includes(rule, shape(topic))``.
"""

from __future__ import annotations

import re

import zenoh

from ._template import Template
from .errors import KeyExprError

#: Zenoh appends where in its source it refused a key; the reason is what's useful.
_SOURCE = re.compile(r' at /\S+$')


def intersects(a: str, b: str) -> bool:
    """Whether some key matches both ``a`` and ``b``.

    Args:
        a: A key expression.
        b: Another.

    Returns:
        ``True`` if one key could match both: a subscription on one would
        receive a put on the other, and a getter on one would reach a
        queryable on the other.

    Raises:
        KeyExprError: If either isn't a key expression.
    """
    return _key(a).intersects(_key(b))


def includes(a: str, b: str) -> bool:
    """Whether ``a`` matches every key ``b`` matches.

    With a concrete ``b``, this is whether ``a`` matches that key: an access
    rule against the key a message is sent on.

    Args:
        a: A key expression.
        b: Another.

    Returns:
        ``True`` if every key matching ``b`` also matches ``a``.

    Raises:
        KeyExprError: If either isn't a key expression.
    """
    return _key(a).includes(_key(b))


def shape(topic: str) -> str:
    """The key expression of every key a ``TOPIC`` template can render or decode.

    ``{name}`` becomes ``*``. A trailing ``{name**}`` becomes ``*/**``, one
    chunk or more, since a tail slot never renders empty, and so does a
    trailing anonymous ``**``, which never decodes empty. The result is
    canonical. ``Template.wildcard``, the form a subscriber declares, keeps
    ``**`` instead: subscribing more widely costs nothing.

    Args:
        topic: A ``TOPIC`` or ``EXTRA_TOPICS`` template.

    Returns:
        The key expression, canonical.

    Raises:
        TopicError: If ``topic`` isn't a valid template.
        KeyExprError: If its literal text isn't a valid key expression.
    """
    wildcard = Template.parse(topic).wildcard
    if wildcard.endswith('/**'):
        wildcard = f'{wildcard[:-3]}/*/**'
    return str(_key(wildcard))


def is_concrete(key: str) -> bool:
    """Whether ``key`` names one key, holding no ``*``, ``**`` or ``$*``.

    A queryable's captures keep a wildcard chunk as written (``'*'`` or
    ``'r-$*'``), so this is how a handler tells a wide query from a narrow one.

    Args:
        key: A key expression, or one chunk of one.

    Returns:
        ``True`` if it holds no wildcard.

    Raises:
        KeyExprError: If it isn't a key expression.
    """
    return '*' not in str(_key(key))


def _key(text: str) -> zenoh.KeyExpr:
    """``text`` as a canonical ``zenoh.KeyExpr``, or a :class:`KeyExprError` saying why not."""
    if not isinstance(text, str):
        msg = f'a key expression is a str, got {type(text).__name__}'
        raise TypeError(msg)
    try:
        return zenoh.KeyExpr.autocanonize(text)
    except zenoh.ZError as exc:
        reason = _SOURCE.sub('', str(exc))
        if any('*' in chunk and chunk not in ('*', '**') and '$*' not in chunk for chunk in text.split('/')):
            reason += '; within a chunk, Zenoh writes the wildcard `$*` (`read-$*`, not `read-*`)'
        raise KeyExprError(reason) from None


__all__ = ['includes', 'intersects', 'is_concrete', 'shape']
