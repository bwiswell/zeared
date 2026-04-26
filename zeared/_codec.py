from __future__ import annotations

import json
from typing import Any, Literal

import msgpack

Encoding = Literal['msgpack', 'json']

# Zenoh encoding hint strings (sent as the publisher's `encoding` attribute).
MIME: dict[Encoding, str] = {
    'msgpack': 'application/msgpack',
    'json': 'application/json',
}


def pack(data: Any, encoding: Encoding) -> bytes:
    if encoding == 'msgpack':
        return msgpack.packb(data, use_bin_type=True)
    if encoding == 'json':
        return json.dumps(data).encode('utf-8')
    raise ValueError(f'unknown encoding {encoding!r}')


def unpack(raw: bytes, encoding: Encoding) -> Any:
    if encoding == 'msgpack':
        return msgpack.unpackb(raw, raw=False)
    if encoding == 'json':
        return json.loads(raw.decode('utf-8'))
    raise ValueError(f'unknown encoding {encoding!r}')


def effective_encoding(cls_encoding: Encoding, debug: bool) -> Encoding:
    """Debug flag forces JSON across the board; otherwise honour the class attribute."""
    if debug:
        return 'json'
    return cls_encoding
