# `_codec.py`

Bytes ⇄ dict wire codec. seared handles the dict ⇄ Python-object layer;
zeared handles the dict ⇄ bytes layer here.

## API

```python
Encoding = Literal['msgpack', 'json']

pack(data: Any, encoding: Encoding) -> bytes
unpack(raw: bytes, encoding: Encoding) -> Any
effective_encoding(cls_encoding: Encoding, debug: bool) -> Encoding
MIME: dict[Encoding, str]  # Zenoh encoding hint strings
```

## Behaviour

| encoding | `pack` | `unpack` | MIME |
|----------|--------|----------|------|
| `'msgpack'` | `msgpack.packb(data, use_bin_type=True)` | `msgpack.unpackb(raw, raw=False)` | `application/msgpack` |
| `'json'`    | `json.dumps(data).encode('utf-8')`         | `json.loads(raw.decode('utf-8'))`  | `application/json` |

`effective_encoding` applies the global debug flag:

```python
if debug: return 'json'
return cls_encoding
```

Unknown encoding strings raise `ValueError`.

## Why msgpack by default

Binary, compact (typically ~40% of the JSON size for typical structured
payloads), and cross-language. JSON is the debug-friendly opt-in — enable
per-class via `ENCODING = 'json'` or globally via `zeared.debug = True`.
