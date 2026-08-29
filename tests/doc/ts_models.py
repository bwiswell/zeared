"""Fixture Message/payload classes exercising the TypeScript emitter's
field→type mapping across the seared field spectrum."""
from __future__ import annotations

import enum

import zeared as z


class Band(enum.Enum):
    UHF = 0
    HF = 1


class Mode(enum.Enum):
    FAST = 'fast'
    SLOW = 'slow'


@z.zeared
class Inner(z.Zeared):
    """A nested payload struct."""

    a: int = z.Int(required=True)
    b: str | None = z.Str(default=None)


@z.zeared
class StartArgs(z.Zeared):
    speed: int = z.Int(required=True)


@z.zeared
class StopArgs(z.Zeared):
    reason: str = z.Str(required=True)


@z.zeared
class FlatA(z.Zeared):
    x: int = z.Int(required=True)


@z.zeared
class Spectrum(z.Message):
    """Every field kind, for the TS mapping."""

    TOPIC = 'rio/telemetry/spectrum/{id}'
    SCHEMA = '3'

    # scalars + optionality
    id: int = z.Int(required=True)                       # required
    name: str = z.Str(required=True)
    count: int = z.Int(default=0)                        # optional, non-null
    ratio: float | None = z.Float(default=None)          # nullable
    flag: bool = z.Bool(default=True)
    blob: bytes = z.Bytes(required=True)                 # base64 → string
    price: object = z.Decimal(as_number=True, required=True)   # number
    label: object = z.Decimal(required=True)             # string (default form)

    # collections
    tags: list = z.Str(many=True, default_factory=list)  # string[]
    counts: dict = z.Int(keyed=True, default_factory=dict)  # Record<string, number>
    pair: tuple = z.Tuple(z.Int(), z.Str(), required=True)  # [number, string]

    # enums
    band: Band = z.Enum(enum=Band, default=Band.UHF)     # 0 | 1
    mode: Mode = z.Enum(enum=Mode, required=True)        # "fast" | "slow"

    # nested + unions
    inner: Inner = z.T(Inner, required=True)
    action: object = z.Union(                            # nested envelope
        variants={'start': StartArgs, 'stop': StopArgs},
        tag_key='type', payload_key='args', default=None,
    )
    kind: object = z.Union(                              # flat envelope
        variants={'a': FlatA}, tag_key='kind',
    )

    # wire-key aliasing + non-identifier key + doc + load-only field
    renamed: str = z.Str(required=True, data_key='wireName')
    dashed: str = z.Str(required=True, data_key='x-ray')
    noted: int = z.Int(default=0, doc='a documented field')
    internal: int = z.Int(default=0, dump=False)         # excluded from wire
