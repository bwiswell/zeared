"""Subscriber dispatch helpers — builds the per-sample callback closure
plus the small inspect / encoding / async-adapter helpers.

Sibling helper inside the ``subscriber`` Pattern B subdir. The dispatch
closure was the bulk of ``Subscriber._declare`` before the split; pulling
it out keeps the class entry point readable without sacrificing the
single ``dispatch`` reference (subscribers and reconnect-redeclare share
the same closure).
"""
from __future__ import annotations

import inspect
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING, Callable, Optional, Type

from .. import _codec as codec
from ..errors import (
    CallbackError,
    DecodeError,
    SchemaMismatchError,
    SubscriptionError,
)
from ..meta import _parse_attachment_schema, from_sample

if TYPE_CHECKING:
    import zenoh

    from ..message import Message


_log = logging.getLogger('zeared.subscriber')


def _wants_meta(cb: Callable) -> bool:
    """Inspect ``cb`` once and return True if it accepts a second positional arg."""
    try:
        sig = inspect.signature(cb)
    except (TypeError, ValueError):
        return False
    positional = 0
    for p in sig.parameters.values():
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
        elif p.kind == inspect.Parameter.VAR_POSITIONAL:
            return True  # *args catch-all — always pass meta
    return positional >= 2


def _adapt_async_callback(cb: Callable) -> Callable:
    """If ``cb`` is a coroutine function, wrap it in a sync shim that schedules
    the coroutine on the loop running at subscribe time (via
    ``run_coroutine_threadsafe``). Otherwise return ``cb`` unchanged.
    """
    if not inspect.iscoroutinefunction(cb):
        return cb
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as e:
        raise SubscriptionError(
            'async callback passed to on_message, but no running event loop '
            'at subscribe time; call from within an async context or use '
            'Cls.alisten() instead'
        ) from e
    inner = cb

    def _sync_shim(*args):
        asyncio.run_coroutine_threadsafe(inner(*args), loop)

    # Preserve arity so _wants_meta inspects correctly.
    _sync_shim.__wrapped__ = inner  # type: ignore[attr-defined]
    _sync_shim.__signature__ = inspect.signature(inner)  # type: ignore[attr-defined]
    return _sync_shim


def _make_presence_dispatcher(msg_cls, templates, dispatch) -> Callable:
    """Build an interested-party dispatcher for a presence observer.

    The dispatcher receives synthesised samples; it checks whether the
    sample's key_expr matches any of the class's declared templates, and
    if so, threads it through the subscriber's normal ``dispatch`` path.
    Returns True iff a match was found (informational; dispatcher output
    isn't used for control flow).
    """
    def on_presence(syn_sample) -> bool:
        key = syn_sample.key_expr
        # Match against the class's declared templates.
        match = templates.match(key)
        if match is None:
            return False
        try:
            dispatch(syn_sample)
        except Exception:  # noqa: BLE001
            # dispatch() already routes its own exceptions through on_error
            # / logging — nothing extra to do here.
            pass
        return True
    return on_presence


def _pick_encoding(sample: 'zenoh.Sample', cls_encoding: str, debug: bool) -> str:
    """Derive the wire encoding to use when decoding an incoming sample."""
    declared = str(sample.encoding) if sample.encoding is not None else ''
    if 'json' in declared:
        return 'json'
    if 'msgpack' in declared:
        return 'msgpack'
    # Fall back to the class default, honouring the global debug flag.
    return codec.effective_encoding(cls_encoding, debug)  # type: ignore[arg-type]


def _build_dispatch(
    msg_cls: Type['Message'],
    on_error: Optional[Callable[[Exception, bytes], None]],
    cb: Callable[..., None],
    *,
    wants_meta: bool,
    dedupe_active: bool,
    expected_schema: Optional[str],
    seen_mismatches: 'OrderedDict[tuple, None]',
    seen_ts: 'dict[str, str]',
    watchdog,
    schema_mismatch_cache_max: int,
) -> Callable[['zenoh.Sample'], None]:
    """Build the per-subscriber sample-dispatch closure.

    Returned closure handles the full sample pipeline: dedupe, schema
    check, decode, callback invocation, watchdog ping, and routing of
    every failure mode through ``on_error`` / ``_log``.
    """
    import zenoh as _zenoh
    import zeared as z

    def dispatch(sample: 'zenoh.Sample') -> None:
        # DELETE samples (tombstones) silently ignore — no callback fire.
        if sample.kind == _zenoh.SampleKind.DELETE:
            return
        raw = bytes(sample.payload)
        key_expr = str(sample.key_expr)

        # Retention dedupe (RETAINED + DEDUPE classes only). Synthesised
        # will samples carry timestamp=None and bypass dedupe — they
        # represent a meaningful single-fire offline event.
        if dedupe_active:
            ts = sample.timestamp
            if ts is not None:
                ts_str = str(ts)
                last = seen_ts.get(key_expr)
                if last is not None and ts_str <= last:
                    return   # duplicate (or out-of-order); drop
                seen_ts[key_expr] = ts_str
        # Schema-mismatch check — only when this class expects a
        # schema (SCHEMA != None). Pulls the wire schema from the
        # attachment; mismatches drop the sample (route via on_error
        # as SchemaMismatchError) and warn-once per (sender_zid,
        # observed_schema) pair to avoid log spam from a misaligned
        # peer.
        if expected_schema is not None:
            attach = sample.attachment
            attach_bytes = bytes(attach) if attach is not None else None
            observed_schema = _parse_attachment_schema(attach_bytes)
            if observed_schema != expected_schema:
                src_info = sample.source_info
                sender_zid = str(src_info) if src_info is not None else ''
                pair = (sender_zid, observed_schema)
                if pair in seen_mismatches:
                    # Touch the entry — keeps recently-seen pairs hot
                    # at the back of the OrderedDict, biasing eviction
                    # toward older / less-active senders.
                    seen_mismatches.move_to_end(pair)
                else:
                    seen_mismatches[pair] = None
                    if len(seen_mismatches) > schema_mismatch_cache_max:
                        seen_mismatches.popitem(last=False)
                    wrapped = SchemaMismatchError(
                        f'{msg_cls.__name__} schema mismatch on '
                        f'key_expr={key_expr!r}: expected '
                        f'{expected_schema!r}, got '
                        f'{observed_schema!r} (from sender '
                        f'{sender_zid!r}); subsequent samples from '
                        f'this (sender, schema) pair will drop '
                        f'silently'
                    )
                    if on_error is not None:
                        on_error(wrapped, raw)
                    else:
                        _log.warning('%s', wrapped)
                return

        try:
            encoding = _pick_encoding(sample, msg_cls.ENCODING, z.debug)
            msg, captures = msg_cls._decode(raw, key_expr, encoding)
        except Exception as exc:  # noqa: BLE001
            wrapped = DecodeError(
                f'{msg_cls.__name__} decode failed on key_expr='
                f'{key_expr!r}: {exc}'
            )
            wrapped.__cause__ = exc
            if on_error is not None:
                on_error(wrapped, raw)
            else:
                _log.warning(
                    '%s: decode failed on key_expr=%s: %s',
                    msg_cls.__name__, key_expr, exc,
                )
            return
        try:
            if wants_meta:
                meta = from_sample(sample)
                if captures:
                    meta.captures = dict(captures)
                cb(msg, meta)
            else:
                cb(msg)
        except Exception as exc:  # noqa: BLE001
            wrapped = CallbackError(
                f'{msg_cls.__name__} callback raised on key_expr='
                f'{key_expr!r}: {exc}'
            )
            wrapped.__cause__ = exc
            if on_error is not None:
                on_error(wrapped, raw)
            else:
                _log.exception(
                    '%s subscriber callback raised', msg_cls.__name__,
                )
            return
        # Successful dispatch — feed the watchdog (if any).
        if watchdog is not None:
            watchdog.ping()

    return dispatch
