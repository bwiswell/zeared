"""Subscriber dispatch helpers and the per-sample callback closure.

Subscriber dispatch helpers — builds the per-sample callback closure plus the small
inspect / encoding / async-adapter helpers.

Sibling helper inside the ``subscriber`` Pattern B subdir. The dispatch
closure was the bulk of ``Subscriber._declare`` before the split; pulling
it out keeps the class entry point readable without sacrificing the
single ``dispatch`` reference (subscribers and reconnect-redeclare share
the same closure).
"""

from __future__ import annotations

import contextlib
import inspect
import logging
from typing import TYPE_CHECKING, Any

from .. import _codec as codec
from ..errors import (
    CallbackError,
    DecodeError,
    SchemaMismatchError,
    SubscriptionError,
)
from ..meta import Origin, _parse_attachment_schema, from_sample

if TYPE_CHECKING:
    from collections import OrderedDict
    from collections.abc import Callable

    import zenoh

    from ..message import Message
    from ..watchdog import _SubscriberWatchdog


_log = logging.getLogger('zeared.subscriber')


_META_CALLBACK_ARITY = 2  # (msg, meta) — a 2-arg callback opts into metadata


def _wants_meta(cb: Callable) -> bool:
    """Inspect ``cb`` once and return True if it accepts a second positional arg."""
    try:
        sig = inspect.signature(cb)
    except TypeError, ValueError:
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
    return positional >= _META_CALLBACK_ARITY


def _adapt_async_callback(cb: Callable) -> Callable:
    """Adapt an async callback into a sync shim, or pass it through.

    If ``cb`` is a coroutine function, wrap it in a sync shim that schedules the
    coroutine on the loop running at subscribe time (via ``run_coroutine_threadsafe``).
    Otherwise return ``cb`` unchanged.
    """
    if not inspect.iscoroutinefunction(cb):
        return cb
    import asyncio

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as e:
        msg = (
            'async callback passed to on_message, but no running event loop '
            'at subscribe time; call from within an async context or use '
            'Cls.alisten() instead'
        )
        raise SubscriptionError(msg) from e
    inner = cb

    def _sync_shim(*args: Any) -> None:
        asyncio.run_coroutine_threadsafe(inner(*args), loop)

    # Preserve arity so _wants_meta inspects correctly.
    _sync_shim.__wrapped__ = inner  # ty: ignore[unresolved-attribute]
    _sync_shim.__signature__ = inspect.signature(inner)  # ty: ignore[unresolved-attribute]
    return _sync_shim


def _make_presence_dispatcher(
    msg_cls: type[Message],  # noqa: ARG001  (kept for call-site symmetry)
    templates: Any,
    dispatch: Callable[..., None],
) -> Callable:
    """Build an interested-party dispatcher for a presence observer.

    The dispatcher receives synthesised samples; it checks whether the
    sample's key_expr matches any of the class's declared templates, and
    if so, threads it through the subscriber's normal ``dispatch`` path.
    Returns True iff a match was found (informational; dispatcher output
    isn't used for control flow).
    """

    def on_presence(syn_sample: zenoh.Sample) -> bool:
        key = syn_sample.key_expr
        # Match against the class's declared templates.
        match = templates.match(key)
        if match is None:
            return False
        # dispatch() already routes its own exceptions through on_error
        # / logging — nothing extra to do here.
        with contextlib.suppress(Exception):
            dispatch(syn_sample, origin=Origin.WILL)
        return True

    return on_presence


def _pick_encoding(
    sample: zenoh.Sample,
    cls_encoding: codec.Encoding,
    debug: bool,  # noqa: FBT001
) -> codec.Encoding:
    """Derive the wire encoding to use when decoding an incoming sample."""
    declared = str(sample.encoding) if sample.encoding is not None else ''
    if 'json' in declared:
        return 'json'
    if 'msgpack' in declared:
        return 'msgpack'
    # Fall back to the class default, honouring the global debug flag.
    return codec.effective_encoding(cls_encoding, debug)


def _build_dispatch(  # noqa: C901, PLR0913, PLR0915
    msg_cls: type[Message],
    on_error: Callable[[Exception, bytes], None] | None,
    cb: Callable[..., None],
    *,
    wants_meta: bool,
    dedupe_active: bool,
    expected_schema: str | None,
    seen_mismatches: OrderedDict[tuple, None],
    seen_ts: dict[str, str],
    watchdog: _SubscriberWatchdog | None,
    schema_mismatch_cache_max: int,
    on_remove: Callable[..., None] | None = None,
) -> Callable[[zenoh.Sample], None]:
    """Build the per-subscriber sample-dispatch closure.

    Returned closure handles the full sample pipeline: dedupe, schema
    check, decode, callback invocation, watchdog ping, and routing of
    every failure mode through ``on_error`` / ``_log``.

    ``on_remove`` (optional) receives DELETE samples (tombstones — e.g. a
    peer's ``unretain()``). It's invoked with a ``ZenohMeta`` whose
    ``captures`` carry the removed key's template slots; a tombstone has no
    payload, so no typed instance is reconstructed. When ``on_remove`` is
    ``None``, DELETE samples are dropped silently (historical behaviour).

    The returned closure takes a keyword-only ``origin`` (default
    ``Origin.LIVE`` — the underlying ``zenoh.Subscriber`` calls it with the
    sample alone). The retained-fetch and presence paths pass ``REPLAY`` /
    ``WILL`` respectively; the value lands on ``meta.origin`` and gates the
    watchdog ping (cadence measures the live stream only).
    """
    import zenoh as _zenoh

    import zeared as z

    tpls = msg_cls._templates()  # noqa: SLF001

    def _dispatch_remove(
        sample: zenoh.Sample,
        origin: Origin,
        cb: Callable[..., None],
    ) -> None:
        key_expr = str(sample.key_expr)
        try:
            meta = from_sample(sample)
            meta.origin = origin
            match = tpls.match(key_expr)
            if match is not None:
                _tpl, captures = match
                if captures:
                    meta.captures = dict(captures)
            cb(meta)
        except Exception as exc:  # noqa: BLE001
            wrapped = CallbackError(f'{msg_cls.__name__} on_remove raised on key_expr={key_expr!r}: {exc}')
            wrapped.__cause__ = exc
            # A tombstone carries no payload — hand on_error empty bytes.
            if on_error is not None:
                on_error(wrapped, b'')
            else:
                _log.exception(
                    '%s on_remove callback raised',
                    msg_cls.__name__,
                )

    def dispatch(  # noqa: C901, PLR0912, PLR0915
        sample: zenoh.Sample,
        *,
        origin: Origin = Origin.LIVE,
    ) -> None:
        # DELETE samples (tombstones): route to on_remove if the subscriber
        # registered one, else drop silently (no typed instance to build).
        if sample.kind == _zenoh.SampleKind.DELETE:
            if on_remove is not None:
                _dispatch_remove(sample, origin, on_remove)
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
                    return  # duplicate (or out-of-order); drop
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
            msg, captures = msg_cls._decode(raw, key_expr, encoding)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            wrapped = DecodeError(f'{msg_cls.__name__} decode failed on key_expr={key_expr!r}: {exc}')
            wrapped.__cause__ = exc
            if on_error is not None:
                on_error(wrapped, raw)
            else:
                _log.warning(
                    '%s: decode failed on key_expr=%s: %s',
                    msg_cls.__name__,
                    key_expr,
                    exc,
                )
            return
        try:
            if wants_meta:
                meta = from_sample(sample)
                meta.origin = origin
                if captures:
                    meta.captures = dict(captures)
                cb(msg, meta)
            else:
                cb(msg)
        except Exception as exc:  # noqa: BLE001
            wrapped = CallbackError(f'{msg_cls.__name__} callback raised on key_expr={key_expr!r}: {exc}')
            wrapped.__cause__ = exc
            if on_error is not None:
                on_error(wrapped, raw)
            else:
                _log.exception(
                    '%s subscriber callback raised',
                    msg_cls.__name__,
                )
            return
        # Successful dispatch — feed the watchdog (if any). Live samples
        # only: cadence measures the live stream, so a retained replay
        # can't establish it from stale data and a will (the producer
        # died) can't reset the quiet timer.
        if watchdog is not None and origin is Origin.LIVE:
            watchdog.ping()

    return dispatch
