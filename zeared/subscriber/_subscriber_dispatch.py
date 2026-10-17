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

import datetime as dt
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
from ..meta import Origin, _is_republish, _parse_attachment_schema, _parse_hlc, _parse_origin_zid, from_sample

if TYPE_CHECKING:
    from collections import OrderedDict
    from collections.abc import Callable

    import zenoh

    from ..message import Message
    from ..watchdog import _SubscriberWatchdog


_log = logging.getLogger('zeared.subscriber')

# Default ceiling on how far ahead of the local clock a sample's HLC may
# be and still advance the dedupe watermark. Overridable per class via
# ``DEDUPE_MAX_SKEW``; ``None`` disables the check.
_DEFAULT_DEDUPE_MAX_SKEW = 300.0


def _safe_on_error(
    on_error: Callable[[Exception, bytes], None] | None,
    exc: Exception,
    raw: bytes,
    msg_cls: type[Message],
) -> bool:
    """Invoke a user ``on_error`` callback, absorbing anything it raises.

    Returns ``True`` when a callback ran, so callers can fall back to
    logging when none is registered.

    Dispatch runs on Zenoh's callback thread. An ``on_error`` that raises
    used to escape into it, which this module's own docstring promises
    against ("every failure mode through ``on_error`` / ``_log``"). The
    queryable side has its own copy of this guard (0.4.0).

    Zenoh's handler does absorb the escape, so the subscription survives
    either way; what it does not do is say what the *original* error was.
    An escaping handler therefore turned a decode failure into a bare
    "callback error" traceback with the actual cause nowhere in the log.
    Catching it here keeps both errors, and keeps the promise.
    """
    if on_error is None:
        return False
    try:
        on_error(exc, raw)
    except Exception:  # noqa: BLE001
        _log.exception(
            '%s: on_error callback itself raised; original error was: %s',
            msg_cls.__name__,
            exc,
        )
    return True


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


def _build_presence_handler(
    msg_cls: type[Message],
    templates: Any,
    on_online: Callable[..., None] | None,
    on_offline: Callable[..., None] | None,
    on_error: Callable[[Exception, bytes], None] | None,
) -> Callable[..., None]:
    """Build the handler a subscriber's liveliness subscriptions call (0.4.0).

    A token PUT is the key coming online, a DELETE the key going offline. The
    handler builds an identity-only ``ZenohMeta`` (the key's template
    captures; a token carries no payload) and calls ``on_online(meta)`` or
    ``on_offline(meta)``. ``origin`` defaults to ``LIVE`` for online and
    ``WILL`` for offline; the subscribe-time liveliness ``get`` passes
    ``REPLAY`` for the keys already online. A callback that raises routes
    through ``on_error`` like any other callback failure.
    """
    import zenoh as _zenoh

    def handle(sample: zenoh.Sample, *, origin: Origin | None = None) -> None:
        online = sample.kind == _zenoh.SampleKind.PUT
        cb = on_online if online else on_offline
        if cb is None:
            return
        key_expr = str(sample.key_expr)
        match = templates.match(key_expr)
        if match is None:
            return  # a token on a key none of the class's templates names
        try:
            meta = from_sample(sample)
            meta.origin = origin if origin is not None else (Origin.LIVE if online else Origin.WILL)
            _tpl, captures = match
            if captures:
                meta.captures = dict(captures)
            cb(meta)
        except Exception as exc:  # noqa: BLE001
            which = 'on_online' if online else 'on_offline'
            wrapped = CallbackError(f'{msg_cls.__name__} {which} raised on key_expr={key_expr!r}: {exc}')
            wrapped.__cause__ = exc
            # A token carries no payload — hand on_error empty bytes.
            if not _safe_on_error(on_error, wrapped, b'', msg_cls):
                _log.exception('%s %s callback raised', msg_cls.__name__, which)

    return handle


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
    sample alone). The retained-fetch path passes ``REPLAY``; the value
    lands on ``meta.origin`` and gates the watchdog ping (cadence measures
    the live stream only). Presence events don't come through here: see
    ``_build_presence_handler``.
    """
    import zenoh as _zenoh

    import zeared as z

    tpls = msg_cls._templates()  # noqa: SLF001

    max_skew = getattr(msg_cls, 'DEDUPE_MAX_SKEW', _DEFAULT_DEDUPE_MAX_SKEW)
    skew_warned: set[str] = set()

    def _within_skew(ts: object, key_expr: str) -> bool:
        """Whether ``ts`` is close enough to now to advance the watermark.

        ``True`` when the check is disabled, when the HLC can't be parsed
        (nothing to compare against — the pre-existing behaviour), or when
        the sample is inside the ceiling. Warns once per key so a genuinely
        skewed publisher is visible in the log rather than silently
        un-deduped.
        """
        if max_skew is None:
            return True
        issued = _parse_hlc(ts)
        if issued is None:
            return True
        skew = (issued - dt.datetime.now(dt.UTC)).total_seconds()
        if skew <= max_skew:
            return True
        if key_expr not in skew_warned:
            skew_warned.add(key_expr)
            _log.warning(
                '%s: sample on key_expr=%s is %.0fs ahead of local time '
                '(ceiling %.0fs); delivering it but not advancing the '
                'dedupe watermark. Check the publisher clock — or, if this '
                'is unexpected traffic, who is publishing on that key.',
                msg_cls.__name__,
                key_expr,
                skew,
                max_skew,
            )
        return False

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
            if not _safe_on_error(on_error, wrapped, b'', msg_cls):
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
        # A publisher's republish on link restore (0.4.0) arrives through the
        # live subscriber but carries an old value: deliver it as a replay.
        if origin is Origin.LIVE and _is_republish(sample.attachment):
            origin = Origin.REPLAY

        # Retention dedupe (RETAINED + DEDUPE classes only). A sample with
        # no timestamp (an unstamped publisher) bypasses dedupe: there is
        # nothing to order it by.
        if dedupe_active:
            ts = sample.timestamp
            if ts is not None:
                ts_str = str(ts)
                last = seen_ts.get(key_expr)
                if last is not None and ts_str <= last:
                    return  # duplicate (or out-of-order); drop
                # Deliver, but only let a sample within the skew ceiling
                # move the watermark. A far-future HLC would otherwise
                # park the watermark ahead of every genuine sample and
                # blind this subscriber to the key until restart — one
                # message, no recovery. Declining the *advance* defeats
                # that while a clock-skewed publisher still gets through;
                # dropping the sample instead would turn someone else's
                # bad clock into our own outage.
                if _within_skew(ts, key_expr):
                    seen_ts[key_expr] = ts_str
        # Schema-mismatch check — only when this class expects a
        # schema (SCHEMA != None). Pulls the wire schema from the
        # attachment; mismatches drop the sample (route via on_error
        # as SchemaMismatchError) and warn-once per (sender_zid,
        # observed_schema) pair to avoid log spam from a misaligned
        # peer. The sender is the zid in the sample's HLC timestamp:
        # Zenoh leaves ``source_info`` unset on an ordinary put, so
        # keying on it put every sender in one pair. A sample with no
        # timestamp has no sender to name ('').
        if expected_schema is not None:
            attach = sample.attachment
            attach_bytes = bytes(attach) if attach is not None else None
            observed_schema = _parse_attachment_schema(attach_bytes)
            if observed_schema != expected_schema:
                sender_zid = _parse_origin_zid(sample.timestamp) or ''
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
                    if not _safe_on_error(on_error, wrapped, raw, msg_cls):
                        _log.warning('%s', wrapped)
                return

        try:
            encoding = _pick_encoding(sample, msg_cls.ENCODING, z.debug)
            msg, captures = msg_cls._decode(raw, key_expr, encoding)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            wrapped = DecodeError(f'{msg_cls.__name__} decode failed on key_expr={key_expr!r}: {exc}')
            wrapped.__cause__ = exc
            if not _safe_on_error(on_error, wrapped, raw, msg_cls):
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
            if not _safe_on_error(on_error, wrapped, raw, msg_cls):
                _log.exception(
                    '%s subscriber callback raised',
                    msg_cls.__name__,
                )
            return
        # Successful dispatch — feed the watchdog (if any). Live samples
        # only: cadence measures the live stream, so a retained replay
        # can't establish it from stale data.
        if watchdog is not None and origin is Origin.LIVE:
            watchdog.ping()

    return dispatch
