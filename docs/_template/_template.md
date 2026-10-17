# `_template.py`

Parser for format-string `TOPIC` declarations on `Message` subclasses.

## `Template`

Immutable slotted dataclass representing one parsed topic pattern.

| Attribute | `robot/{id}/telemetry` | `robot/**` | `log/{service}/{tail**}` |
|-----------|------------------------|------------|--------------------------|
| `raw`         | `'robot/{id}/telemetry'`        | `'robot/**'` | `'log/{service}/{tail**}'` |
| `field_names` | `('id',)`                       | `()`         | `('service', 'tail')` |
| `wildcard`    | `'robot/*/telemetry'`           | `'robot/**'` | `'log/*/**'` |
| `publishable` | `True`                          | `False`      | `True` |
| `_regex`      | `^robot/(?P<id>[^/]+)/telemetry$` | `^robot/.+$` | `^log/(?P<service>[^/]+)/(?P<tail>.+)$` |
| `_named_multi`| `None`                          | `None`       | `'tail'` |

## Grammar

- **`{name}`** — single-segment capture. `name` must be a valid Python
  identifier; unique within the template. Allowed at any position.
- **`{name**}`** — multi-segment capture. Trailing only (must be the
  final segment); captures one-or-more remaining segments as a single
  string with embedded slashes. Subscribe AND publish capable.
- **`**`** — anonymous trailing multi-segment wildcard. Subscribe-only.

Slots can be mixed with literal text inside a segment (e.g.
`user-{id}-backup/data`). Anonymous `**` and named `{name**}` must stand
alone as a whole segment.

### Publishability

A template is publishable unless it holds an anonymous wildcard, any of
which would put a wildcard in the key it sends on:
- a trailing `**`;
- a `*` chunk in its literal text (`robot/*/status`), since 0.4.2;
- a `$*` within a literal chunk (`fleet/r-$*/status`), since 0.4.2.

Both kinds of named slot are publishable. Publishing on a subscribe-only
template, or picking one with `send(topic=…)`, raises `TopicError`.

A literal `*` or `$*` matches on receive as Zenoh matches it: `*` one whole
chunk, `$*` any run within one, none included. Until 0.4.2 the parser kept
both as literal text. `send()` put a wildcard key, and the match regex
escaped them, so the subscriber on `robot/*/status` received
`robot/r1/status` but failed to decode it. A `{name}` slot still does the
same job and keeps the value (capture-only when it isn't a declared
field).

| Template form                          | Subscribe | Publish |
|----------------------------------------|-----------|---------|
| `peer/{name}/status` (named single)    | ✓         | ✓       |
| `peer/{cluster}/{host}/status`         | ✓         | ✓       |
| `log/{service}/{tail**}` (named multi) | ✓         | ✓       |
| `robot/*/status` (anonymous single)    | ✓         | ✗ (0.4.2; it sent on `robot/*/status` before) |
| `fleet/r-$*/status` (within a chunk)   | ✓         | ✗ (0.4.2) |
| `peer/**` (anonymous trailing multi)   | ✓         | ✗       |
| `**` (lone anonymous multi)            | ✓         | ✗       |

## `Template.parse(template: str) -> Template`

Validates and constructs. Raises `TopicError` on:

- Empty / non-string template.
- Format spec or conversion inside `{...}` (`{x:03d}`, `{x!r}`,
  `{x**:fmt}`, `{x**!r}`).
- Invalid slot name.
- Duplicate slot name within a template.
- Anonymous `**` in a non-trailing position.
- `{name**}` not at the trailing segment.
- More than one `{name**}` in a single template.
- First literal segment is `__zeared` (reserved namespace) — see below.

### `__zeared/**` namespace reservation

The `__zeared` prefix is reserved for zeared's internal keys. Presence
used it until 0.4.0 (`__zeared/alive/<zid>`, `__zeared/will/<zid>/<slug>`);
its tokens sit on contract keys now, and the prefix stays reserved so a
future internal key can't stomp on a user's. Parse-time `TopicError`
rather than silent stomping.

Exemption: the unmodified anonymous catch-all forms `**` (universal)
and `__zeared/**` (diagnostic — "subscribe to all internal traffic
explicitly"). Named multi-segment under the prefix
(`__zeared/{tail**}`) and any single-segment captures
(`__zeared/alive/{x}`) are NOT exempt — they make a structural claim
on internal routing.

| Template form | Verdict |
|---------------|---------|
| `__zeared/alive/{x}` | `TopicError` |
| `__zeared/will/foo` | `TopicError` |
| `__zeared/**` | exempt (diagnostic catch-all) |
| `__zeared/{tail**}` | `TopicError` |
| `__zeared/alive/{x}` (in `EXTRA_TOPICS`) | `TopicError` |
| `__zeared` (lone literal) | `TopicError` (defensive) |
| `**` | exempt (universal catch-all) |
| `mything/__zeared/x` | passes (only first segment matters) |
| `{tenant}/__zeared/x` | passes (first segment is a runtime slot) |

## `Template.render(values: dict) -> str`

Renders the concrete topic for publish. Raises `TopicError` if:
- The template isn't publishable (contains an anonymous `**`).
- A `{name}` or `{name**}` slot is missing from `values`.
- A `{name**}` slot's value is the empty string (`''`) — the wire
  semantics of `**` is one-or-more segments, so an empty tail would
  produce a key the subscribe-side regex would never match. Fail loudly.

For a named multi-segment slot, the supplied value is substituted
verbatim — slashes pass through unescaped, so
`Log(service='svc', tail='a/b/c').send()` renders `log/svc/a/b/c`.

## `Template.render_selector(partial: dict) -> str`

Renders a query selector key. The query getters (`Cls.query`,
`iter_query`, `query_one`) call it on the canonical template, after
running the supplied slot values through the canonical render.

- A supplied slot is substituted as `str(value)`.
- A slot that is absent or `None` widens: a `{name}` slot to `*`, a
  trailing `{name**}` slot to `**`.
- Keys that aren't slots of the template are ignored.
- Values pass through verbatim, with no `/` check, so wildcards survive:
  a whole `*` segment, or Zenoh's `$*` inside a segment (`'r-$*'`).
- A subscribe-only template (anonymous `**`) renders as its plain
  `wildcard`, whatever is passed.

```python
t = Template.parse('log/{service}/{tail**}')
t.render_selector({})                                    # 'log/*/**'
t.render_selector({'service': 'api'})                    # 'log/api/**'
t.render_selector({'service': 'api-$*', 'tail': 'a/b'})  # 'log/api-$*/a/b'
Template.parse('peer/**').render_selector({'x': 1})      # 'peer/**'
```

## `Template.match(key_expr: str) -> Optional[dict[str, str]]`

Regex-matches an incoming concrete key. Returns a `{name: str}` dict of
captures (all strings — callers coerce via each field's `deserialize`),
or `None` if the key doesn't match. `{name}` captures use `[^/]+` (no
slash crossing); trailing anonymous `**` matches `.+`; `{name**}`
captures `(?P<name>.+)` (slashes included).

## Field-binding validation

A slot's declared seared field must render as its key segment(s). Both
checks run at the first `_templates()` call
(`Message._validate_slot_field_bindings`), so a bad binding fails before
any key is built:

- **`{name**}`:** the field (if any) must be
  `z.Str(many=False, keyed=False)`. Other field types (`Int`, `Float`,
  `Bool`, `Enum`, `T`, `Union`, `NDArray`, `Bytes`, `Date*`, `UUID`,
  `many=True` / `keyed=True` collections) make no sense for a path-tail
  string.
- **`{name}`** (0.4.0): the field's string form must never contain `/`,
  or the rendered key splits and matches no template. `Path` can, and so
  can `Bytes` in its default base64 form, so both raise `TopicError`;
  `Bytes(encoding='hex')` can't. A value that still renders with a `/`
  (a `Str` slot holding `'a/b'`) raises when a key is built to send on.

Capture-only slots (no declared field) land on `meta.captures[name]: str`
as usual.

## `Templates`

A class's whole declared topic set: canonical `TOPIC` + `EXTRA_TOPICS`.
Each template is parsed independently; there is **no** cross-template
slot-set requirement.

- `canonical: Template` — the publish default.
- `extras: tuple[Template, ...]` — additional templates (any mix of
  publishable and subscribe-only).
- `all: tuple[Template, ...]` — canonical + extras, declaration order.
- `field_names: frozenset[str]` — union of capture names across templates.
- `multi_field_names: frozenset[str]` — names of `{name**}` slots
  across templates (used by field-binding validation).

### `Templates.resolve_publish_topic(override: Optional[str]) -> Template`

Restrictive publish override. Raises `TopicError` if `override` isn't
one of the declared raw templates, or if the resolved template isn't
publishable (contains an anonymous `**`).

### `Templates.match(key_expr: str) -> Optional[tuple[Template, dict]]`

Tries each declared template in order; returns the first matching
`(template, captures)` pair. Order matters for overlapping templates —
more-specific first.
