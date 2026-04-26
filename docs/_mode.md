# `_mode.py` — `Mode` enum

The session-mode enum used by `SessionConfig` and the `open()`
dispatcher. Two members:

- `Mode.PEER` — peer mode (multicast scouting + explicit `connect=`
  endpoints; no router).
- `Mode.CLIENT` — client mode (one or more router endpoints required).

Re-exported as `z.Mode` for the public surface. `open(cfg)` dispatches
to `peer(config=cfg)` or `client(config=cfg)` based on `cfg.mode`.
