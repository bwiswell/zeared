# `_mode.py` — `Mode` enum

The session-mode enum used by `SessionConfig` and the `open()`
dispatcher. Three members:

- `Mode.PEER` — peer mode (multicast scouting + explicit `connect=`
  endpoints; no router).
- `Mode.CLIENT` — client mode (one or more router endpoints required).
- `Mode.ROUTER` — router mode, a relay hub (see [`hubd.md`](hubd.md)).

Re-exported as `z.Mode` for the public surface. `open(cfg)` dispatches
to `peer(config=cfg)`, `client(config=cfg)` or `hub(config=cfg)` based
on `cfg.mode`; `ROUTER` with `cfg.auto_reconnect` set raises
`ValueError`.
