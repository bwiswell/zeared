# `hubd.py` — the relay hub daemon

`python -m zeared.hubd` runs a stateless Zenoh **router** that relays traffic
between nodes which can't reach each other directly — e.g. two peers behind
NAT, both outbound-only, each connecting *out* to the hub. It relays pub/sub,
queries, and liveliness, so everything zeared needs works through it:
retention / queryables (which route as queries) and presence (which
routes as liveliness). The routing happens in Zenoh's Rust core; the Python
main thread only holds the session open and waits for a shutdown signal.

Named `hubd` (the daemon) to pair with the `zeared.hub()` factory it wraps —
a submodule and a same-named function can't coexist on the `zeared` namespace,
and the `-d` suffix mirrors `zenohd`.

## CLI

```bash
python -m zeared.hubd [-l ENDPOINT ...] [-c ENDPOINT ...] \
                      [--config FILE] [--no-timestamping] [--log-level LEVEL]
```

- `-l/--listen` — endpoint(s) to bind (repeatable). Without it, the hub
  binds the `--config` file's listen endpoints, and `tcp/0.0.0.0:7447` only
  when the file names none (or there is no file). Given, it overrides the
  file's (0.3.9; before, the default overwrote the file's listeners).
- `-c/--connect` — endpoint(s) of other hubs to link to (repeatable), for a
  multi-hub mesh (HA / scale). Given, it overrides the file's; without it,
  the file's stand.
- `--config` — a JSON5 Zenoh config file for **TLS / access-control**.
  zeared's own settings land over it, as over a `SessionConfig.zenoh_config_file`
  (0.3.9): **router mode** (this daemon is a router; before 0.3.9 a file
  without `mode` opened a peer), **timestamping** unless `--no-timestamping`,
  and **shared memory off**, whatever the file says: a hub is where Zenoh's
  shared-memory transport first starts silently dropping large messages
  (`config.md` → "Shared memory off"). A hub embedded with
  `z.hub(zenoh_config=...)` is taken as given, so set router mode and shared
  memory off there.
- `--no-timestamping` — don't force on the HLC timestamping that's on by
  default (retention dedupe needs it), with or without `--config`.

Stops cleanly on `SIGINT` / `SIGTERM`.

## Library entry point

`zeared.hubd.run(*, listen=, connect=, timestamping=True, zenoh_config=,
stop=None, on_ready=None)` opens the hub and blocks until `stop` (a
`threading.Event`) is set — or, when `stop` is omitted, until a signal
arrives. `on_ready(session)` fires once the hub is up. Pass your own `stop`
to embed or test the daemon without signals.

## Deploying as a systemd service

The hub is a stateless daemon — the rio-* deployment model. A minimal unit:

```ini
[Unit]
Description=zeared relay hub
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/opt/zeared/.venv/bin/python -m zeared.hubd \
  --listen tls/0.0.0.0:7447 --config /etc/zeared/hub.json5
Restart=on-failure
# Logs go to stdout → journald (stdlib logging, no extra config).

[Install]
WantedBy=multi-user.target
```

## Security

A public hub is reachable by anyone who can hit the port. **Configure TLS/mTLS
and Zenoh access-control** via `--config` (or `z.hub(zenoh_config=...)`) before
exposing it. The daemon itself performs no auth — that's the transport's job,
by design, so consumers can layer their own identity/scope policy.

## Limits

- **Single hub is a SPOF.** Run several and link them (`--connect`); nodes can
  list multiple routers.
- **No message-layer involvement.** The hub routes opaque samples — it needs
  no schemas / `rio-protocol` and never decodes payloads.
