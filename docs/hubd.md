# `hubd.py` — the relay hub daemon

`python -m zeared.hubd` runs a stateless Zenoh **router** that relays traffic
between nodes which can't reach each other directly — e.g. two peers behind
NAT, both outbound-only, each connecting *out* to the hub. It relays pub/sub,
queries, and liveliness, so everything zeared needs works through it:
retention / queryables (which route as queries) and presence / LWT (which
route as liveliness). The routing happens in Zenoh's Rust core; the Python
main thread only holds the session open and waits for a shutdown signal.

Named `hubd` (the daemon) to pair with the `zeared.hub()` factory it wraps —
a submodule and a same-named function can't coexist on the `zeared` namespace,
and the `-d` suffix mirrors `zenohd`.

## CLI

```bash
python -m zeared.hubd [-l ENDPOINT ...] [-c ENDPOINT ...] \
                      [--config FILE] [--no-timestamping] [--log-level LEVEL]
```

- `-l/--listen` — endpoint(s) to bind (repeatable; default `tcp/0.0.0.0:7447`).
- `-c/--connect` — endpoint(s) of other hubs to link to (repeatable), for a
  multi-hub mesh (HA / scale).
- `--config` — a JSON5 Zenoh config file for **TLS / access-control**;
  `--listen` / `--connect` still layer on top.
- `--no-timestamping` — disable the HLC timestamping that's on by default
  (retention dedupe needs it).

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
