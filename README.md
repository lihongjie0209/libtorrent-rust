# libtorrent-rs (tokio powered)

Experimental Rust reimplementation of the `libtorrent` session stack built on top of Tokio. The goal is to support thousands of independent BitTorrent server instances inside a single process while preserving the architectural ideas from the original C++ project located under `refs/libtorrent`.

## Workspace layout

```
crates/
  libtorrent/        # High-level session + peer actors
  libtorrent-proto/  # Wire-level primitives (handshake, messages, codecs)
```

## Current capabilities

- Tokio-based listener that can spawn a large number of independent `Session`s sharing the same runtime.
- Minimal BitTorrent handshake implementation (receive handshake, validate info-hash, reply with local handshake).
- Back-pressure using a semaphore to avoid overloading a single node with too many concurrent peers.

## uTP (BEP 29)

- Build flag: enable with `--features utp`.
- Runtime toggle: set `SessionConfig.utp_enabled = true` to accept/prefer uTP.
- Optional bind: set `SessionConfig.utp_bind = Some(<ip:port>)` to listen on a dedicated UDP address/port for uTP; defaults to `listen_addr` when `None`.

Example (tests):

```powershell
cargo test --features utp
```

## Running the demo server

```
cargo run -p libtorrent --example simple_server
```

Environment variables:

- `LT_SESSION_COUNT` – how many independent sessions to spawn (default `4`).
- `LT_BASE_PORT` – first TCP port to bind (each subsequent session increments by 1).

Once running, press `Ctrl+C` to stop the example. The example aborts all session tasks when shutting down.

## Next steps

1. Flesh out the protocol crate with message parsing (bitfield, piece, choke, etc.).
2. Port the disk I/O and storage layers (likely using `tokio::fs` + `mio`-style direct I/O).
3. Implement DHT support in a dedicated crate (`libtorrent-dht`).
4. Add integration tests that perform actual BitTorrent handshakes between sessions for regression coverage.
