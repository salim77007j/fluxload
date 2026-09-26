# fluxload

**A hybrid-engine download manager for 2026.** Native Rust, memory-safe, multi-connection, multi-protocol.

![build](https://github.com/salim77007j/fluxload/actions/workflows/ci.yml/badge.svg)

## Why

Download managers have been stuck in 2005: single-protocol, 32-bit, ad-encrusted, or all three.
fluxload is a clean-room, commercial-grade alternative:

- **Hybrid engine** — HTTP/1.1, HTTP/2 and HTTP/3 (QUIC) with multi-connection segmented transfer,
  plus opportunistic BitTorrent acceleration when a torrent/magnet is available for the same payload.
- **Crash-safe by construction** — partial-file layout with per-range fsync-before-metadata commits,
  atomic metadata writes, and byte-exact resume after `kill -9` mid-transfer.
- **Integrity for multi-TB files** — streaming SHA-256 over ranges, verified without a second pass.
- **Adaptive RAM cache** — token-bucket rate limiting and a memory budget that scales with system RAM.
- **Privacy-first** — no telemetry, no accounts, no ads. Direct connections or your own proxy.
- **Rust everywhere** — memory safety at the protocol-parsing boundary where C download managers
  historically fail.

## Crates

| Crate | Purpose |
|---|---|
| `flux-core` | Engine: segments, resume, integrity, cache, scheduler, security, hybrid torrents |
| `flux-cli` | `flux` — headless CLI (scripts, servers, CI) |
| `flux-gui` | `fluxload` — native desktop GUI (egui): queue, live speed graph, themes |
| `flux-testsrv` | Local test server: ranges, throttling, HTTP/3, BitTorrent tracker |

## Build

```bash
# needs Rust 1.85+, and the http3 feature requires:
export RUSTFLAGS="--cfg reqwest_unstable"
cargo build --release --workspace
```

GUI builds need Linux X11/Wayland dev libraries (`libxkbcommon-dev libwayland-dev libx11-dev`)
or nothing extra on Windows.

## Test

```bash
cargo test --workspace --release
```

The suite is real: it starts a local HTTP/QUIC/tracker server, transfers actual bytes,
kills processes mid-download and verifies byte-exact resume, and runs a live BitTorrent swarm.

## Binaries

- `flux` — CLI: `flux add URL`, `flux list`, `flux pause ID`, `flux resume ID`, `flux doctor`
- `fluxload` — GUI: paste a URL or magnet, press Enter

## Status

`1.0.0-beta.1` — core engine complete and under test; GUI in active polish.
License architecture is in place (Ed25519-signed keys, demo key for development).
