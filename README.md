# Retorrent

Lightweight BitTorrent client for **desktop** (Linux / Windows / macOS) and **Android**, with a custom Rust engine and a [Repose](https://github.com/mlm-games/repose)-based UI.

[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

> UI is early / experimental (Repose). Engine seems functional for day-to-day use, but might have unsolved edgecases.

## Features

### Engine
- **Magnet links** and **`.torrent` files** (including Android content/file intents and `magnet:` scheme)
- **DHT** (bootstrap nodes, routing table, peer discovery; optional)
- **Trackers** (HTTP/UDP announce tiers)
- **Peer exchange (PEX)** and **web seeds** (optional)
- **UPnP / IGD port mapping** for TCP + UDP (refreshing lease; non-fatal on failure)
- **Resume data** (piece bitfield, stats, file priorities, previous run state; auto-resume)
- **File priorities** — Skip / Low / Normal / High
- **Rate limits** — global download/upload (token-bucket style)
- **Endgame mode**, piece pipelining, connection limits, choke / optimistic unchoke, seed-ratio options
- **Storage** — mmap-friendly layout, optional preallocation, disk cache size
- **Headless mode** — `--headless` with periodic stats logging

### Early UI (tracking helpers for reusable parts)
- **Piece map** visualization and per-file progress
- Add via magnet dialog, URL fetch, or file picker (pending-add flow with path + file selection)
- **Desktop system tray** (show/hide, quit; optional minimize-to-tray / close-to-tray)
- Dark theme by default

### Platforms
- **Desktop** — multi-threaded Tokio runtime, tray integration
- **Android** — foreground service (`dataSync` / mediaPlayback), notifications permission, deep links for torrents/magnets, window insets, external files download dir
- Config + resume under platform data dirs (`config.json`, `resume/*.resume.json`)

## Build / run

### Desktop
```bash
git clone https://github.com/mlm-games/retorrent
cd retorrent
cargo run --release
# or
cargo run --release -- path/to/file.torrent
cargo run --release -- "magnet:?xt=urn:btih:..."
cargo run --release -- --headless --download-dir /path/to/downloads
```

Binary name: `retorrent-desktop` (see `Cargo.toml` `default-run`).

### Android
Package id: `org.mlm.retorrent`. Build with the project’s Android metadata (`cargo-apk` / packaging under `others` + `java/`). Handles:

- `MAIN` / `LAUNCHER`
- `VIEW` for `application/x-bittorrent` (content + file)
- `VIEW` for `magnet:`

Permissions include Internet, network state, foreground service (data sync / media playback), notifications, wake lock.

### Config highlights (`config.json`)

| Option | Notes |
|--------|--------|
| `download_dir` | Default download root |
| `listen_port` | Default 6881 |
| `dht_enabled` / `upnp_enabled` / `webseed_enabled` / `pex_enabled` | Feature toggles |
| `max_connections` / `max_connections_per_torrent` | Connection caps |
| `max_download_rate` / `max_upload_rate` | 0 = unlimited |
| `cache_size_mb`, `prealloc_files`, `endgame_mode`, `pipeline_depth` | Storage / protocol tuning |
| `seed_ratio_limit` / `seed_ratio_enabled` | Seeding policy |
| `auto_resume`, `minimize_to_tray` | Session / desktop UX |

## Architecture (high level)

```
src/
  engine.rs      # Multi-torrent engine, resume, start/pause/remove
  network.rs     # Per-torrent session
  peer.rs        # Peer wire
  piece.rs       # Piece manager
  storage.rs     # Disk I/O
  tracker.rs     # Tracker client
  dht/           # DHT node + routing
  nat.rs         # UPnP mapping
  webseed.rs     # HTTP web seeds
  metainfo.rs    # .torrent / bencode
  bencode.rs
  ui/            # Repose UI (app, components, theme, icons)
  tray.rs        # Desktop tray
  android_service.rs
```

Peer ID prefix: `-RE0100-`. License: **AGPL-3.0**.

## Status & limitations

- UI is kinda early, layout and polish will evolve with Repose.
- Peer list detail UI is minimal (“shown during active connections”).
- Not a drop-in replacement for every advanced qBittorrent/Transmission feature set.
  
## Contributing

Issues and PRs welcome. Please keep the engine dependency-light and failures (e.g. UPnP) non-fatal where practical.

## License

AGPL-3.0 — see [LICENSE](LICENSE).
