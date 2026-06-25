# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

bili-sync is a Bilibili video sync tool for NAS users, built in Rust with a SvelteKit web UI. It automatically downloads videos from Bilibili favorites, collections, submissions, and watch-later lists, generating media-server-friendly output (NFO, posters, ASS danmaku, SRT subtitles) merged via ffmpeg.

## Commands

### Backend (Rust)

```bash
# Build (release, the standard target is linux-musl)
cargo build --target x86_64-unknown-linux-musl --release

# Run locally
cargo run

# Run all automated tests
cargo test

# Run a single test
cargo test <test_name>

# Lint (CI blocks on warnings)
cargo clippy -- -D warnings

# Format check (requires nightly rustfmt — uses custom config in rustfmt.toml)
cargo +nightly fmt --check

# Format fix
cargo +nightly fmt
```

Many tests touching the Bilibili API are marked `#[ignore = "only for manual test"]` and require real credentials. Run them with `cargo test -- --ignored`.

### Frontend (web/)

```bash
cd web
bun install --frozen-lockfile
bun run dev       # Dev server, proxies /api to localhost:12345
bun run build     # Static build → web/build/
bun run lint      # prettier check + eslint (CI runs this)
bun run format
bun run check     # TypeScript type check
```

### Justfile

`just debug` builds the frontend then runs `cargo run`. `just build` builds frontend + cross-compiled Rust. `just build-docker` additionally packages into a Docker image.

## CI Pipeline

`.github/workflows/pr-check.yaml` is the quality gate, triggering on PRs and pushes to `main`. Both jobs must pass:
- **Backend:** `cargo +nightly fmt --check` → `cargo clippy -- -D warnings` → `cargo test`
- **Frontend:** `bun install --frozen-lockfile` → `bun run lint`

The other workflows (`build-binary`, `build-doc`, `commit-build`, `release-build`) handle cross-compilation, Docker packaging, docs, and releases — not PR checks.

## Build & Packaging

**Frontend → binary embedding**: The SvelteKit frontend is built statically (`adapter-static`, `fallback: index.html`) into `web/build/`, then embedded into the Rust binary at compile time via `rust-embed-for-web` (`task/http_server.rs`, `#[folder = "../../web/build"]`), with brotli/gzip pre-compression. All non-`/api` routes fall through to this embedded SPA. **Always build the frontend first** — `web/build/` only holds a `.gitkeep` in git, so `cargo build` without a frontend build embeds nothing useful and the Web UI will be broken.

**Dockerfile** (two-stage, `just build-docker`): `alpine` installs `ffmpeg` and unpacks a pre-built `bili-sync-rs` tarball (selected by `TARGETPLATFORM` for amd64/armv7/aarch64) → copied into a `scratch` image. **Rust is not compiled inside Docker**; the binary comes from a prior cross-compile.

**CI release** (push tag `v*` → `release-build.yaml` → `build-binary.yaml`):
- Matrix cross-compiles 6 targets via `actions-rust-cross`: Linux `x86_64`/`aarch64`/`armv7` musl, macOS x86_64/aarch64 darwin, Windows `x86_64-pc-windows-msvc`.
- The frontend is built once as an artifact and shared across all target jobs.
- Produces a draft GitHub Release + multi-arch Docker image (amd64/arm64/armv7) pushed to DockerHub.

**ffmpeg**: Hard runtime dependency — `main.rs` checks `ffmpeg -version` at startup and exits if missing; `downloader.rs` uses it to merge separate video/audio streams into MP4 (`-c copy`). Override with `--ffmpeg-path` or the `BILI_SYNC_FFMPEG_PATH` env var.

**Windows note**: `just build*` hardcodes the `x86_64-unknown-linux-musl` target and will fail on Windows. For a native Windows build use `cargo build --release` (binary at `target/release/bili-sync-rs.exe`); cross-compiling to Linux requires `cross` (via Docker) or `cargo-zigbuild`.

## Architecture

Cargo workspace with 3 crates:

| Crate | Purpose |
|---|---|
| `crates/bili_sync` | Main binary: API client, download engine, HTTP server, scheduling |
| `crates/bili_sync_entity` | SeaORM entity models (video, page, favorite, collection, submission, watch_later, config) |
| `crates/bili_sync_migration` | SQLite schema migrations (12 migrations, tracking evolution since 2024) |

### Main crate module layout (`crates/bili_sync/src/`)

- **`main.rs`** — Entry point. Initializes logging, verifies ffmpeg, sets up SQLite (WAL mode), runs migrations, initializes versioned config, then spawns two long-lived tasks under a `CancellationToken`/`TaskTracker`:
  1. HTTP server (Axum) — serves embedded SvelteKit SPA + REST API
  2. `DownloadTaskManager` — cron-scheduled download cycles with config hot-reload via `arc-swap`

- **`database.rs`** — SQLite connection setup: WAL journal mode, normal synchronous, connection pool (max 50, min 5), busy timeout, optimize on close.

- **`bilibili/`** — Bilibili API client: WBI request signing, credential management with auto-refresh, video/stream metadata, danmaku-to-ASS conversion, subtitle parsing, risk control detection (HTTP 412/403 terminates the current cycle). Cookie sanitization (strips `ac_time_value` to avoid multi-line cookie issues).

- **`workflow.rs`** — Core download pipeline: `process_video_source` → `refresh_video_source` (fetch from Bilibili, chunks of 30) → `fetch_video_details` (uses `stream::buffer_unordered` for concurrency control, batch-updates video details and pages in single transactions to minimize DB round-trips) → `download_unprocessed_videos` → per-page download (video stream, poster, NFO, danmaku, subtitles). Videos returning -404 are marked invalid via batch update.

- **`adapter/`** — `VideoSource` trait unified via `enum_dispatch` across 4 source types: `Favorite`, `Collection`, `Submission`, `WatchLater`. Each adapter handles fetching, filtering, and path management for its source type.

- **`api/`** — Axum REST routes under `/api`: config, dashboard, login (QR code flow), me, task, video_sources, videos, plus WebSocket endpoints for live logs/sysinfo/task status. Token-based auth middleware.

- **`config/`** — `Config` struct (credential, paths, handlebars templates for file naming, concurrency limits, notifier settings, filter/danmaku options). Versioned and stored in the SQLite database. CLI args parsed via `clap` into the global `ARGS`.

- **`downloader.rs`** — HTTP file downloader supporting serial and parallel (chunked) modes. Uses ffmpeg to merge separate video+audio streams into MP4.

- **`error.rs`** — `ExecutionStatus` enum: domain-specific error type for download outcomes (`Skipped`, `Succeeded`, `Ignored`, `Failed`, `Fixed`).

- **`utils/`** — Shared utilities: `model` (batch DB operations for videos/pages with conflict handling), `download_context`, `format_arg` (handlebars template args), `nfo` (NFO generation), `notify`, `rule` (filter rule evaluation), `status` (video/page status tracking), `validation`, `filenamify`, `signal`, `convert`.

- **`notifier/`** — Telegram bot and generic webhook notifications.

- **`task/http_server.rs`** — Embeds SvelteKit build output via `rust-embed-for-web`; non-API routes fall through to the SPA.

- **`task/video_downloader.rs`** — `DownloadTaskManager` with `tokio-cron-scheduler`; supports interval and cron expressions. Reloads schedule on config change.

### Frontend (`web/`)

SvelteKit static SPA (Svelte 5, Tailwind CSS 4, Vite). Routes: `/` (dashboard), `/video-sources`, `/videos`, `/video/[id]`, `/settings`, `/logs` (WebSocket), `/me`. API client in `web/src/lib/api.ts`. Uses `bits-ui`, `layerchart`, and `qrcode` packages.

## Code Conventions

- **Rust edition 2024**, toolchain pinned to `1.96.0` in `rust-toolchain.toml`.
- **rustfmt** requires nightly: `group_imports = "StdExternalCrate"`, `imports_granularity = "Module"`, `max_width = 120`.
- Use `anyhow::Result` / `anyhow::Context` for error handling. `ExecutionStatus` in `error.rs` is the domain-specific error type for download outcomes.
- Config is hot-reloaded; avoid reading config values directly from disk at runtime — use the in-memory `VersionedConfig` / `arc-swap` mechanism.
- The Bilibili client is a shared `Arc<BiliClient>` passed into both tasks.
