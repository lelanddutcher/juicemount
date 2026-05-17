# Tier 6 — Search and metadata (Shade-class, optional)

Goal: content-based and metadata-rich search across the volume.
"Find clips that look like this frame," "find files tagged with
faces," "find every .prproj that references this LUT." On-device
ML, local-first, no cloud uploads.

Tier 6 is **optional**. It doesn't gate tier-1/2/3 production
readiness. If a contributor wants to build it, this doc is the spec.

## Acceptance tests

| # | Test | Pass criterion |
|---|---|---|
| 6.1 | Content-based image search | Drop an image on the search window → similar images in the volume rank-listed within 2 s |
| 6.2 | EXIF / video codec indexing | Filter by camera model, lens, codec, framerate, color space; results in <500 ms |
| 6.3 | `.juiceproject` bundle | YAML in a project root declares paths to pin + tag preferences + warmup budget |
| 6.4 | On-device only | No bytes leave the Mac unless the user explicitly enables a cloud ML hook |
| 6.5 | Indexing under budget | Background indexer never uses >10% sustained CPU or >2 GB RAM |

## Architecture

```
                    ┌─────────────────────┐
                    │  metadata.Store     │ existing SQLite FTS5
                    │                     │
                    │   + tags table      │ new
                    │   + embeddings table│ new (CLIP feature vectors)
                    │   + exif table      │ new
                    └─────────────────────┘
                              ▲
                              │
                    ┌─────────────────────┐
                    │  Indexer (new)      │ background goroutine pool
                    │                     │
                    │   - on new file:    │
                    │     mediainfo       │ codec, resolution, duration
                    │     CoreML Vision   │ object/face detection, OCR
                    │     CLIP            │ embedding for similarity
                    │     exif sidecar    │ when present
                    │                     │
                    │   - rate-limited,   │
                    │     pause-on-load   │
                    └─────────────────────┘
```

## Feature backlog

### 6.A — Content-based image/video search

CoreML CLIP model on-device (Apple's `ImageFeatures` API since
macOS 14). On indexing, run the model on a downscaled frame
(image) or sampled frames (video). Store the embedding in SQLite.

Query: user drops a reference image on the search bar → we embed
it → cosine-similarity search against the embeddings table → return
top-K.

Same model also runs for text-to-image: "warehouse at night" → text
embedding → image embeddings → results.

### 6.B — EXIF / codec / metadata indexing

`mediainfo` binary (vendored or sidecar) provides JSON for any file.
Store the relevant fields per file:

- For images: camera, lens, focal, aperture, ISO, color space.
- For video: codec, resolution, framerate, bitrate, duration.
- For audio: sample rate, channels, codec.

Surface as filters in the search window.

### 6.C — Face / object / OCR detection

Vision framework's `VNRecognizeTextRequest`, `VNDetectFaceRectangles
Request`, `VNRecognizeObjectsRequest`. On-device, no cloud.

Tags are user-confirmable: detected → suggested → user accepts →
becomes searchable. Avoids hallucinating tags users didn't endorse.

### 6.D — `.juiceproject` bundle format

A YAML file at the root of a project directory:

```yaml
# .juiceproject — committed alongside .prproj/.fcpx/.drp
name: "Brand Spot Vol 3"

pin:
  - "Footage/2024/A001"
  - "Footage/2024/A002/dailies"
  - "Audio/dialog/scene12"

warmup:
  cellular_mb: 50000
  wifi_mb: 500000

tags:
  - "client:acme"
  - "phase:edit"
  - "deadline:2026-06-15"

prefetch_on_open: true
```

Double-click → JuiceMount pins the listed paths up to the active
budget, applies the tags, and surfaces this project in the menu-bar
"Active Projects" tree.

The `tags` field is searchable via tier 5's Spotlight extension and
the in-app search window.

### 6.E — Optional external LLM hooks

Default: nothing. Off.

Opt-in only: user can enable `OpenAI` or `Ollama` for caption
generation on indexed media. The bytes that go to the LLM are
clearly scoped (a thumbnail, not the full file) and the user is
warned each time the budget is hit ("this session sent 47 thumbnails
to OpenAI").

Never default. Never silent.

## Anti-patterns

- **No cloud upload by default.** Shade's recent pivot included
  cloud-fallback for heavier models. We never do that without
  explicit opt-in.
- **No tagging as a separate product surface.** Shade positions
  itself as the organize-around-it product. We are not. Tagging
  is an additive feature in a single pane.
- **No replacing the user's folder structure.** Indexing happens
  in-place. We add metadata; we don't move files.
- **No "smart collections" that drift.** Saved searches are explicit
  named queries that re-run on demand. No background magic.

## Dependencies

- Tier 4's `.juiceproject` schema (tier 4's per-project warmup uses
  the `pin` and `warmup` blocks; tier 6 adds `tags` and content
  fields).
- Tier 5's mdimport plugin can ingest tier 6's tags for Spotlight.
- The indexer is a heavy background process — it must respect tier
  4's offline-mode (don't index while on cellular by default) and
  tier 2's preferences (CPU/RAM caps).

## Bottom line

Don't build this until tiers 1–5 are production-ready. The order of
business is: a file system that works reliably > a file system that
also has great search. Search-with-an-unreliable-mount serves nobody.

## Iteration plan (deferred — for future-self reference)

| # | Slice | Hours | Files |
|---|---|---|---|
| 6.A.1 | CoreML CLIP model integration: load Apple's `ImageFeatures` via `Vision.framework`, embed thumbnails | 6 | new `internal/index/clip.go` (cgo to Vision) or Swift sidecar |
| 6.A.2 | Embeddings table in metadata store + cosine-similarity query | 4 | extend `metadata/store.go` |
| 6.A.3 | Drop-image-on-search-bar → embed → top-K search UI | 4 | extend `SearchWindowView.swift` |
| 6.B.1 | `mediainfo` sidecar binary integration; per-file JSON parse on first stat | 5 | new `internal/index/mediainfo.go` |
| 6.B.2 | EXIF/codec filters in search UI | 3 | search window |
| 6.C.1 | Vision framework face/object detection on ingest | 5 | extend `internal/index/` |
| 6.C.2 | Tag-confirmation UI (suggested → user-accept → searchable) | 4 | tag review pane |
| 6.D.1 | `.juiceproject` YAML schema (parser, validator) — also consumed by tier 4 | 4 | new `internal/project/` |
| 6.D.2 | Search by project-tags | 2 | extend search query DSL |
| 6.E.1 | Optional LLM hooks: OpenAI/Ollama config, opt-in per-session budget | 5 | new `internal/llm/`, Preferences pane |

Total: ~42 hours. Optional/deferred.

## Signals to watch

| Item | Signal |
|---|---|
| 6.A | Drop a frame on search → top-K visually-similar clips appear in <2s; query precision >50% on a curated test set |
| 6.B | Filter "codec=ProRes422HQ" returns only matching files; sub-500ms across a 100K-file library |
| 6.C | Face-detected tags appear as suggestions; only accepted ones become searchable |
| 6.D | `git clone <repo with .juiceproject>` → open → pinned + tagged within 60s |
| 6.E | LLM-generated captions appear only after explicit opt-in; cost tracking surfaces in popover |
