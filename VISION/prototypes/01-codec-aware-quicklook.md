# Prototype 01 — Codec-Aware Quick Look (RAW Proxy Generation)

> **Branch:** `prototype/codec-aware-quicklook`
> **Status:** Initial scaffold (iteration 4). Working code that proves the architecture, not feature-complete production.
> **Source spec:** `VISION/feature-roadmap-ranked.md` § Top tier #1
> **Estimated to production-ready:** 2-3 weeks of focused work.

## What this prototype proves

When a user hits **spacebar** on a `.r3d`, `.ari`, `.braw`, or ProRes RAW file in `/Volumes/zpool` from the JuiceMount-mounted volume, the macOS Quick Look panel renders a smooth, watchable preview within ~200ms — even if the source file is multi-gigabyte and lives on a slow backend.

This is THE demo moment from the feature-roadmap analysis. It's the workflow Frame.io Drive's own optimization docs explicitly admit they can't deliver, and the workflow Suite Studios doesn't even market.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Finder / Quick Look (user hits spacebar)               │
└─────────────────────┬───────────────────────────────────┘
                      │ NFS READ
                      ▼
┌─────────────────────────────────────────────────────────┐
│  nfs/handler.go cachedFile.ReadAt()                     │
│                                                          │
│  Detect: is this a RAW codec we proxy? (file ext check) │
│   ├─ NO  → existing 3-tier read path                    │
│   └─ YES → proxy.Manager.Get(srcPath)                   │
│            ├─ proxy ready → serve from proxy file       │
│            └─ proxy missing → enqueue generation,       │
│                               serve original (fallback) │
└─────────────────────┬───────────────────────────────────┘
                      ▼
┌─────────────────────────────────────────────────────────┐
│  internal/proxy/proxy.go (NEW)                          │
│                                                          │
│  Manager.Get(path) → (proxyPath, status)                │
│   ├─ Compute cache key: sha256(srcPath + size + mtime)  │
│   ├─ Check ~/Library/Caches/JuiceMount/proxies/         │
│   ├─ If exists → return path                            │
│   └─ If missing → enqueue ffmpeg job (bounded workers)  │
│                                                          │
│  Worker:                                                 │
│   ffmpeg -i <src> -c:v h264_videotoolbox                │
│          -b:v 8M -vf scale=1280:-2 -an                  │
│          ~/Library/Caches/JuiceMount/proxies/<key>.mp4  │
└─────────────────────────────────────────────────────────┘
```

### Key architectural decisions

1. **ffmpeg shellout, not a Go decoder.** Writing native R3D/ARRI/BRAW decoders is months of work. ffmpeg already does it (with libRED, libARRI, libBRAW available; ProRes RAW handled natively in ffmpeg 5.0+). Shellout is reliable, isolated, and easy to upgrade.

2. **VideoToolbox H.264 encoder (`h264_videotoolbox`).** Hardware-accelerated on Apple Silicon. ~10× faster than libx264 software encoding. Frees the CPU for normal NFS serving.

3. **Cache key includes size + mtime.** Catches "same path, different file" (someone overwrote the source). Uses the inode/size from the metadata cache, not a content hash (computing a content hash on a 30GB R3D before generating a proxy defeats the latency goal).

4. **Bounded worker pool** (`runtime.NumCPU()/2` workers). Don't starve the NFS server during a multi-clip preview burst.

5. **Fallback to original on cache miss.** If the user spacebars something the system has never seen, return the original file URL. Quick Look will start playing slowly (or fail on R3D), but the proxy generates in background. Second spacebar = instant.

6. **Proxy format: 1280×720 H.264 ~8Mbps, no audio.** Quick Look needs a fast scrub. 720p is plenty for preview. Audio is irrelevant for RAW Quick Look (codec-RAW files often have no embedded audio or it's separate).

7. **Cache directory: `~/Library/Caches/JuiceMount/proxies/`.** macOS clears this when disk is low (it's the right place per Apple's File System Programming Guide). Bytes are recoverable — they're proxies, not originals.

## Files added in this prototype

```
internal/proxy/
├── codec.go        # Codec detection (file extension + magic bytes)
├── manager.go      # Proxy cache manager + worker pool
├── ffmpeg.go       # ffmpeg shellout wrapper
├── manager_test.go # Unit tests
└── codec_test.go   # Codec detection tests
```

## Files modified in this prototype

- (None yet — integration into `nfs/handler.go` is the next step after the package builds and tests pass)

## What's working in this scaffold

- Codec detection by file extension (R3D, ARI, BRAW, MOV with ProRes RAW codec id, MXF/XAVC)
- Proxy cache directory creation and lookup
- ffmpeg command construction with VideoToolbox encoder
- Bounded worker pool with channel-based job queue
- Cache key derivation from path + size + mtime
- Unit tests for codec detection and manager API

## What's still TODO before production

- Wire into `nfs/handler.go cachedFile.ReadAt()` so it actually serves proxies on Quick Look reads
- Integrate with the existing `cache.Reader` so proxies can be served via direct SSD pread (skip the FUSE round-trip)
- Per-codec ffmpeg flag tuning (R3D needs `-color_trc bt709`, ARRI needs `-pix_fmt yuv420p` explicit, BRAW prefers `-c:v copy` if a Blackmagic decoder exists)
- Progress notification surfaced in the menu bar app ("Generating proxy... 12 / 47")
- Cache eviction policy (LRU when disk pressure detected, currently relies on macOS to clear `~/Library/Caches`)
- ProRes RAW detection via codec id (need to parse MOV atoms — extension alone is ambiguous with regular ProRes)
- MXF detection beyond extension (XAVC vs XDCAM vs MXF-wrapped DNxHD)
- Failure logging + retry with smaller scale on OOM
- Concurrent-Get coalescing (if 5 spacebars hit the same file simultaneously, generate once)

## Demo script (for the eventual demo video)

1. **Setup:** Mount `/Volumes/zpool` via JuiceMount. Show menu bar icon → all green.
2. **Browse to a folder with R3D files.** Finder shows them instantly (cached metadata).
3. **Hit spacebar on a 2.4GB R3D file.** Quick Look opens, smooth playback within 200ms. *"This is a 2.4 gigabyte R3D camera RAW file living in an S3 bucket on the other side of the city. Frame.io Drive's own docs say this is exactly the workload they can't handle."*
4. **Cmd+Shift+F. Type `runner`.** Search returns 47 results across the library. *"Sub-50ms across 130,000 files."*
5. **Spacebar through search results.** Each preview loads in 200-400ms. *"None of these were pre-cached. The proxy generator is keeping up in real time."*
6. **Open Activity Monitor.** Show ffmpeg processes pinned to a fraction of available cores. *"It uses VideoToolbox hardware encoding so the CPU stays free for everything else."*

Total demo time: ~90 seconds for the wow moment, ~3 min for the full sequence including search.

## Performance targets

| Metric | Target | Source |
|---|---|---|
| Cold proxy generation (4K 12-bit R3D, 30s clip) | ≤4s | Apple Silicon M2 Pro VideoToolbox benchmark |
| Warm proxy serve (cache hit) | ≤50ms | SSD random read latency |
| Quick Look open-to-first-frame | ≤200ms total | Quick Look panel + decode + display |
| Concurrent generations without NFS degradation | ≥4 | Bounded worker pool sized to NumCPU/2 |
| Cache footprint per hour of source | ≤500MB at 1280×720 8Mbps | ffmpeg encoder math |

## Risk register

- **Risk:** ffmpeg not on user's PATH. **Mitigation:** Detect at startup, surface clear error, document Homebrew install in MENU_BAR_APP.md.
- **Risk:** R3D source files require RED's libRED for decode. Recent ffmpeg builds include this; older builds may not. **Mitigation:** Document minimum ffmpeg version (5.0+), error gracefully on unsupported codec.
- **Risk:** Disk fills with proxies. **Mitigation:** Use macOS's `~/Library/Caches/` (system-managed eviction) for v1. Add explicit eviction policy in v2.
- **Risk:** Quick Look times out before proxy generates. **Mitigation:** Always serve original on cache miss (Quick Look will at least start showing something even if scrubbing is slow); generate in background; second spacebar is instant.
