# Prototype 02 — Content-Hash Backup Verification

> **Branch:** `prototype/backup-verification`
> **Status:** Working core engine (iteration 5). UI integration into the menu bar app is the next step.
> **Source spec:** `VISION/feature-roadmap-ranked.md` § Top tier #2
> **Estimated to production-ready:** 3-4 weeks of focused work + UI shell.

## What this prototype proves

Editors can finally answer the question they actually want to know: **"Are my backups real?"**

Not "did the rsync exit with status 0" (because half the time the rsync didn't even see the file that just got moved). Not "did Backblaze report success" (because Backblaze can't tell you the file isn't actually the bytes you think it is). Not "I checked it last quarter" (because nobody actually does).

Real verification: walk every backup target, compute content hashes, compare them to the source library, surface a traffic-light status per file:

- 🟢 **GREEN** = ≥3 verified copies exist with matching hashes
- 🟡 **YELLOW** = 1-2 verified copies (one drive failure away from loss)
- 🔴 **RED** = 0 verified copies OR a hash mismatch detected (silent corruption)

Plus the killer feature: a **"safe to delete"** check. The system refuses to delete a file unless ≥2 verified copies exist on independent targets. Toy Story 2 trauma, solved.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  internal/verify (NEW package)                          │
│                                                          │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Target interface                               │    │
│  │  - Walk(ctx) → channel of (path, size, mtime)   │    │
│  │  - Hash(path) → sha256                          │    │
│  │  - Identifier() → "local:/Volumes/zpool"        │    │
│  └─────────────────────────────────────────────────┘    │
│         ▲              ▲              ▲                 │
│         │              │              │                 │
│   ┌─────────┐   ┌─────────┐   ┌──────────┐             │
│   │ Local   │   │ S3      │   │ NAS over │             │
│   │ FS      │   │ (B2/R2/ │   │ SMB/NFS  │             │
│   │ target  │   │  AWS)   │   │ target   │             │
│   └─────────┘   └─────────┘   └──────────┘             │
│                                                          │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Manifest                                       │    │
│  │  - JSON file at ~/Library/Application Support/  │    │
│  │    JuiceMount/manifest.json                     │    │
│  │  - records: path → []TargetVerification         │    │
│  │  - TargetVerification = {target, hash, ok,      │    │
│  │    verifiedAt}                                  │    │
│  └─────────────────────────────────────────────────┘    │
│                                                          │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Manager                                        │    │
│  │  - VerifyAll(ctx, targets) → walks each, hashes │    │
│  │  - Status(path) → green/yellow/red + details    │    │
│  │  - SafeToDelete(path) → bool + explanation      │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Key architectural decisions

1. **Target as an interface.** Local filesystem, S3-compatible (Backblaze, R2, AWS, MinIO), SMB/NFS-mounted NAS, eventually LTO via `tar -tvf`. Each target implements the same Walk + Hash + Identifier surface. The Manager doesn't know what's behind the target.

2. **SHA-256 hash, content-only.** Not blake3 (less universal), not size-only (silent corruption), not mtime-only (broken on copies). Sha-256 is the verifier-of-record across the industry — Backblaze publishes them, AWS uses them for ETag in some cases, every Unix tool computes them. Future tier may add blake3 for speed.

3. **Manifest as JSON file, not SQLite.** For prototype simplicity. Production: migrate to SQLite as a `verifications` table alongside the existing `entries` schema.

4. **Verification is a checkpoint, not a daemon.** The user runs verification on a schedule (cron, launchd, or "Verify Now" button in the menu bar app). Long-running daemon adds complexity; explicit run + cached results is the right primitive.

5. **Hash on read (memory-streaming).** Walk a target with `io.Copy(sha256, file)` — never load the full file into memory. Multi-TB libraries verify on commodity machines.

6. **Per-target byte-range optimization (future).** S3 supports `If-Match` with ETag. We can skip re-hashing if the ETag matches our last record. Saves bandwidth on terabytes. Not in prototype — future work.

7. **Concurrent target verification, sequential within target.** Each target gets its own goroutine; within a target, files are walked serially to avoid overwhelming spinning disks or S3 rate limits.

## Files added in this prototype

```
internal/verify/
├── target.go      # Target interface + common types
├── local.go       # Local filesystem target (walks a directory)
├── manifest.go    # JSON manifest persistence
├── manager.go     # Cross-target verification + status calculation
├── target_test.go
├── local_test.go
├── manifest_test.go
└── manager_test.go

cmd/verify-smoke/
└── main.go        # CLI smoke test
```

## What's working in this scaffold

- Target interface with Local implementation (filesystem walker + sha256 hasher)
- JSON manifest persistence with atomic write (.tmp + rename)
- Manager that runs Verify across all configured targets and updates the manifest
- Status calculation: per-file traffic light based on verification count and hash agreement
- SafeToDelete check that returns false unless ≥2 independent targets verify the same content
- Concurrent target verification with bounded parallelism
- Smoke test CLI that exercises the full workflow end-to-end

## What's still TODO before production

- **S3 target implementation.** Pattern is there (Target interface), but actual `aws-sdk-go-v2` integration is its own work item. Use B2/R2/Wasabi SDKs.
- **SMB/NFS target.** Mostly handled by Local target if the share is mounted. Real SMB target would talk SMB protocol directly to avoid mount overhead.
- **Migrate manifest from JSON to SQLite.** When the file count crosses ~50K entries, JSON load/save gets slow. SQLite + indexed lookup needed.
- **UI in menu bar app.** A "Backups" tab in the popover showing aggregate status (e.g., "47 files at risk: 12 yellow, 35 red") + a per-file drilldown.
- **Schedule integration.** Run verification weekly/daily via launchd; surface status in the menu bar icon (badge color when red).
- **ETag-based fast-path for S3.** Skip re-hashing files whose S3 ETag hasn't changed since last verification.
- **Notification when status drops.** "3 files moved from green to yellow" → user notification.
- **Bonus: snapshot-aware deletes.** Before deleting, check if the file appears in a Time Machine snapshot. That's a verified copy.

## Demo script (the eventual demo)

1. **Setup:** A populated `/Volumes/zpool` library. A second drive (USB) at `/Volumes/backup`. A B2 bucket already configured.
2. **Open the menu bar popover → "Backups" tab.** Show: 14,832 files, 12,401 green, 1,902 yellow, 529 red.
3. **Click red.** Drill into the 529 files. Filter to "Footage/Master Selects." Show: these 47 files have 0 verified backup copies.
4. **Hit "Verify Now."** Watch the worker walk the local source + USB + B2 in parallel. Status updates live.
5. **Most of the 47 jump to green.** A few stay red — show the diff: file size on USB matches but hash doesn't. *"Silent bit-rot. Your file rotted on the USB drive 7 months ago. The Finder thinks it's fine."*
6. **Right-click a green file → "Safe to delete?"** Get green response: "Yes — 4 verified copies on 3 targets."
7. **Right-click a red file → "Safe to delete?"** Get red response with explanation: "No — 0 verified copies. Source is the only known copy."
8. **(Editor's eyes well up with relief.)**

Total demo time: ~2 minutes for the wow moment.

## Performance targets

| Metric | Target | Source |
|---|---|---|
| Walk + hash 100K-file library on local NVMe | ≤4 minutes | sha256 on M2 Pro = ~2.5 GB/s; even at 1MB avg = 40s of CPU |
| Walk + hash 100K-file library on B2 (cold) | ≤6 hours | Bottlenecked by S3 GET throughput per object |
| Manifest load + status calc for 100K entries | ≤200ms | JSON load + map iteration; SQLite migration comes here |
| Concurrent target count without thrash | ≥4 | Goroutine per target, no contention on different storage |
| SafeToDelete query | ≤1ms | manifest lookup + count |

## Risk register

- **Risk:** Hashing terabytes is slow even on NVMe. **Mitigation:** Incremental verification (only re-hash files whose mtime/size changed since last run). For S3, use ETag fast-path.
- **Risk:** B2/AWS rate limits during Walk. **Mitigation:** Configurable concurrency per target; default conservative (4 parallel GETs per target).
- **Risk:** False-positive hash mismatches on actively-being-written files. **Mitigation:** Skip files modified within the last 60 seconds of the verification run.
- **Risk:** Manifest gets large. **Mitigation:** Migrate to SQLite at 50K entries.
- **Risk:** User deletes the manifest, all history is lost. **Mitigation:** Verification re-derives state on next run (idempotent design — manifest is a cache, not the truth).
