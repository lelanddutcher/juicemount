# JuiceMount Spool — Option 2

**Status:** IN PROGRESS — Slice E COMPLETE 2026-05-28 (CI pending) — Slices A-E shipped; spool live runtime + /spool endpoint. Swift menu bar + Manager web UI deferred to follow-on commits.
**Type:** Foundational architecture change
**Scope:** ~1 week implementation + 2–3 days hardening/testing
**Branch:** `production-hardening` (continues on it; not a separate branch)

### Slice progress
- [x] **Slice A** — Spool primitives + SQLite index — COMPLETE 2026-05-28
- [x] **Slice B** — Drainer goroutine — COMPLETE 2026-05-28
- [x] **Slice C** — Write-path integration — COMPLETE 2026-05-28
- [x] **Slice D** — Read-path 3-tier lookup — COMPLETE 2026-05-28
- [x] **Slice E** — Runtime wiring + /spool HTTP endpoint — COMPLETE 2026-05-28 (Swift menu bar + Manager web UI deferred)
- [ ] Slice F — Crash recovery + shutdown semantics
- [ ] Slice G — Integrity hardening
- [ ] Slice H — Preferences + WAN mode polish

---

## 1. Vision

Make `/Volumes/zpool` writes feel like local SSD — like Dropbox Smart Sync / Suite / Lucid — regardless of upstream network speed. Decouple Finder's perception of "write complete" from MinIO upload completion. The architectural answer: **interpose a JuiceMount-owned write spool on local SSD between the NFS handler and JuiceFS FUSE**, so writes ack the moment data is durable on the user's local SSD, with background drain into JuiceFS happening at MinIO's pace.

This is what Suite, Lucid, Shade, Dropbox, and Box Drive all do under the hood. JuiceFS's `--buffer-size`-gated flow control is the unusual design among write-fast cloud filesystems; this proposal works around that design choice without forking JuiceFS.

### Why now

- Live testing on 2 GB / 600 file Finder copies over Tailscale hit 2-hour ETAs (~280 KB/s) because JuiceFS back-pressures on a RAM-tracked budget even though `rawstaging/` has 700+ GB of disk headroom.
- Bumping `--buffer-size` is hostile to our target user (video editors) — they need that RAM for DaVinci/Premiere project caches.
- Slice B of QA-37 (FDPool keyspace split) eliminated the `-36` error class. The throughput cliff is what's left.

### Project-vision alignment

From `project_vision.md`: "Open-source LucidLink for creators." Lucid's write architecture is exactly this pattern — local-disk-durability boundary with async upload. Until we ship this, we're not actually delivering the value proposition for off-LAN workflows.

---

## 2. Non-goals

- **Not** rewriting JuiceFS's flow control. We leave `--writeback` on, leave `--buffer-size` modest, leave MinIO upload to JuiceFS.
- **Not** changing the read cache architecture (cache reader, readahead, memBuf, pin store). Those layers are untouched except for one new lookup tier added in front.
- **Not** building a separate metadata store. Spool index reuses the existing `metadata.Store` SQLite database (new tables).
- **Not** changing the on-disk JuiceFS cache layout or the FUSE mount semantics.
- **Not** building a sync-conflict resolution UI. Writes are append-only-from-clients perspective; no merge logic needed.

---

## 3. Architecture overview

### Current write flow

```
NFS WRITE RPC ──▶ handler.OpenFile(write) ──▶ fdPool.GetWrite ──▶ os.File.WriteAt ──▶ FUSE
                                                                       │
                                                                       ▼
                                                              JuiceFS rawstaging/
                                                                       │
                                                              ┌────────▼─────────┐
                                                              │ uploader queue   │ ◀── --buffer-size
                                                              │ (RAM-tracked)    │     gates here
                                                              └────────┬─────────┘
                                                                       ▼
                                                                     MinIO
```

The WriteAt syscall blocks once JuiceFS's uploader queue (RAM-budget-tracked) exceeds `--buffer-size`. That's the cliff.

### New flow with spool

```
NFS WRITE RPC ──▶ handler.OpenFile(write) ──▶ spool.Open(path) ──▶ writeAt to local file ──▶ ACK
                                                    │
                                                    ▼  on Close:
                                          markReady(entry)
                                                    │
                                                    ▼
                                          ┌──────── drainer ────────┐
                                          │ picks ready entries     │
                                          │ copies into FUSE mount  │
                                          │ verifies SHA-256        │
                                          │ deletes spool file      │
                                          └─────────────────────────┘
                                                    │
                                                    ▼
                                              FUSE → rawstaging → MinIO
                                                              (JuiceFS handles at its pace)
```

### Read flow (modified — 3-tier)

```
NFS READ RPC ──▶ handler.OpenFile(read)
                       │
                       ▼
                spool.LookupActive(path)
                  ├── hit (file still in spool):  read from spool file on local SSD
                  └── miss:  existing path  → cacheReader / readahead / memBuf / FUSE
```

The miss case is the existing hot path — unchanged behavior, ~50 ns added for one in-memory map check.

### Durability boundary

- **Today:** durable when JuiceFS's `rawstaging/` write completes (still inside JuiceFS's domain).
- **Proposed:** durable when our spool file's `fsync()` completes (inside JuiceMount's domain).

Both are local SSD-resident. The spool boundary moves the durability checkpoint one step earlier so we can ACK to Finder before JuiceFS even sees the bytes.

### Integrity discipline

**Mandatory SHA-256 at every hop** per the integrity discussion 2026-05-28:
1. Compute SHA-256 streaming as bytes land in spool (NFS WRITE → spool).
2. Re-verify when drainer reads spool file before WriteAt-to-FUSE.
3. (Optional, defer to slice G) re-verify against JuiceFS object-store SHA after upload.

Cost: ~1 GB/s on Apple Silicon NEON. Negligible vs 80–500 MB/s NVMe write speeds.

---

## 4. File-by-file inventory

### NEW files

| Path | Purpose | Approx. LOC |
|------|---------|-------------|
| `nfs/spool.go` | Spool primitives: `SpoolStore`, `SpoolEntry`, capacity tracking, in-memory index | 400 |
| `nfs/spool_test.go` | Unit tests for spool primitives | 350 |
| `nfs/spool_index.go` | In-memory `map[path]*SpoolEntry` with RWMutex | 120 |
| `nfs/spool_index_test.go` | Lock contention bench, race tests | 150 |
| `nfs/drainer.go` | Background drainer goroutine: picks ready entries, copies to FUSE, SHA-verifies, marks done | 300 |
| `nfs/drainer_test.go` | Drain success/failure/retry tests | 250 |
| `nfs/spool_sha.go` | Streaming SHA-256 helpers for write + verify paths | 80 |
| `metadata/spool_schema.go` | `spool_entries` table DDL, migration | 100 |
| `metadata/spool_store.go` | SpoolStore CRUD on top of `metadata.Store` SQLite | 250 |
| `internal/manager/spool.go` | `/api/spool` control-plane endpoints (list pending, stats) | 150 |
| `app/JuiceMount/Sources/JuiceMount/Core/SpoolStatus.swift` | Swift type for pending-uploads display | 80 |
| `docs/ROADMAP/option-2-spool.md` | THIS DOC | — |

### MODIFIED files

| Path | Lines touched | Change description |
|------|---------------|--------------------|
| `nfs/handler.go:1100-1272` (`OpenFile`) | ~40 added | Add write branch that creates spool entry; return new `spoolWriteFile` type instead of `writeFile` when writing |
| `nfs/handler.go:1100-1230` (`OpenFile` read branch) | ~15 added | Add `spoolIndex.LookupActive(path)` check before falling through to existing cache/FUSE path; if hit, return new `spoolReadFile` type |
| `nfs/handler.go:752-942` (`Stat`) | ~10 added | Prefer spool entry's size/mtime when path is in spool index (same shadowing rule QA-16 established for writeSizes) |
| `nfs/handler.go:968-1006` (`Lstat`) | ~10 added | Same shadowing as Stat |
| `nfs/handler.go:1636-1702` (`writeFile.Close`) | UNCHANGED | Old write path stays as a fallback for slice F-late |
| `nfs/handler.go:32-81` (`JuiceMountHandler` struct) | ~5 added | Add `spool *SpoolStore`, `spoolIndex *SpoolIndex`, `drainer *Drainer` fields |
| `nfs/handler.go:167-184` (`NewHandler`) | ~10 added | Initialize spool components |
| `nfs/handler.go:434-447` (`StopHandler`) | ~5 added | Stop drainer, flush spool index, close spool store |
| `health/fuse.go:240-243` (juicefs mount args) | 2 changed, 0 added | Bump `--max-uploads` 20 → 64 only; leave `--buffer-size` at 4096 (spool handles the burst) |
| `cmd/jm5/main.go:60-130` (boot) | ~15 added | Spool dir flag (`--spool-dir`, `--spool-size`); kick boot-time spool scrubber for crash recovery |
| `bridge/cbridge.go:255-285` (status hooks) | ~10 added | Surface spool pending count to Swift bridge |
| `internal/manager/api.go` | ~5 added | Register `/api/spool` route |
| `internal/manager/overview.go` | ~15 added | Include spool stats in Overview tile fanout |
| `internal/manager/static/index.html` | ~30 added | "Pending uploads" tile + drill-down |
| `internal/manager/static/app.js` | ~50 added | Poll `/api/spool`, render list, progress bars |
| `internal/manager/static/style.css` | ~40 added | Pending-uploads card styling |
| `app/JuiceMount/Sources/JuiceMount/UI/MenuBarController.swift` | ~30 added | Show pending count in menu bar icon (badge) |
| `app/JuiceMount/Sources/JuiceMount/UI/MenuPopoverView.swift` | ~60 added | Pending-uploads section in popover; per-file rows |
| `app/JuiceMount/Sources/JuiceMount/Core/ServerController.swift` | ~25 added | Poll spool status from control plane; publish to UI |
| `app/JuiceMount/Sources/JuiceMount/Core/Preferences.swift` | ~15 added | Spool capacity setting (default 50 GB), spool location setting (default `~/Library/Application Support/JuiceMount/spool/`) |
| `app/JuiceMount/Sources/JuiceMount/UI/PreferencesWindowView.swift` | ~40 added | Spool-config UI panel |
| `app/JuiceMount/Sources/JuiceMount/App.swift` | ~30 added | Graceful-shutdown dialog when spool has pending entries |

### Inventory totals

- **NEW Go:** ~2,000 LOC
- **NEW Swift:** ~80 LOC
- **MODIFIED Go (in-file delta):** ~150 LOC
- **MODIFIED Swift (in-file delta):** ~200 LOC
- **MODIFIED HTML/JS/CSS:** ~120 LOC
- **Test code (included in NEW):** ~750 LOC

Grand total: ~3,300 LOC new + modified, weighted toward NEW.

---

## 5. Slice-by-slice plan

Eight slices. Each slice ships independently green to CI before the next begins.

### Slice A — Spool primitives + SQLite index

**Files:** `nfs/spool.go`, `nfs/spool_test.go`, `nfs/spool_index.go`, `nfs/spool_index_test.go`, `metadata/spool_schema.go`, `metadata/spool_store.go`

**Scope:**
- `SpoolStore` opens a directory, manages capacity, maintains the SQLite-backed index.
- `SpoolEntry` represents one in-flight file: holds an `*os.File` for the spool file, tracks `writtenEnd`, drain state.
- API:
  - `SpoolStore.OpenWrite(path string) (*SpoolEntry, error)` — creates spool file + index row
  - `SpoolEntry.WriteAt(p []byte, off int64) (int, error)` — writes + updates SHA streaming hasher
  - `SpoolEntry.Close() error` — fsync, marks index row `ready`
  - `SpoolStore.LookupActive(path string) (*SpoolEntry, bool)` — O(1) in-memory lookup
  - `SpoolStore.ListReady() ([]*SpoolEntry, error)` — drainer's input
  - `SpoolStore.MarkDone(entry *SpoolEntry) error` — drainer's commit
  - `SpoolStore.Capacity() (used, total int64)`
- SQLite schema:
  ```sql
  CREATE TABLE IF NOT EXISTS spool_entries (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    path         TEXT NOT NULL,
    spool_file   TEXT NOT NULL,
    size         INTEGER NOT NULL DEFAULT 0,
    sha256       BLOB,
    drain_state  TEXT NOT NULL CHECK(drain_state IN ('writing','ready','draining','done','failed')),
    drain_attempts INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    UNIQUE(spool_file)
  );
  CREATE INDEX idx_spool_drain_state ON spool_entries(drain_state);
  CREATE INDEX idx_spool_path ON spool_entries(path);
  ```
- Capacity policy: refuse new `OpenWrite` if used+expected > cap; expected = 0 (we don't know in advance; capacity check happens incrementally on each `WriteAt`).

**Success criteria:**
- All unit tests green under `-race`
- Spool file is fsync'd on Close
- Index survives process restart (boot-time scrubber not in this slice, but state IS persistent)
- SHA-256 computed correctly on write
- Capacity cap is enforced (writes fail cleanly with a typed error when exceeded)

**Reviewer gates:** code-reviewer + tdd-guide. No security-reviewer in this slice (no auth surface).

### Slice B — Drainer goroutine

**Files:** `nfs/drainer.go`, `nfs/drainer_test.go`, `nfs/spool_sha.go`

**Scope:**
- `Drainer` goroutine with bounded worker pool (default 4).
- Loop: `SpoolStore.ListReady()` → pick batch → for each entry: open spool file, copy to FUSE path (`fusePath/<path>`), verify SHA-256 mid-stream, on success `MarkDone` + delete spool file, on failure increment `drain_attempts`, set `failed` after 5 attempts.
- Exponential backoff on transient failures (network errors, FUSE EAGAIN).
- Metrics export: drains/sec, bytes/sec, current queue depth, failure rate.
- Graceful shutdown: signal stop, wait for in-flight drains to complete or hit a 30s deadline.

**Success criteria:**
- Drain success → spool file removed, index row `done`, file present in JuiceFS mount.
- Drain failure → retry up to 5 times with backoff, then `failed` state with `last_error` populated.
- Concurrent drain bound respected (worker pool).
- Shutdown drains in-flight or times out cleanly.
- SHA mismatch → drain marked `failed`, file quarantined, never deleted.

**Reviewer gates:** code-reviewer + go-reviewer (concurrency correctness).

### Slice C — Write-path integration

**Files modified:** `nfs/handler.go`

**Scope:**
- New `spoolWriteFile` type implementing `billy.File` interface:
  ```go
  type spoolWriteFile struct {
      name    string
      entry   *SpoolEntry
      handler *JuiceMountHandler
  }
  // WriteAt → entry.WriteAt; Close → entry.Close + signal drainer
  ```
- `OpenFile` write branch (line 1237–1261) returns `spoolWriteFile` instead of `writeFile`.
- `writeFile` is RETAINED as a fallback for paths that bypass spool (e.g. internal migrator writes via sync.go don't hit NFS).
- **Read path UNCHANGED in this slice.** Files just written via spool are temporarily invisible to reads until drainer copies them to FUSE. Slice C ships with this known limitation; slice D closes it.
- `OpenFile` notifies drainer to wake when a new entry is marked ready.

**Success criteria:**
- E2E test: 1 GB Finder-style copy completes in seconds at local SSD speed; spool fills to ~1 GB; drainer empties it in background within minutes.
- Read after Close: file is briefly invisible (~seconds, until drained) — DOCUMENTED known limitation.
- WRITE p95 latency drops from current 9.26s to <50ms on Tailscale.
- Spool capacity refusal: write that would overflow gets clean NFS3ERR_NOSPC.

**Reviewer gates:** code-reviewer + go-reviewer + e2e-runner (live FUSE test against real Redis+MinIO).

### Slice D — Read-path 3-tier lookup

**Files modified:** `nfs/handler.go`

**Scope:**
- `OpenFile` for read (line 1158): consult `spoolIndex.LookupActive(filename)` first. If hit, return new `spoolReadFile` that serves from the spool file's local SSD. If miss, fall through to existing cachedFile/fuseFD path.
- `Stat` (line 752) and `Lstat` (line 968): when `spoolIndex.LookupActive(filename)` hits, prefer the spool entry's size/mtime over both writeSizes map and SQLite. Same shadowing pattern as QA-16's writeSizes.
- New `spoolReadFile` type: `ReadAt` from `entry.spoolFile`, `Close` releases ref.
- IN-MEMORY INDEX: `map[path]*SpoolEntry` synchronized with the SQLite spool_entries table. Reads use sync.RWMutex; writes (on OpenWrite/MarkDone) take exclusive lock.
- **QA-35 perf gate:** the new lookup MUST add <100 ns to read OpenFile latency at p99 when spool is empty. Bench gate in CI: `BenchmarkOpenFileReadEmptySpool`.

**Success criteria:**
- Read-after-write within drain window: client reads the just-written bytes from spool, identical to source.
- QA-35 perf bench: read OpenFile p99 latency does NOT increase vs slice C baseline by more than 100 ns (within noise).
- Lock contention: `BenchmarkSpoolIndexConcurrent` shows no significant slowdown vs baseline when reads + writes overlap.
- Stat/Lstat consistency: size and mtime reported match spool's writtenEnd in real-time during ongoing writes.

**Reviewer gates:** code-reviewer + go-reviewer + e2e-runner. **This is the highest-risk slice** — QA-35 territory. Reviewer is REQUIRED to gate on the benchmark numbers.

### Slice E — Manager UI + menu bar surfaces

**Files modified:** `internal/manager/api.go`, `internal/manager/overview.go`, `internal/manager/static/{index.html,app.js,style.css}`, `app/JuiceMount/Sources/JuiceMount/UI/{MenuBarController.swift,MenuPopoverView.swift}`, `app/JuiceMount/Sources/JuiceMount/Core/ServerController.swift`

**Files new:** `internal/manager/spool.go`, `app/JuiceMount/Sources/JuiceMount/Core/SpoolStatus.swift`

**Scope:**
- `GET /api/spool` returns:
  ```json
  { "pending_files": 12, "pending_bytes": 3400000000, "in_progress": 4, "failed": 0,
    "entries": [{ "path": "...", "size": ..., "drain_state": "draining", "progress": 0.4 }] }
  ```
- Manager Overview tile shows pending count + bytes; click drills into a list view.
- Menu bar icon shows a small badge with pending count when > 0.
- Menu popover: new "Pending uploads" section with file rows, progress bars, click-to-copy-path.
- Polls 1 Hz when popover is open, 0.2 Hz when closed (badge only).

**Success criteria:**
- Menu badge appears/disappears as drainer fills/empties.
- Manager Overview tile updates in real time.
- "Force drain now" button on Manager works (manual trigger).
- Failed-drain state surfaces with error message in UI.

**Reviewer gates:** code-reviewer. E2E manual smoke test in browser + menu bar.

### Slice F — Crash recovery + shutdown semantics

**Files modified:** `nfs/spool.go`, `cmd/jm5/main.go`, `app/JuiceMount/Sources/JuiceMount/App.swift`

**Scope:**
- Boot-time scrubber (`SpoolStore.RecoverOnBoot`):
  - Scan spool dir, list files
  - Cross-reference with SQLite spool_entries
  - Orphaned file (no index row): delete file, log warning
  - Orphaned row (no file): mark row `failed`, log warning
  - `writing` state row: file may be incomplete from a crash mid-write; mark `failed` (cannot safely resume — NFS client doesn't know to retry)
  - `draining` state row: reset to `ready` so drainer picks it up again
- Graceful shutdown:
  - On `StopHandler`: signal drainer stop, wait up to 30s for in-flight drains
  - If pending entries remain: write status to spool index, exit cleanly
- macOS app shutdown dialog:
  - When user attempts to quit with pending entries:
    - "12 files (3.4 GB) waiting to upload. Wait for upload / Quit anyway (resume next launch) / Cancel"
  - Default: "Wait" with a cancellable progress sheet
- Next launch resumes from where we left off (the index drives the drainer).

**Success criteria:**
- Kill -9 mid-write → reboot → orphaned entry cleaned, no panic.
- Kill -9 mid-drain → reboot → drain resumes, file lands correctly.
- Quit dialog appears and is functional.
- 24-hour soak test: continuous Finder copies + 100 forced restarts, no data loss, no orphan accumulation.

**Reviewer gates:** code-reviewer + tdd-guide.

### Slice G — Integrity hardening

**Files modified:** `nfs/spool.go`, `nfs/drainer.go`, `nfs/spool_sha.go`

**Scope:**
- SHA-256 is COMPUTED on write (slice A) and VERIFIED on drain (slice B). This slice adds:
  - After drainer's WriteAt-to-FUSE completes, compute SHA-256 of the data we wrote (by reading back through FUSE — yes, this is a real cost).
  - Compare to the stored SHA from spool.
  - If mismatch: don't delete spool file; mark entry `failed`; surface to UI with explicit "integrity mismatch" error.
  - Quarantine flow: failed-integrity entries move to `spool/quarantine/` for manual inspection.
- Streaming SHA via `crypto/sha256` (NEON-accelerated on Apple Silicon).
- Manifest log: append `(path, sha256, timestamp)` to a tamper-evident log for audit (`spool/manifest.log`).

**Success criteria:**
- Inject a bit flip into spool file before drain → drainer detects, refuses delete, quarantines, surfaces error.
- SHA verification overhead < 5% of drain time on M2 Pro.
- Manifest log entries are append-only and timestamped.

**Reviewer gates:** code-reviewer + security-reviewer (for the manifest + quarantine flow).

### Slice H — Preferences + WAN mode polish

**Files modified:** `app/JuiceMount/Sources/JuiceMount/Core/Preferences.swift`, `app/JuiceMount/Sources/JuiceMount/UI/PreferencesWindowView.swift`, `health/fuse.go`, `cmd/jm5/main.go`

**Scope:**
- Preference: spool capacity (slider, 10–200 GB, default 50 GB)
- Preference: spool location (defaults to `~/Library/Application Support/JuiceMount/spool/`, user can override)
- Preference: "WAN mode" toggle that bumps `--max-uploads` 20 → 64 on the JuiceFS side (defer the actual bump to when this is toggled; default off)
- Preference: "Quit behavior when pending uploads" (Wait / Resume next launch / Ask each time)
- jm5 CLI flags: `--spool-dir`, `--spool-size-gb`
- Preferences UI panel: "Sync & Upload" tab

**Success criteria:**
- Preferences round-trip cleanly to disk.
- Changing spool location triggers safe migration of existing entries.
- WAN-mode toggle changes JuiceFS args on next mount.

**Reviewer gates:** code-reviewer.

---

## 6. Testing infrastructure

### Local test harness

Existing test setup (Docker-compose with Redis + MinIO) extends to spool tests. Add:

**`scripts/test-spool-harness.sh`**
- Starts ephemeral Redis + MinIO containers
- Starts jm5 with `--spool-dir=$(mktemp -d) --spool-size-gb=10`
- Mounts NFS to a temp directory
- Returns env vars for downstream test scripts

**`scripts/bench-spool-throughput.sh`**
- Copies a deterministic test corpus (100 files × 10 MB each, plus 10 files × 100 MB each)
- Measures: time-to-ack from Finder perspective, time-to-drain, peak spool size
- Outputs JSON for regression tracking

**`scripts/qa-suite/30-spool-soak.sh`**
- 1-hour continuous Finder copy
- Random kill -9 of jm5 every 5 minutes
- Restart, verify all files eventually land in MinIO with correct SHA
- Exit non-zero on any data loss

### CI integration

`.github/workflows/spool-tests.yml`:
- Runs on every push to `production-hardening`
- Runs all `nfs/spool*_test.go` and `nfs/drainer*_test.go` under `-race`
- Runs `scripts/bench-spool-throughput.sh` with a fixed corpus and asserts:
  - Time-to-ack < 100 ms per file (slice C bar)
  - Drain throughput > 80% of raw MinIO PUT throughput
- Runs `scripts/qa-suite/30-spool-soak.sh` (10-minute mini-soak in CI; full 1-hour soak is nightly)
- Blocks merge if any of the above fail

### Performance regression gates

Three benchmark gates added to CI:

1. `BenchmarkOpenFileReadEmptySpool` — slice D's QA-35 gate. Must show <100 ns delta vs HEAD baseline.
2. `BenchmarkSpoolIndexConcurrent` — 64 concurrent readers + 8 writers on the index, no >10% slowdown vs baseline.
3. `BenchmarkSpoolDrainThroughput` — drain ≥ 200 MB/s on M2 Pro test runner.

### Test fixtures

- **Deterministic corpus:** `testdata/spool-corpus/` — 100 small files (10 MB) + 10 large (100 MB) with known SHA-256 values committed to repo.
- **Chaos test:** `nfs/spool_chaos_test.go` — uses `gofakeit` to randomly inject failures (FUSE EAGAIN, SHA mismatch, capacity exhaustion, kill signal) and asserts the spool stays consistent.

### Hardware

Existing TrueNAS infrastructure serves as the MinIO/Redis backend for E2E tests. No new hardware required. Mac runner is the development machine + a future Mac mini in the test rack (out of scope for this plan; document as a "nice to have" follow-up).

---

## 7. Sub-agent allocation matrix

| Slice | Implementation agent | Reviewer agent(s) | E2E agent |
|-------|----------------------|-------------------|-----------|
| A | `claude` (general) | `code-reviewer` + `tdd-guide` | — |
| B | `claude` | `code-reviewer` + `go-reviewer` | — |
| C | `claude` | `code-reviewer` + `go-reviewer` | `e2e-runner` |
| D | `claude` | `code-reviewer` + `go-reviewer` + **MUST** include QA-35 perf-discipline review | `e2e-runner` |
| E | `claude` | `code-reviewer` | manual UX smoke |
| F | `claude` | `code-reviewer` + `tdd-guide` | `e2e-runner` (crash test) |
| G | `claude` | `code-reviewer` + `security-reviewer` | `e2e-runner` (corruption injection) |
| H | `claude` | `code-reviewer` | — |

Every slice MUST end with 0 CRITICAL/HIGH findings from reviewers before commit. Same gate the manager work used.

---

## 8. Risk register

| Risk | Likelihood | Severity | Mitigation |
|------|------------|----------|------------|
| Read-path lookup regresses QA-35 perf budget | Medium | High | Slice D perf bench gate in CI; QA-35 perf-discipline reviewer required |
| Lock contention on `spoolIndex` map | Medium | Medium | sync.RWMutex; if hot, swap to sync.Map; bench in slice D |
| Bit flip between spool and FUSE | Low | Catastrophic | Mandatory SHA-256 verify at every hop (slice G) |
| Spool fills, blocks writes | Medium | Low | Clean NFS3ERR_NOSPC; "Force drain now" + capacity slider in UI |
| Crash mid-write leaves partial spool file | High | Low | Slice F scrubber marks `writing` rows as `failed` and cleans up |
| User quits with pending uploads | High | Medium | Slice F shutdown dialog; resume on next launch |
| Drainer goroutine deadlock | Low | High | Bounded worker pool + 30s shutdown deadline; race tests |
| Index/file disagreement after disk corruption | Low | Medium | Boot-time scrubber reconciles |
| Migration of existing in-flight writes when shipping | Low | Medium | Ship behind a feature flag for one release cycle (`JM_SPOOL_ENABLE=1`); flip to default-on after a week |
| Read of in-flight write returns torn data | Low | High | Slice D: locking + spool entry exposes `writtenEnd` ≤ `Read` cap |

---

## 9. Rollback strategy

The spool is gated by `JM_SPOOL_ENABLE` env var for the first release. Set to 0 → handler reverts to the existing `writeFile` path; spool dir is unused; no behavior change.

If a critical bug surfaces post-release, ops sets `JM_SPOOL_ENABLE=0` on TrueNAS app env and restarts. Spool files persist but go un-drained (manual recovery procedure documented in `docs/RUNBOOK/spool-recovery.md` — slice F deliverable).

After 1 month of clean operation, the env var defaults to on and the rollback option remains for another quarter.

---

## 10. Acceptance criteria (feature-complete)

The feature ships when ALL of the following hold:

1. ✅ All 8 slices' CI gates green
2. ✅ 1 GB Finder copy ack'd in <10 s wall-clock from start, regardless of network mode (LAN or WAN)
3. ✅ Background drain completes within MinIO-bandwidth-bounded ETA (matches today's existing pattern)
4. ✅ Read-after-write within drain window returns correct bytes (verified via SHA)
5. ✅ Kill-restart preserves all written data
6. ✅ QA-35 perf bench: no regression vs HEAD-before-spool baseline
7. ✅ Menu bar + Manager UI surfaces pending state correctly
8. ✅ 24-hour soak test green
9. ✅ DaVinci playback regression test green (4K MP4 read during 500 MB parallel write — user's specific concern)
10. ✅ Manual smoke test on Tailscale + LAN both pass

---

## Appendix A — Loop statement (for next agent invocation)

A separate file `docs/ROADMAP/option-2-spool-LOOP.md` contains the exact prompt to fire as the multi-day autonomous loop driver. Each tick advances exactly one slice through implementation → review gate → commit → push → CI → next slice.

See that doc for the prompt.
