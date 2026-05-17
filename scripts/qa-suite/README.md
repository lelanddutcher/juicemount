# JuiceMount QA Suite

A 3-hour pressure-test suite for JuiceMount that combines:
- Real-world Finder/media-editor workloads
- Synthetic IO benchmarks via `fio`
- Concurrency stress tests
- Failure injection (kill juicefs, toggle offline)
- macOS network shaping (dnctl/pfctl: bandwidth, latency, packet loss, full block)
- 20-minute endurance loop with memory/fd leak detection
- HTTP control-plane stress

## Phases

| # | Name | Target | What it covers |
|---|------|--------|----------------|
| 00 | precheck | 3 min | Tool availability, mount health, baseline metrics, generate shared 256 MiB random pool |
| 01 | smoke | 5 min | Existing `write-integrity.sh` (8 cases) + basic file ops |
| 02 | finder | 20 min | `cp -p`, `cp -R`, `mv`, Spotlight stat-storm, Quick Look pattern, rsync, 1000 small files |
| 03 | media | 25 min | Sequential whole-file playback, random ±1 MiB scrubbing, 20-file bulk import, render-while-reading, ffprobe storm |
| 04 | fio | 40 min | randread-4k, randwrite-4k, seqread-1m, seqwrite-1m, mixed-7030, randread-1m, parallel-write, parallel-randmix |
| 05 | metadata | 15 min | 5000-file flat dir, readdir storm, stat storm, deep tree, find, 8-way concurrent stat |
| 06 | concurrency | 25 min | 16 parallel writers, 32 readers across 16 files, mixed 8R+8W, churn loop, tar archive round-trip |
| 07 | failure | 20 min | SIGSTOP juicefs daemon, cancel mid-copy, pin/unpin cycle, force /sync |
| 08 | netshape | 20 min | 5 Mbps cap, 200 ms latency, 5% packet loss, full backend block (auto-offline trigger + recovery) |
| 09 | endurance | 25 min | 4 writers + 4 readers for 20 min, samples RSS / fd / RPC counters every 10s, fails on >50% RSS growth |
| 10 | control-plane | 10 min | All HTTP endpoints (`/health`, `/metrics`, `/offline`, `/cache/status`, `/selftest`, `/sync`, `/pin`, `/unpin`, `/search`), 100-way parallel `/health` hammer |

Total: ~3 hours wall-clock (each phase has guard timeouts).

## Quick start

```bash
# Full 3-hour run
bash scripts/qa-suite/run-all.sh

# Shorter validation pass (skips fio, shortens endurance)
QUICK=1 bash scripts/qa-suite/run-all.sh

# Specific phases only
PHASES="01-smoke 06-concurrency" bash scripts/qa-suite/run-all.sh

# Halt at first failing phase
STOP_ON_FAIL=1 bash scripts/qa-suite/run-all.sh
```

Artifacts land in `/tmp/jm-qa-artifacts/<run-id>/`.

## Prerequisites

Required: `curl`, `python3`, `md5`, `stat`, `dd`, `lsof` (all built-in on macOS), `fio` (`brew install fio`).

Optional: `ffmpeg` for media tests, `rsync` for incremental-sync test, `tar` for archive round-trip. Missing optional tools log `[WARN]` and skip their test.

Phase 08 (netshape) needs **passwordless sudo** for `pfctl` and `dnctl` — if sudo prompts for a password, the phase warns and skips.

## Env knobs

| Var | Default | Purpose |
|-----|---------|---------|
| `MOUNT` | `/Volumes/zpool-dev` | NFS mount path to test against |
| `JM_METRICS_ADDR` | `127.0.0.1:11050` | JuiceMount HTTP metrics endpoint |
| `BACKEND_HOST` | `192.168.0.197` | Backend MinIO IP (for shaping target) |
| `BACKEND_PORT` | `9000` | Backend MinIO port |
| `ENDURANCE_DURATION` | `1200` | Endurance phase length in seconds (1200 = 20 min) |
| `ARTIFACTS_ROOT` | `/tmp/jm-qa-artifacts` | Artifact root |
| `RUN_ID` | timestamp | Override the dated artifact subdir name |

## Pass/fail semantics

Each phase tracks pass/fail counters. The orchestrator:
- ALWAYS continues to the next phase even if a phase has failures.
- Restarts JuiceMount between phases if the mount went unhealthy.
- Aggregates a final `run.summary` table at the end.

A failure in any phase means **investigate but the test continues** — the
goal is to catch as many issues as possible per run, not to bail early.
Use `STOP_ON_FAIL=1` if you'd rather halt at the first failure.
