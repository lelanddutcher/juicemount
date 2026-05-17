# Loop B — Plan (post-QA loop, 2026-05-17)

After Loop A closed the QA backlog (10 goals, 6 commits landed-needs-
validation + 3 environmental closures + 1 tier-3 foundation), the
next pass picks up the items that move JuiceMount toward production
readiness on the dimensions Loop A couldn't address:

- self-host completeness (tier-3 still half-built)
- UX honesty (visible state, self-explaining failures)
- background observation (24h soak the watcher was built for)
- adjacent tier-4 wins (bandwidth probe)

This doc is the canonical plan for Loop B. The loop statement at
the bottom references it; each iteration reads here for slice
boundaries and acceptance criteria.

## Goals in priority order

### B.1 — Tier-3 iter 2: `juicemount-server` container (~3h)

**Why first:** completes the docker-compose foundation Loop A iter
A.8 started. Until this lands, the stack only provides backing
services — users still need their Mac running JuiceMount.app to
do the NFS export. With this container the stack becomes truly
deployable: Synology / TrueNAS / Hetzner / Raspberry Pi NAS can
serve the volume to multiple clients without any Mac involvement.

**Slice:**
1. `server/juicemount-server/Dockerfile`: builds `cmd/jm5` for
   linux/amd64 in a multi-stage build (Go builder + minimal
   distroless runtime).
2. `server/docker-compose.yml`: new `juicemount-server` service with
   - depends_on: minio + redis healthy AND juicefs-init exit 0
   - ports: 2049 (NFS), 11050 (admin API)
   - healthcheck: GET /health expects 200
   - volumes: bind-mount for SQLite metadata + pin store
3. `server/README.md`: update quick-start to point clients at the
   container's NFS export (vs the Mac app's local export).

**Acceptance test:** `docker compose up -d` brings up all four
services; `curl http://localhost:11050/health` returns 200 with
all components OK; macOS can `mount -t nfs localhost:/zpool /Volumes/test`.

**Validation:** I can build the image; user starts Docker to run it.

**Files touched:** `server/Dockerfile` (new), `server/docker-compose.yml`,
`server/README.md`, `cmd/jm5/main.go` (only if a Linux-specific
flag is needed; expect not).

---

### B.2 — Tier-2: self-test dashboard in popover (~2h)

**Why second:** Loop A's QA pattern was "user can't tell if it's
actually working" — three QAs (QA-1, QA-2, QA-5) turned out to be
environmental issues that LOOKED like product bugs because the
popover doesn't surface backend health visibly. A self-test pane
fixes that.

**Slice:**
1. New `SelfTestSection` in MenuPopoverView. Reads existing
   /metrics + /health + /offline endpoints already polled at 2s.
2. Display:
   - 4 health dots (Redis / MinIO / FUSE / NFS) with last-check
     latency next to each
   - Rolling 30s read-throughput from `bytes_read` delta
   - Cache hit rate from existing metrics
   - Click any row → copies a diagnostic snippet to clipboard
     (timestamp + the four values + JM version)
3. Collapsible by default — sits between cacheCounts and rootsList,
   adds maybe 60px of popover height when expanded.

**Acceptance test:** popover opens, four green dots visible; toggle
Wi-Fi off, Redis dot goes red within 5s; export-on-click produces
copyable plaintext.

**Validation:** osascript open popover + screencapture before/after
Wi-Fi toggle; visual diff.

**Files touched:** `MenuPopoverView.swift` (add component + integrate).

---

### B.3 — Tier-2: self-explaining errors (~2h)

**Why:** every `showAlert(title:"X failed", message: error.localizedDescription)`
in the codebase produces a useless dialog. Replace with a remediation
template that includes: what failed, why it likely failed (top 2-3
causes), what command to try, what to copy when filing an issue.

**Slice:**
1. New `RemediationAlert` helper in a new
   `app/JuiceMount/Sources/JuiceMount/UI/RemediationAlert.swift`.
   Takes an error category + raw error, produces a structured dialog
   with: Title, Cause (best guess), Try this (concrete step),
   Copy diagnostic (button that copies the raw error + JM version).
2. Audit existing `showAlert` call sites (Pin failed, Reclaim failed,
   Clear cache failed, etc) — replace with RemediationAlert and a
   category-specific message bank.

**Acceptance test:** trigger one error path (e.g. /pin against an
unreachable backend); dialog shows three sections, not just the raw
NSError string.

**Validation:** osascript trigger via the Services menu + screencapture.

**Files touched:** new RemediationAlert.swift, sites currently
calling showAlert in MenuPopoverView.swift, MenuBarController.swift
(if any), maybe PreferencesWindowView.swift.

---

### B.4 — Tier-1.6: kick off 24h soak in background (~5min code, 24h wait)

**Why parallel:** soak runs in the background while other items
ship. The watcher (iter 11) was built for exactly this. Closes a
real tier-1 acceptance gate that's been gated on "find a quiet 24h
window" for sessions now.

**Slice:**
1. Verify-build --running confirms fresh binary alive
2. Launch with nohup + disown:
   ```
   /tmp/jmstress-bin --mount /Volumes/zpool-dev --duration 24h \
     --finder-workers 4 --nle-workers 2 --backup-workers 1 \
     --discovery-depth 6 --large-file-min-mb 200 \
     --json --periodic-json 60s \
     --goroutine-warmup 5m --goroutine-tick 60s \
     > /tmp/jm-soak-24h.jsonl 2>&1 &
   ```
3. Periodic check (every wake, ~30 min): `tail -1` the JSONL, scan
   for goroutine breaches or error bursts.
4. At T+24h: assert finder.stat.p99 < 500ms, nle.errors < 10,
   breaches == 0. If pass → tier-1.6 ✓. If fail → triage.

**Acceptance test:** 24h soak completes with thresholds met.

**Validation:** the watcher's own anomaly flags + the assertions above.

**Files touched:** none — pure execution against existing tools.

---

### B.5 — Tier-3 iter 3: Caddy reverse-proxy + admin-key middleware (~2h)

**Why after B.1:** Caddy needs something to proxy to. Once
juicemount-server is in the compose, Caddy adds TLS termination +
admin-key auth + a single user-facing port (443) instead of the
sprawl of 9000/9001/6379/11050/2049.

**Slice:**
1. `server/Caddyfile`: terminates TLS on :443, routes:
   - `/` → MinIO console (9001)
   - `/api/*` → juicemount-server admin (11050) with admin-key check
   - WebSocket upgrade for the eventual admin UI
2. `server/docker-compose.yml`: new `caddy` service with cert volume
   + Let's Encrypt staging by default (production via .env flag).
3. `.env.example`: new `ADMIN_KEY` field, generate with openssl.
4. README: TLS quickstart for production hosts with a DNS name.

**Acceptance test:** `curl -k https://localhost/api/health` returns
401 without admin-key, 200 with the right one.

**Files touched:** `server/Caddyfile` (new), `server/docker-compose.yml`,
`server/.env.example`, `server/README.md`.

---

### B.6 — Tier-4: bandwidth probe on launch (~2h)

**Why last in this batch:** independent backend code, lower per-hour
leverage than B.1/B.2/B.3, but a clean win once the higher-priority
items ship. Measures RTT + throughput to MinIO at Start time, stores
a per-network baseline.

**Slice:**
1. New `health/bandwidth.go`: probe runs once at Start, after FUSE
   mounts and before declaring server ready. Issues a small (1 MB)
   GET against MinIO, times it; record RTT + MB/s in metrics.
2. New `/bandwidth` admin endpoint that returns the latest probe
   result.
3. Popover surfaces it next to the existing cache row when known.

**Acceptance test:** /bandwidth returns numbers within 5s of Start;
the popover shows "Backend: 230 Mb/s · 8ms RTT" (or similar).

**Files touched:** `health/bandwidth.go` (new), `bridge/cbridge.go`
(route + Start integration), `MenuPopoverView.swift` (display).

---

## Loop ordering

B.4 (24h soak) starts FIRST and runs in background. While it ticks,
the foreground iterations are: B.1 → B.2 → B.3 → B.5 → B.6. Each is
one slice / one commit per the established rules.

## Mandatory checks per iteration

Same as Loop A:
- code-reviewer on any request-path / mount-lifecycle / metadata /
  cache / admin-endpoint / UI-talks-to-bridge commit
- specific file paths to git add — never `git add -A`
- `go vet + go test -race` on touched packages
- STATE.md update marking the slice ⚠ landed-needs-validation
- ScheduleWakeup 60–120s; pivot to next goal on degraded mount

## STOP conditions

- All 6 goals ✓ or ⚠ in STATE.md
- OR three consecutive iterations produce no shippable progress
- OR user explicitly halts
- On stop: PushNotification with one-line outcome + next action
