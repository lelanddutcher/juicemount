# Farm Tab sprint — from read-only coverage to a governed, legible derivative pipeline

Scoped from a 5-specialist pass grounded in the real manager + farm code. Extends
`MANAGER_INTEGRATION.md` (Phase-1 read-only coverage is shipped). Addresses Leland's
five asks (educate · CPU lanes · file-size estimates · proxy-scope/size-delta · directory
whitelisting) and folds in the highest-leverage surfaced extras.

**Design spine:** Phases 1–3 are ALL read-only (relay/measure) so they ship with **zero new
endpoints and no farm control plane**. The one new control path (launching a sweep) is isolated
to Phase 4, the keystone everything that *sets* a knob depends on.

---

## MVP (ships behind the admin key, no rebuild)
The **education + transparency** slice over the JSON `/api/farm` already returns + the existing
throttle env: a "What is the farm?" explainer, per-kind **process-provenance** rows ("proxy →
ffmpeg libx264 -crf 21 -preset slow", "ai → whisper.cpp medium.en"), the last sweep as a
plain-English sentence, coverage as "142 of 150 proxies ready (95%) — 8 to do", and a **read-only
knob inspector** showing the live governor. One S-effort farm add earns its place even here: stamp
the *resolved* governor values + live vcodec/crf/preset/model into `farm-status.json` so the
inspector shows real values (degrades to documented defaults if it lands a beat later).

## Decisions locked (Leland, 2026-06-25)
- **Pre-generation = OFF volume-wide by default.** A folder is **opted IN per directory** (whitelist),
  with **whitelistable subfolders** inside an opted-in tree. So Phase 5 leads with an **opt-in `.juicefarm`
  marker** (or a manager-stored whitelist), NOT a blacklist. **Rationale (load-bearing):** DaVinci/Premiere
  write their OWN proxies into the project tree — we must NOT make *proxies of proxies*. Those NLE proxy
  folders won't be opened in OpenLoupe, and OpenLoupe already filters those file types, so default-off + opt-in
  is the safe shape (a giant folder landing on the volume never triggers a surprise full-volume sweep either).
- **Proxy skip-threshold = a user-exposed setting** (NOT a hardcoded default). It's something to experiment
  with, so surface the saving-% cutoff as an editable field (global default + per-folder override), backed by
  the Phase-3 estimator + actual-size radar that calibrate what a sane number is.
- **Triggering = scheduled + manual, Jellyfin-style.** Model the UX on open-source media managers (Jellyfin
  library scans): a **"Scan / generate now"** manual button AND a **scheduled** recurring sweep, via the
  manager's existing `/api/schedules` framework. The farm stays a thin one-shot the schedule/button launches.

## Phase 1 — Educate & expose *(read-only, zero new endpoints)* — S–M
Make a non-engineer understand what runs, on what, how covered they are, under what governor.
- Plain-language "What is the farm?" panel
- Per-pass "what process this runs" provenance rows
- Last-sweep summary in human sentences
- Per-kind coverage as plain-English health + micro-bar
- Sweep-mode / pass legend
- **Read-only knob inspector** (the read half of "CPU lanes" — surfaces the existing throttle env before any write path)

## Phase 2 — Live activity *(read-only, one thin relay)* — M
- Live sweep activity feed via the `since=` delta poll (new: a same-origin manager relay `GET /api/farm/changes`, mirror of `handleFarm` — NOT a farm control endpoint)
- "Transcribing clip 7 of 14, ~2 min left" progress + ETA (degrades to count-up + throughput when no sweep total exists)
- Live CPU/mem panel via `docker stats` (reuse Overview's bounded-probe pattern; degrades to "load unavailable")

## Phase 3 — Proxy economics, read-only *(the size-delta truth)* — M
The answer to "estimated file sizes" + the read-only half of the near-same-size-proxy worry.
- **Unblocker (S):** map `probe.go`'s already-parsed `format.bit_rate` onto `Tech`/video (it's currently discarded) — the estimator's missing input
- Per-codec proxy-size **estimator** (dry-run column) — rides the existing `-dry-run` path
- Record **actual** proxy size + delta on the row (os.Stat → the `extra` JSON column, FilmstripGeo precedent)
- **Proxy-bloat radar** — batch view flagging clips where proxy ≥ X% of original (the iPhone/h264 case)
- Folder-level proxy-economics summary card
- *(P3 follow-up):* the bloat radar identifies clips by **inode** today (proxy rows store no source path) — store the source basename on the proxy row (the `extra` column) so the radar shows **filenames**, making it actionable

## Phase 4 — The sweep-trigger linchpin *(the one new control path)* — ✅ TRIGGER BUILT (2026-06-25)
**DECISION (Leland): farm-queue endpoint, NOT docker-socket** (the production manager has no docker-socket
access; the queue also serves the OpenLoupe FARM_CONTROL_SURFACE — built once, two producers). Shipped as a
shared Redis job queue (`internal/farmqueue`) the manager enqueues to (`POST /api/farm/sweep` + `GET
/api/farm/jobs`, Farm-tab "Generate on the farm" UI) and a **standing worker** drains (`jmfarm -queue`,
`JM_FARM_QUEUE=1`). Smoke-verified end-to-end with real generators (commit fea9e82, local-only). Remaining
Phase-4 knobs below ride this surface; standing-worker production rollout is the next step.
- Sweep trigger + tracked job (the path that makes every SET knob real)
- **CPU lane allocator** — "give the farm up to N cores" + separate proxy lane (the write half of "CPU lanes")
- Quality dials — CRF/preset + whisper model + which passes
- **Dedup / skip-already-done** (GenerateProxy re-encodes unconditionally today — real cost+correctness fix; precondition for safe scheduling)
- Coverage-gaps + **retry-failed** scoped via the existing `-files` flag
- *(Add here, surfaced by the adversarial pass:)* **failure legibility** — stamp the error string into the row's `extra` column so retry shows WHY, not just a count

## Phase 5 — Scope governance & scheduling *(rides the launcher)* — M each
WHERE and WHEN the farm spends CPU — the whitelist/blacklist ask + off-peak + the skip policy.
- **`.juicefarm` opt-IN marker** (per-folder; S, works even from Finder, no launcher needed) — the load-bearing
  default-deny gate (resolved by an upward walk, fail-closed, cached per-directory). Matches the locked opt-IN
  decision above; the worker's `collectTargets` blind `filepath.Walk` must gain the same re-check. The marker's
  content schema (empty = whole-subtree vs a parsed whitelist/exclude + per-folder skip-threshold) needs defining.
  See [`WATCH_FOLDER_DESIGN.md`](WATCH_FOLDER_DESIGN.md) — this marker is its load-bearing prerequisite.
- Manager-owned **exclude-path set** injected on the sweep (`-exclude` glob / `JM_FARM_EXCLUDE`)
- **Skip-if-delta-below-threshold** size budget — operationalizes the size-delta worry; records a `skipped` status distinct from `failed`
- Off-peak window + interval via the manager's existing `/api/schedules` framework
- **Watch-folder** — auto-process on card offload (composes launcher + dedup + `since=`; lands last)

---

## Cross-cutting dependencies (gate the back half)
1. **The sweep-trigger control path (Phase 4).** The running farm reads env ONCE at start and
   `--cpus` is a launch-time cgroup flag — never re-readable. So every SET feature needs a launch.
   Chosen mechanism: manager `docker run` of a one-shot on `ix-juicemount_default` (zero farm code).
   The alternative (farm-side admin-keyed `POST /sweep` standing service) is only needed if a
   standing service is chosen — flagged, not defaulted. This is why Phases 1–3 are all read-only.
2. **OpenLoupe rebuild / contract bump.** Anything that alters the OL-3-locked proxy recipe
   (container/pix_fmt/audio) needs a coordinated client rebuild — so "smarter proxy preset for
   compressed sources" is **deferred out of this sprint**; the Phase-5 size-budget SKIP achieves
   the same goal without touching the locked recipe.
- The additive `farm-status.json` / row `extra` fields are schema-additive + relayed verbatim by
  the CGO-free manager; they need a farm re-sweep to backfill (bump producer Version — but gate the
  re-derive to the cheap tech pass, NOT a full proxy re-encode).

## Open questions (Leland's calls)
- **Pre-gen default:** default-ON (process all except blacklist) or default-OFF (opt-in per folder)?
  Default-off is safer on a big shared NAS (no surprise 22-core full-volume sweep) but shifts burden
  to per-project enablement. Decides whether Phase 5 leads with `.nojuicefarm` or a `.juicefarm` opt-in.
- **Proxy skip-threshold %:** the predicted-saving cutoff below which the farm refuses to transcode
  (`JM_FARM_PROXY_MIN_SAVING`). Inputs suggest ~25%; needs a real default, ideally per-folder overridable.
- **Trigger mechanism:** manager `docker run` (needs the manager container to have docker-socket
  access in the TrueNAS deploy — confirm) vs farm-side `POST /sweep`. docker-run recommended.
- **Run mode:** stay one-shot/manual, interval standing service, or manager-scheduled (preferred —
  keeps the farm thin)? Decides whether off-peak is enforced manager-side or in the entrypoint.
- **Whisper large-v3 in the model dropdown** (fetches ~3GB on first use) — offer with a warning or cap at medium.en?
- **Reclaim semantics:** deleting proxies to reclaim disk → clients regenerate locally; confirm acceptable + reversible-by-re-sweep before building a delete path.

## Key risks
- Manager needs docker access to launch sweeps; if the TrueNAS deploy sandboxes it from the docker
  socket, Phase 4 falls back to the larger farm-side `POST /sweep`.
- **Cancel = kill container** is abrupt — confirm proxy blobs are written temp-then-rename so a
  cancelled sweep can't leave a truncated `proxy.mp4` the contract serves.
- The estimator is a heuristic; Phase-3's actual-size capture calibrates it — ship the radar before
  trusting the skip policy in Phase 5.
- A careless producer Version bump re-runs EVERY pass (incl. the CPU-hog proxy) volume-wide — gate
  the bitrate backfill to the cheap tech pass only.
- `since=` cursor can drop rows on a missed poll — count distinct inodes seen, don't assume contiguous delivery.
