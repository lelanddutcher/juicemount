# juicefarm ↔ juicemount-manager — operability backlog

Notes on what the server-side farm exposes today and the operational controls the **juicemount-manager**
web UI should grow to drive it. The farm is deliberately a thin, scriptable container right now; the manager
is where humans should throttle, schedule, and watch it. Captured as we build so the manager can be beefed up
to match.

The manager already has the right scaffolding to host this: an admin-key-auth'd JSON API
(`internal/manager/api.go`, every route wrapped in `a.auth()` / `X-JuiceMount-Admin-Key` HKDF) with a
**jobs framework** (`/api/jobs`, `handleJobOps`, persisted job state) built for migrations. A farm sweep is
conceptually just another long-running job — model it on that.

---

## 1. What the farm exposes TODAY (the control surface to build on)

- **Config = env vars** (`server/juicefarm/entrypoint.sh`): `JM_FARM_TARGET` (path to sweep), `JM_FARM_MODE`
  (`all`|`derivatives`|`proxy`|`transcript`), `JM_FARM_WORKERS` (→ `jmfarm -concurrency`), `JM_FARM_MODEL`
  (whisper model), `JM_FARM_PRODUCER`, `JM_FARM_DB`, `JM_FARM_ONCE`, `JM_FARM_INTERVAL`.
- **Invocation = a `docker run`** on the `ix-juicemount_default` network (one-shot or interval-loop). No
  runtime control plane on the farm itself yet.
- **Output the manager can already READ:**
  - the server-side **`derivatives.db`** — per-asset rows with `status` (ready/failed) + `source_size` +
    `updated_at`. This IS the per-asset coverage + failure surface.
  - the **`GET /derivatives/changes?since=`** delta feed (just built) — the natural **live progress** signal:
    poll it during a sweep to count/stream "N derivatives generated", swap badges, show throughput.
- **Throttle knobs that exist:** `jmfarm -concurrency` (worker count), `-limit` (cap files), Docker
  `--cpus` / `--memory` / cgroup on the container. Nothing finer yet.

---

## 2. Controls the manager should expose (the backlog)

### A. Job control — "run the farm on this folder"
A **Farm tab** that lets a user pick a `/jfs` subtree (reuse `/api/browse-jfs`), choose modes, and launch a
sweep as a tracked job (mirror `/api/jobs` + `handleJobOps`: list / status / cancel / resume).
- **Farm side needed:** a way to trigger a bounded sweep with params. Cleanest: the manager `docker run`s a
  one-shot `juicefarm` with the chosen env (it's on the same network), OR the farm grows a tiny control
  endpoint. Cancel = stop the container / signal.
- **Why:** today a sweep is a hand-run `docker run`; the manager should own it like it owns migrations.

### B. Resource throttling — the CPU / parallelism governor ⭐ (Leland's call-out)
Proxy transcode (`libx264 -preset slow`) is the CPU hog; whisper is also heavy. On a shared NAS the manager
must cap the farm so it never starves the live mount / other apps.
- **CPU cap:** container `--cpus=N` (Docker cgroup) — surface as a slider "give the farm up to N cores".
- **Per-pass concurrency:** `jmfarm -concurrency` is one knob for all passes today. **Split it:** a separate,
  *lower* `-proxy-concurrency` (e.g. 2) because each proxy pins a core for the whole clip, vs higher
  concurrency for the cheap tech/poster/filmstrip pass. *(Farm side: add a proxy-concurrency flag/env.)*
- **Max parallel proxies / nice / ionice:** an explicit "max simultaneous transcodes" + optional `nice`/
  `ionice` so the farm yields to interactive load.
- **Off-peak window:** "only run 1am–7am" (ties into scheduling, §D).
- **Why:** without this, a full-volume `all` sweep could saturate 22 cores and degrade everyone.

### C. Quality / model selection
- **Whisper model:** base.en (fast, baked) ↔ medium.en ↔ large-v3 (accurate, bigger image, slower). Expose a
  dropdown; the bigger models change the image (a build/bundle decision — see §4).
- **Proxy quality:** `-crf` (20–23) + `-preset` (slow/medium) — a "smaller vs sharper" knob. *(Farm side:
  plumb these as flags/env; currently hard-coded crf 21 / preset slow.)*
- **Which passes:** checkboxes for tech/poster/filmstrip/waveform/proxy/transcript (and later embeddings).

### D. Scheduling / lifecycle
- **Standing service vs triggered:** the compose `juicefarm` service is an interval loop (`JM_FARM_INTERVAL`)
  today; the live deploy is one-shot. Decide + let the manager toggle: continuous watch, periodic sweep, or
  pure manual.
- **Watch-new-files:** auto-process newly-ingested media (a JuiceFS/Redis change hook or a periodic
  diff-sweep) so a freshly-offloaded card gets derivatives without a human trigger.
- **Container health / restart:** show the farm container's status; restart it.

### E. Visibility — coverage + progress + failures
- **Coverage:** per folder, "X of Y assets have full derivatives" — computed from `derivatives.db` status
  rows (the `source_size` + status make this exact). The single most useful at-a-glance number.
- **Live progress:** poll `/derivatives/changes?since=` during a sweep → a progress bar + throughput + ETA.
- **Failures:** surface `status:"failed"` rows (e.g. a proxy that couldn't render) with the error, + a
  "retry failed" button (re-run the farm scoped to those inodes).
- **Throughput / load:** sweep rate + current CPU (the manager Overview already probes liveness; extend it).

---

## 3. What the FARM must grow to enable the above (farm-side TODOs)

- **`-proxy-concurrency` / `JM_FARM_PROXY_WORKERS`** — separate, lower parallelism for the transcode pass
  (the §B core ask). Small change to `cmd/jmfarm` + the proxy worker pool.
- **`-crf` / `-preset` flags** (proxy) + `-whisper-model` is already a flag — surface them as env.
- **A sweep-progress signal** beyond exit code — e.g. write a `progress` row / heartbeat, or rely on the
  `since=` feed (probably sufficient: the manager counts rows appearing).
- **A trigger mechanism the manager can call** — simplest is "manager `docker run`s a one-shot with env"; if
  a standing service is chosen, a tiny `POST /sweep` control endpoint on the farm (admin-key-auth, same HKDF
  pattern as the manager) to enqueue a job + a `GET /status`.
- **`nice`/`ionice` + a configurable CPU/concurrency ceiling** read from env so the manager sets them.

## 4. Image/bundle decisions the manager surfaces but the build owns
- Bigger whisper models (medium/large) bloat the image (~1.5–3 GB) — either bake a chosen default or fetch on
  first use into the `/state` (or a model) volume so the manager can switch models without a rebuild.
- The forthcoming **embeddings** pass (OL-2: CLIP ViT-L/14 + dlib via a Python onnxruntime sidecar) adds the
  heaviest CPU load of all — its concurrency/CPU cap belongs in §B from day one, and GPU offload (if the NAS
  ever has one) is a future knob.

## 5. Suggested phasing
1. **Read-only first (cheap, high value):** a Farm tab that shows **coverage + failures** from `derivatives.db`
   and **live progress** from the `since=` feed — no farm changes needed, pure manager UI over data that
   already exists.
2. **Triggered sweeps:** manager launches one-shot farm jobs (path + modes + a CPU/concurrency cap) via the
   jobs framework.
3. **Throttling controls:** `--cpus` slider + split proxy-concurrency + off-peak window.
4. **Scheduling / watch-new-files** + the standing-service toggle.
5. **Model/quality selectors** + retry-failed.
