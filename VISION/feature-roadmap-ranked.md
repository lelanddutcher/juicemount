# JuiceMount Feature Roadmap — Ranked

> **Purpose:** Decide what to build next. The top-ranked items become **prototype branches** that ship in `VISION/prototypes/`. Everything below the cut line is a known feature gap with an opinion attached.

Inputs: `VISION/positioning.md` v2, `VISION/pain-points.md` (10-item editor wishlist), `VISION/competitive/*.md`.

---

## Implementation status (2026-05-08)

| # | Feature | Score | Branch | Status |
|---|---|---|---|---|
| Pre-1 | Offline pinning + read-time / open-time gates | n/a (built in response to user need, not from this list) | `production-hardening` (8 commits in) | ✅ **production-hardened**, see Prototype 03 |
| 1 | Codec-aware Quick Look proxies | 14 | `prototype/codec-aware-quicklook` | 🟡 prototype scaffold, needs 2–3 weeks to ship |
| 2 | Content-hash backup verification | 13 | `prototype/backup-verification` | 🟡 core engine works, no UI shell, needs 3–4 weeks |
| 3 | Bandwidth-aware streaming fallback | 12 | (covered partially by offline-pin) | 🟢 manual offline-mode toggle shipped; auto-detect mode pending |
| 4 | Project version history / snapshot layer | 10 | — | ⏸ not started |
| 5 | NLE bin sharing / cooperative locking | 10 | — | ⏸ not started |
| 6 | Pre-cache heuristic | 9 | (extends offline-pin) | ⏸ not started |
| 7 | Hosted backend SaaS | 7 | — | ⏸ not started |
| 8 | Windows client | 6 | — | ⏸ not started |
| 9 | Linux client | 5 | — | ⏸ not started |
| 10 | AI semantic search | 5 | — | ⏸ deferred |
| 11 | NLE panel integrations | 5 | (foundation: `internal/nle/`) | 🟡 parsers exist for `.prproj` / `.drp` / `.fcpxml`, no panels yet |
| 12 | Mobile companion app | 5 | — | ⏸ not started |

Legend: ✅ shipped to a hardened branch · 🟢 partial · 🟡 in-branch prototype · ⏸ not started

---

## Ranking framework

Each feature scored 1-5 on three axes. **Total = sum**, max 15.

- **D — Differentiation:** Do competitors have it? 5 = unique, 1 = table stakes. Higher = more strategic.
- **W — Wow factor (demo gravity):** When you show this in the 3-min demo, do jaws drop? 5 = "holy shit," 1 = "ok."
- **E — Effort inverse:** 5 = days, 1 = months. Higher = cheaper.

Plus a fourth column **U — Urgency** (just YES/NO) for things gated on a specific event (e.g. "Frame.io Drive launching")  — not a score, but a tiebreaker.

I deliberately rank effort **inverse** so all axes point the same direction (higher = pick me).

---

## Top tier — must-ship next 60 days (≥11 points)

### 1. Codec-aware Quick Look proxies (R3D / ARRI / BRAW / ProRes RAW) — **score 14, U: YES**

| Axis | Score | Rationale |
|---|---|---|
| D | **5** | Frame.io Drive's optimization doc tells Premiere users to disable analyzers and route media cache to local SSD — implicit admission that their mount is not a primary I/O surface for RAW. Suite has nothing here. Shade markets "500+ file types" but uses cloud inference. **Nobody has fast local Quick Look on RAW.** |
| W | **5** | This is THE demo moment. Spacebar a 1.2GB R3D file in Finder, get a smooth 12fps preview within 200ms. Editors will gasp. |
| E | **4** | We already have the cache + memory buffer. Add: detect codec from file magic, generate a low-res H.264 proxy on-the-fly using ffmpeg (already on every Mac with Resolve installed), cache the proxy, serve it to QLPreviewPanel. ~2-3 weeks of focused work. |
| U | YES | This is the wedge that beats Frame.io Drive head-to-head on the workflow editors care about most. |

**Build approach:**
- Detect: file extension + magic-byte sniff for `.r3d`, `.ari`, `.braw`, `.mxf` (XAVC), `.mov` with ProRes RAW codec id
- Generate proxy: shell out to `ffmpeg` with codec-specific decoder + scale to 720p H.264 + pin to 1-2 cores
- Cache: keyed by `(content-hash, target-size)` → SSD cache directory (existing infrastructure)
- Serve: hijack QLPreviewPanel's URL when our handler sees a RAW request, return the cached proxy
- Fallback: if no proxy yet, spawn the generate job and return original (Quick Look will still play, just slow)

**Why this is prototype #1.** It's the clearest "JuiceMount does what no competitor can" moment. It plays to our strength (local cache + native macOS) against their weakness (cloud bandwidth). And it lands hard in the demo: every editor in the room knows R3D Quick Look is broken everywhere else.

---

### 2. Content-hash backup verification + multi-target inventory — **score 13**

| Axis | Score | Rationale |
|---|---|---|
| D | **5** | Editor wishlist #7: "Not 'did the rsync finish' but 'did the bytes I have match the bytes I'm supposed to have.'" Nobody in the competitive set does this. Backblaze, CCC, ChronoSync only verify what they touched, not the editor's actual layout. |
| W | **4** | The demo shows: open the JuiceMount inventory window. Each project shows green/yellow/red status — green = 3+ verified copies, yellow = 1-2, red = single source of truth. Click red, see exactly which files are at risk. Toy Story 2 trauma is universal. |
| E | **4** | We already content-hash everything in the cache. Add: a `verify` command that walks N additional targets (LTO mount, secondary B2 bucket, external SSD), reads byte-by-byte (or just ranges + size), confirms the hash matches. ~3-4 weeks. |

**Build approach:**
- Schema addition to SQLite: `verifications(path, target_url, last_verified, hash_match, last_byte_count)` 
- Worker: configurable schedule, walks each target, computes/compares hash, updates rows
- UI: new "Backups" tab in menu bar app showing the inventory with traffic-light status
- Bonus: a "delete this safely" button that refuses unless N≥2 verified copies exist

**Why this is prototype #2.** It's the "you'll thank us when your drive dies" feature. Demo gravity is high (pulls on universal editor trauma). Differentiation is structural — no competitor sits at the metadata layer and can plausibly claim this. It also sets up the paid tier story: free users get manual verify, Pro gets scheduled.

---

### 3. Bandwidth-aware streaming fallback (LAN-first, cellular-degraded) — **score 12**

| Axis | Score | Rationale |
|---|---|---|
| D | **4** | Frame.io Drive caches but doesn't auto-switch to lower-res when bandwidth drops. LucidLink doesn't either. Suite's onsite cache appliance is the only thing close. |
| W | **4** | Disconnect Wi-Fi mid-demo, watch the player switch to a 720p proxy without a hiccup. Reconnect, full-res returns. Trust me, editors will sit forward. |
| E | **4** | Network-quality detection is built-in to macOS (`SCNetworkReachability`). We already have proxies (from #1). Just add a "preferred quality" toggle in the read path. ~2 weeks. |

**Build approach:**
- Network watcher: poll bandwidth using a 1MB read against a known endpoint every 30s
- Quality state machine: `full` (≥100Mbps), `proxy` (10-100Mbps), `metadata-only` (<10Mbps)
- Read path: when a NFS read fires, check current quality. Serve cached proxy from #1 if degraded.
- UI: small indicator in menu bar popover showing current quality + last bandwidth measurement

---

## Second tier — ship in next 90 days (8-10 points)

### 4. Project version history + snapshot layer — **score 10**

| Axis | Score | Rationale |
|---|---|---|
| D | **3** | Time Machine exists. Resolve has a project autosave. But unified across NLEs in Finder is unique. |
| W | **4** | Right-click .prproj → "Show versions" → scrubbable timeline of project state. "Take me back to yesterday at 4pm." |
| E | **3** | Real engineering. Need: file-modification watcher, dedup'd snapshot store, UI to scrub. ~6-8 weeks. |

Defer to second tier because the underlying tech (snapshot dedup) is the whole job, and we don't want to take a 6-week build on the critical path.

### 5. NLE bin sharing / cooperative locking layer — **score 10**

| Axis | Score | Rationale |
|---|---|---|
| D | **5** | Suite explicitly admits: "no native locking… teams must stay communicative." Premiere's Productions is broken (Larry Jordan: "the padlock is not dynamic"). This is a yawning competitive gap. |
| W | **3** | Demo is harder — needs 2 machines + 2 editors. But the value is unmistakable to anyone who's lost a Premiere project to "everyone hits Save." |
| E | **2** | Hard. Need: project-file content-aware diffing, a presence service, conflict-resolution UI. ~3 months. |

Defer because effort is high and demo doesn't sing solo. Land it after #1-3 establish the wow.

### 6. Pre-cache heuristic (read-ahead based on bin/timeline activity) — **score 9**

| Axis | Score | Rationale |
|---|---|---|
| D | **3** | Frame.io Drive lets users pre-cache manually. Automatic is novel. |
| W | **3** | Subtle in demo — "see how this clip plays the first time even though it wasn't cached?" Editors who've been burned by cold cache will appreciate it; everyone else will miss it. |
| E | **3** | Need: signal source (Premiere XMP? Resolve project parsing? OS-level fseek pattern?). 4-5 weeks of fiddly work. |

Defer because the signal sources are flaky. Worth doing once we have at least one NLE-specific integration.

---

## Third tier — ship in next 6 months (5-7 points)

### 7. Hosted backend SaaS (one-click managed Redis+MinIO) — **score 7**

The product widening play. Once OSS has traction, sell a $50-200/mo hosted backend for non-technical users. **D=2 (table stakes for SaaS), W=2 (no demo gravity), E=3 (real ops work but tractable).** Critical for the GTM expansion to tier-3 persona. Not critical for the launch.

### 8. Windows client (FUSE-T or Dokany based) — **score 6**

`D=2, W=3, E=1.` Windows users are 30%+ of post. But it's months of code and a different test matrix. Wait until the macOS product has a moat.

### 9. Linux client — **score 5**

`D=2, W=2, E=1.` Tiny but loud audience (DITs running Linux carts). Wait until #8 forces us to abstract the platform layer.

### 10. AI semantic search (CLIP-based local inference) — **score 5**

`D=3 (Shade/Iconik own the cloud version, local would be unique), W=4 (search "sunset shots" demo is great), E=1 (CLIP model integration + index storage + UI is months of work).` The dream feature, but the engineering tail is long. Ship after the foundation is bulletproof.

### 11. NLE panel integrations (Premiere CEP / Resolve workflow integration / FCPX share extension) — **score 5 each**

`D=3, W=3, E=1 each (per NLE).` Each is a separate engineering project. Land after we've decided the GTM bets ("we're winning Resolve users" might justify Resolve panel first).

### 12. Mobile companion app — **score 5**

`D=4 (Suite has nothing, Frame.io has the legacy app but no mounted-storage parity), W=3, E=2.` Browse + Quick Look from iPad/iPhone over local network. iPad is the producer's review device. Defer until macOS is rock-solid.

---

## Below the line — known gaps we're choosing NOT to address yet

- **Frame.io review tools (timeline comments, approvals).** Frame.io owns this and they bundle it with Adobe CC. Building it from scratch is a multi-quarter project that distracts from the storage-layer wedge. **Decision: integrate with Frame.io as a target, don't replace it.**
- **C2C ingest hardware integration.** Frame.io has Teradek/RED/ARRI partnerships. We're not the ingest layer. **Decision: expose an ingest API + Webhook so partners can target us, but don't build cameras.**
- **TPN/SOC 2 compliance.** Required for major enterprise studios. Expensive and slow to obtain. **Decision: defer until enterprise-tier revenue justifies it. The sovereign-engineer persona doesn't need it.**
- **CDN edge caching.** Adobe has CloudFront. Suite has Cloudflare. Building a global edge is a $10M+ infra play. **Decision: rely on the user's local cache + their own bucket's region. Don't compete on global delivery.**

---

## What this means for the next two iterations

**Iteration 4 should produce `VISION/prototypes/01-codec-aware-quicklook.md` and a `prototype/codec-aware-quicklook` git branch.** This is feature #1 in the ranking — highest score, highest demo gravity, sharpest competitive wedge, and we have the cache infrastructure already in place.

**Iteration 5 should produce `VISION/prototypes/02-backup-verification.md` and a `prototype/backup-verification` git branch.** This is feature #2 — high score, structural differentiation, sets up the paid tier story.

Both prototypes should be:
- Working code on a feature branch (compileable, even if not feature-complete)
- A `prototypes/0X-*.md` writeup explaining: what the prototype proves, the architecture, what's still TODO before production, what the demo script shows
- Targeted at "sharp enough to demo by end of iteration," not "production-ready"

After both prototypes ship, the loop's `Complete: true` requires the remaining docs (gtm-strategy, pitch-onepager, landing-copy, demo-script) which are all relatively quick synthesis tasks.

---

## Summary table

| # | Feature | D | W | E | Total | When |
|---|---|---|---|---|---|---|
| 1 | Codec-aware Quick Look (RAW proxies) | 5 | 5 | 4 | **14** | **Prototype this iter** |
| 2 | Content-hash backup verification | 5 | 4 | 4 | **13** | **Prototype this iter** |
| 3 | Bandwidth-aware fallback | 4 | 4 | 4 | 12 | Next 60 days |
| 4 | Project version history | 3 | 4 | 3 | 10 | Next 90 days |
| 5 | NLE cooperative locking | 5 | 3 | 2 | 10 | Next 90 days |
| 6 | Pre-cache heuristic | 3 | 3 | 3 | 9 | Next 90 days |
| 7 | Hosted backend SaaS | 2 | 2 | 3 | 7 | Next 6 months |
| 8 | Windows client | 2 | 3 | 1 | 6 | Next 6 months |
| 9 | Mobile companion app | 4 | 3 | 2 | 9 | Q3 2026 |
| 10 | Linux client | 2 | 2 | 1 | 5 | Q4 2026 |
| 11 | AI semantic search (local CLIP) | 3 | 4 | 1 | 8 | Q4 2026 |
| 12 | NLE panel integrations (per NLE) | 3 | 3 | 1 | 7 each | Per-NLE basis |
