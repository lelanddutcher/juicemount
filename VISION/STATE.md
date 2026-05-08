# JuiceMount Vision Loop — STATE

This file is the source of truth for the strategic-vision loop. Every iteration reads this, picks one task, executes, and updates it.

**Mission:** Take JuiceMount from "working menu bar app" to "the winning product in the pro-video shared storage space" — replacing Suite Studios, Shade, Iconik, Frame.io Drive, LumaForge Jellyfish, and overpriced NAS appliances with something a single video engineer or post-production team can self-host on commodity gear (or run hosted).

**Vision-loop status:** ✅ COMPLETE (iteration 7, all 16 deliverables landed)
**Implementation status (2026-05-08):** Prototype 03 (offline-pin) → **production-hardened**. Prototypes 01 and 02 still in branches, not yet production.
**Complete:** true (for the research loop; implementation roadmap continues — see "Implementation status" below)

---

## Implementation status update — 2026-05-08

Production-hardening branch (`production-hardening`, 8 commits in) has taken Prototype 03 from "working end-to-end on a wired LAN" to "survives 97%-full-disk + cellular handoff + Resolve playback + cache eviction." The full punch list of fixes is in `CHANGELOG.md` under the 2026-05-07 / 2026-05-08 entry. Highlights against the strategic positioning:

- **"The local-first answer to Shade" frame holds.** Pinning + offline mode + auto-fail-fast on cellular = the cloud-default-vs-local-default differentiator made operational.
- **Verified perf numbers** for the demo script: 215+ MB/s pinned offline reads, 4.6 MB of network traffic on a 200 MB sequential read, 4–67 ms fail-fast on un-pinned offline reads. These are the numbers to put on the landing page.
- **Disk reality**: the user's 97%-full machine forced us to confront APFS purgeable space (Time Machine local snapshots) and JuiceFS `--free-space-ratio` defaults. Both are documented in code and `ARCHITECTURE_juicemount.md` § 11. Real "we shipped this on a real video editor's Mac" credibility, not a synthetic demo.
- **Eviction-pressure UI** (3-state banner) addresses the "where does the user know caching stopped" question directly — answer to the exact question the persona would ask.

Of the **6 of 10 wishlist features the editor research said we already had**, all 6 still hold and have stronger evidence now:

1. Instant search ✅ (FTS5, ~29 ms across 100 K entries)
2. Quick Look preview ✅ (spacebar in search window)
3. Fast metadata ✅ (sub-millisecond for cached LOOKUPs)
4. Multi-machine sync ✅ (Redis pub/sub)
5. NLE-friendly mounts ✅ (NFS at /Volumes/zpool, no special drivers)
6. No recurring fees with self-host ✅ (BYO Redis + MinIO/B2/R2)

Of the **4 remaining**:

1. **Proxy auto-generation** — Prototype 01 (codec-aware Quick Look) is the start. Branch: `prototype/codec-aware-quicklook`. Score 14. Estimated 2–3 weeks to production.
2. **AI tagging / transcript search** — score 5, deferred. Local CLIP inference is plausible on Apple Silicon, not the next priority.
3. **NLE bin/timeline integration** — score 5–10 across Premiere CEP / Resolve workflow / FCPX share extension. Foundation laid by `internal/nle/` parsers (Premiere .prproj, Resolve .drp, FCPX .fcpxml extract media references) — this is what bin-sharing builds on.
4. **Multi-user write locking** — score 10. Not started.

### What production-hardening shipped that wasn't on the original wishlist

- APFS purgeable-space reclamation
- JuiceFS daemon log promoted into our structured log
- Cold-start retry for transient network unreachability (cellular handoff)
- Live cache-pressure UI banner

These are "we live in macOS reality, not theoretical reality" wins. The persona ("Sovereign Video Engineer") respects this kind of polish.

---

## Done

- [x] **2026-05-07 iter 1:** Bootstrapped VISION/. Spawned 4 parallel research agents. Wrote hypothesis-driven `positioning.md`.

- [x] **2026-05-07 iter 2 (extended — skipped scheduled sleep since cache was warm and all 4 agents had completed):** Reaped all research, then rewrote `VISION/positioning.md` v2 evidence-based, ~3500 words. Major synthesis:
  - **The OEM bombshell** is now the central wedge: Frame.io Drive runs on Suite's substrate. Two of four cloud-streaming "competitors" share one proprietary engine.
  - Built a verified pricing comparison table (10TB / 5 seats baseline). JuiceMount + B2 = $60/mo, ~12× cheaper than Suite managed, ~6× cheaper than Suite BYO, ~2.5× cheaper than Shade.
  - **Locked the primary persona: "Sovereign Video Engineer"** — TrueNAS-owning, 10GbE-equipped, Resolve+Premiere-using, Drobo-burned, terminal-comfortable, OSS-leaning. Where to find them: r/truenas, r/selfhosted, Lift Gamma Gain, HN.
  - Refined "who we beat" table with explicit strategic stance per competitor.
  - Wrote a one-paragraph product narrative ready to be the messaging-pillar source.
  - **The wishlist match** is the PMF proof: 6 of 10 editor wishes already shipping, 4 on credible roadmap.
  - Explicit non-goals captured: not the easiest cloud storage, no Premiere panel, no AI semantic search yet, not enterprise.

- [x] **2026-05-07 iter 3:** Wrote two foundational docs that gate the prototypes:
  - `VISION/feature-roadmap-ranked.md` — 12 features scored on Differentiation × Wow × Effort. Top 2 picks for prototypes:
    1. **Codec-aware Quick Look proxies (R3D / ARRI / BRAW / ProRes RAW)** — score 14/15. This is THE demo moment. ~2-3 weeks of work using the existing cache infrastructure + ffmpeg.
    2. **Content-hash backup verification + multi-target inventory** — score 13/15. Sets up the paid tier story. ~3-4 weeks.
  - `VISION/personas.md` — three personas locked with day-in-the-life vignettes, where-to-find-them, and willingness-to-pay:
    - **P1 Sovereign Video Engineer** (Leland-class) — bottoms-up OSS adoption, $5-15/mo Pro convert, found at r/truenas + HN
    - **P2 Boutique Post House** (3-15 ppl, $50-200/mo Team license) — referral-driven from P1
    - **P3 Sovereignty-Bound Shop** (broadcaster/gov/legal) — $5K-50K/yr enterprise, 12-18 months out
  - Spawned brand-identity sub-agent in background; expected to land before next wake.

- [x] **2026-05-07 iter 4 (extended — skipped scheduled sleep again, brand-identity landed and cache was warm):**
  - Reaped `VISION/brand-identity.md` (~3000 words). Picks: keep "JuiceMount" for OSS v1, rename hosted/Pro tier to **Loft** later. Tagline: **"Your bytes are your bytes."** Voice: Tailscale + Backblaze + Linear. Visual: oil/slate/parchment + juice-orange + signal-green/amber, Inter + JetBrains Mono, dark mode primary. One-line design rule: **"Boring on the outside, fast on the inside."** Logo: mount-arrow glyph that survives the JuiceMount → Loft rename.
  - **Built prototype #1 — codec-aware Quick Look proxy generation.** Branch: `prototype/codec-aware-quicklook` (commit `b9db579`). Working code:
    - `internal/proxy/codec.go` — codec detection (extension + magic-byte sniff for ProRes RAW)
    - `internal/proxy/ffmpeg.go` — VideoToolbox-accelerated ffmpeg shellout, atomic .tmp.<pid>+rename writes
    - `internal/proxy/manager.go` — content-hash cache + bounded worker pool + 30-sec failure backoff + concurrent-Get coalescing
    - `internal/proxy/codec_test.go` + `manager_test.go` — 11 tests, all pass
    - `cmd/proxy-smoke/` — manual end-to-end smoke test
    - `VISION/prototypes/01-codec-aware-quicklook.md` — spec + demo plan
  - End-to-end smoke test: 100KB H.264-disguised-as-R3D source → 588KB valid mp4 proxy in 819ms.
  - Bugs caught while building: TOCTOU on partial mp4 reads (fixed with .tmp.<pid> rename), ffmpeg auto-format-detection breaking on .tmp suffix (fixed with explicit `-f mp4`), retry-storm on persistent ffmpeg failure (fixed with 30-sec backoff). 296 false failures observed before backoff went in.

- [x] **2026-05-07 iter 5:** **Built prototype #2 — content-hash backup verification.** Branch: `prototype/backup-verification` (commit `d5939a7`). 1,502 LOC across 9 files, 18/18 tests pass. Working code:
  - `internal/verify/target.go` — Target interface (Walk/Hash/Available/Identifier) + sha256 stream helpers
  - `internal/verify/local.go` — local filesystem target with Apple-Double + .DS_Store skip filter
  - `internal/verify/manifest.go` — JSON-backed verification record store with atomic writes; majority-vote canonical hash; per-target OK flag for mismatch detection
  - `internal/verify/manager.go` — runs targets concurrently; computes per-file traffic-light status (green ≥3 verified copies / yellow 1-2 / red 0 or corruption); SafeToDelete check refuses unless ≥2 OTHER targets verify
  - `cmd/verify-smoke/` — CLI E2E: takes N target dirs, walks/hashes them, prints aggregate stats + per-file status + SafeToDelete demo
  - `VISION/prototypes/02-backup-verification.md` — spec + 8-step demo script + risk register
  - Verified end-to-end with the killer scenario: 3 local targets, one with silent bit-rot (same file size, different bytes). System correctly: detected the corruption, marked status RED, refused SafeToDelete (only 1 OTHER target with verified bytes).
  - Both prototype branches now landed. **Code-side exit criteria met.**

- [x] **2026-05-07 iter 6:** Two strategic docs landed.
  - Copied `VISION/prototypes/01-codec-aware-quicklook.md` and `02-backup-verification.md` from their respective branches to main so they're visible in the canonical VISION/ tree.
  - **`VISION/gtm-strategy.md`** — full launch sequence with: pricing table (Free / Pro $5/mo / Team $25/mo per machine / Loft $50-500/mo hosted / Enterprise), 4-week launch sequence (T-30 pre-launch → Show HN Tuesday morning → Lift Gamma Gain T+1 → ProVideo Coalition T+3 → first retro T+7 → Pro tier T+30 → Team T+90 → Loft T+180 → NAB 2027 inflection), specific Reddit/forum URLs + 8 named writers for direct outreach, 90-day content calendar, founder-led sales playbook, risk register, and 6/12/24-month milestone targets ($3K/$15K/$60K MRR).
  - **`VISION/pitch-onepager.md`** — investor-ready single page: problem with named editor quotes, 5-bullet product summary, the OEM-bombshell wedge, traction (working app + 2 prototype branches with line counts and test results), market sizing ($500M-1B+ TAM with 1% = $9M opportunity at Pro pricing), business model with year-2 ARR projection (~$2M), team (Leland solo + year-2 hires), the ask ($750K-$1.5M at $7M-$12M post), why now (5 specific market conditions), why us (4 founder-fit points). Contact info at the bottom.

- [x] **2026-05-07 iter 7 (FINAL):** Two creative docs landed; loop complete.
  - **`VISION/landing-copy.md`** — production-ready copy for juicemount.io: hero with the locked tagline ("Your bytes are your bytes."), 3 feature blocks (search / RAW Quick Look / backup verification), the OEM-bombshell narrative section, real editor quotes from pain-points.md (Capterra + Lift Gamma Gain), full pricing comparison table, three-layer architecture explainer, 11-question FAQ, closing CTA, plus designer notes on visual treatment (palette, typography, OG image direction).
  - **`VISION/demo-script.md`** — 3-minute walkthrough with 4 beats (cold open + R3D Quick Look + Cmd+Shift+F search + backup verification + pricing reveal), production notes (pacing, music, voice direction, format), 6 channel-specific cuts (HN GIF / Twitter / landing hero / YouTube full / NoFilmSchool / investor deck), backup plan if something fails on stage, and explicit "what NOT to show" rules.
  - All 16 deliverables verified present. **STATE: complete.** Loop ends here; no further wake-ups scheduled.

## In progress

(nothing currently running)

## Queue (priority order)

✅ **Empty. All deliverables shipped. See "Done" above for the full audit trail across 7 iterations.**

## Notes / running insights

- Loop iteration 1 was bootstrap-heavy: directory creation, STATE.md, research agent spawn. Real synthesis starts iteration 2.
- The four parallel agents should produce 6-7 markdown files between them. Iteration 2's job: reap those, do the positioning synthesis, then queue feature-roadmap.
- DO NOT re-research what's already in VISION/. Always check this file before WebSearching anything.
- Code prototypes use branches off main (`git checkout -b prototype/<name>`). Don't commit prototypes to main until they're production-ready.

### 🔥 Critical insight from Frame.io Drive research (agent B, completed early)

**Frame.io Drive is OEM'd from Suite Studios.** Confirmed via Adobe launch blog + Suite businesswire press release. Adobe didn't build the streaming filesystem — they bolted Frame.io's account/permissions/version model onto Suite's tech. Implications:

1. **Two of our top "competitors" share the same proprietary substrate.** If Suite's tech has a structural flaw (e.g. macFUSE kext-deprecation risk on Sonoma+, or scaling limits), Frame.io Drive inherits it.
2. **The OEM relationship is itself a vulnerability.** Suite is pivoting to an S3-native, no-client model that competes with what Adobe just shipped. There's tension between vendor and OEM customer that JuiceMount can exploit.
3. **Hard product limits to use in messaging:** Frame.io Drive caps at one project mounted per user, 250-version cap per asset, AWS us-east-1-only for Storage Connect, Linux unsupported, Storage Connect = Enterprise-Prime-only. JuiceMount has none of these.
4. **Pricing math:** Frame.io Drive Pro tier ≈ **$7.50/TB/month bundled**. Wasabi ≈ $0.50/TB/mo, B2 ≈ $6/TB/mo. JuiceMount + Wasabi crosses break-even vs Frame.io at ~$200/month and pulls ahead from there.
5. **Adobe's own optimization doc tells Premiere editors to disable analyzers and route media cache to local SSD** — implicitly admitting the mount is not a primary-tier I/O surface. Use this in positioning.
6. **Positioning recommendation from agent:** don't fight Adobe for Premiere-centric CC shops. Pitch JuiceMount to Resolve/FCPX/Avid users, sovereignty-constrained studios, and shops where per-seat storage pricing breaks. **"The Linux to their macOS" framing.**

Iteration 2's positioning synthesis must lean into this — the OEM relationship is the wedge.

### 🔥 Critical insights from Suite Studios research (agent A, completed)

Confirms and extends the Frame.io finding above:

1. **Suite IS the engine inside Frame.io Drive** (April 2026 BusinessWire). They get free distribution to every Premiere/Frame.io customer. Single biggest threat development of the past year.
2. **Real, confirmed pricing:** Managed = **$75/TB/mo**, BYO = **$40/TB/mo + customer's own bucket** (20 TB minimum), 5 users included, $10/seat after, zero egress, 14-day trial.
3. **Mount mechanism deliberately undocumented.** Custom user-space filesystem (likely FUSE-T-class), separate Intel and Apple Silicon binaries, no public confirmation. Lock-in by obscurity.
4. **Resolve Local Project Libraries have NO native locking** — admitted in Suite's own KB. They tell customers to "stay communicative." This is a workflow gap a competitor can fill.
5. **What Suite explicitly does NOT have:** mobile app, native MAM, AI search, Quick Look-as-feature. They partner-out for asset management (iconik, Frame.io).
6. **Funding:** ~$21.5M total ($3.5M seed 2021 + $10M + $12.5M in 2025). Boulder CO. ~50 employees. Founders: Craig + Mike Hering (brothers) + Jay Maxwell.
7. **Agent's positioning recommendation:** Don't fight Suite for enterprise logos — they're winning that. Own the segment they've **priced out**: indie editors, freelancers, 1–10-person shops with existing B2/R2/Wasabi buckets. JuiceMount can be **~10–15× cheaper at the storage layer** with no lock-in.

Combined Frame.io + Suite intel sharpens the wedge:
- Suite + Frame.io Drive together own the "we'll mount the cloud for you" market — at $40-75/TB/mo + per-seat. JuiceMount's ceiling for "the cheap, sovereign alternative" is enormous.
- Their workflow gaps (no Resolve locking, no MAM, no AI, no mobile) are JuiceMount's differentiation lanes.

### 🔥 Critical insights from Jellyfish/NAS research (agent C, completed)

1. **LumaForge no longer exists as a brand.** `lumaforge.com` redirects to `owc.com/solutions/jellyfish`. OWC acquired them in 2021. Treat "Jellyfish" = OWC, not LumaForge.
2. **The actual SKU JuiceMount competes with is the new Jellyfish Studio at $4,990** (2024) — not the $30K Tower. That's the freelance/boutique tier. Tower & Rack ($10K-$50K) are different conversations.
3. **TrueNAS users = natural early adopters** (confirms our hypothesis). They already self-host, run Docker, value sovereignty. Synology Mac users = secondary.
4. **Drobo's Chapter 11→7 liquidation (2022→2023) + Beyond-RAID proprietary format = perfect framing.** "JuiceMount sits on top of standard SMB/NFS — your data is portable to any S3 bucket. No proprietary lock-in."
5. **QNAP has serious ransomware history** (DeadBolt / CVE-2022-27593). Security-conscious editors are a target segment.
6. **Recurring NAS pain point: Mac Finder over SMB is slow.** Browse-time on a 100K+ file NAS via SMB is painful. JuiceMount's instant FTS5 search and cached metadata directly fix this — even for users who keep the NAS as the backend.

### 🔥 Critical insights from Iconik/Shade/pain-points research (agent D, completed)

1. **Shade IS a direct competitor** (not complementary as we initially hoped). Cloud-NAS+MAM hybrid, **$29.75-35/seat/month** Growth tier (500GB active + 1TB BYOS). Founder pedigree from Maxon/Cinema 4D. Native BRAW/R3D/ARRI handling. Their own marketing claims 70% cost savings vs the Iconik+LucidLink+Frame.io stack. Most philosophically aligned competitor in the set.
2. **JuiceMount's natural framing vs Shade:** "the local-first answer to Shade." Shade is cloud-default; JuiceMount is local-default with cloud option. Shade is per-seat SaaS; JuiceMount is OSS + optional hosted backend.
3. **Iconik pricing is bizarrely public:** $0/$9/$65/$120 per-user tiers. The $9 tier is a clear "trying before they want to spend $65" target. Iconik is enterprise — JuiceMount targets the buyer who **bounced off Iconik's pricing page**.
4. **The wishlist file is gold:** 10-item editor wishlist. **6 of 10 features are already built in JuiceMount** (instant search, Quick Look preview, fast metadata, multi-machine sync, NLE-friendly mounts, no recurring fees with self-host). 4 remaining: NLE bin/timeline integration, AI tagging/transcript search, proxy auto-generation, multi-user write locking. **That's product-market-fit territory** — we don't need to invent demand, we need to ship the remaining 4.
5. **Reddit was blocked from WebFetch** during research — pain-points.md uses Capterra, G2, Trustpilot, Lift Gamma Gain, Creative COW, Blackmagic Forum, Adobe Community quotes instead. Has named, dated, attributed quotes from real editors (Perry Paolantonio, Michael Cavanagh, etc.). Future Reddit pass would only strengthen the file.
6. **Frame.io Drive launch reaction (April 2026):** trade press enthusiastic, editors cautious about Adobe billing/account behavior. The "I don't trust Adobe with my storage" sentiment is a real wedge.
7. **Suite Studios reaction:** customers concentrated complaints on the $75/TB + $10/user pricing math + bandwidth dependency. Both are JuiceMount's natural strengths.

## Definition of "complete"

`Complete: true` requires ALL of these to exist as committed/written files:
- `VISION/competitive/suite.md`
- `VISION/competitive/shade.md`
- `VISION/competitive/iconik.md`
- `VISION/competitive/frameio.md`
- `VISION/competitive/jellyfish.md`
- `VISION/competitive/nas-vendors.md`
- `VISION/pain-points.md`
- `VISION/personas.md`
- `VISION/positioning.md`
- `VISION/brand-identity.md`
- `VISION/feature-roadmap-ranked.md`
- `VISION/gtm-strategy.md`
- `VISION/pitch-onepager.md`
- `VISION/landing-copy.md`
- `VISION/demo-script.md`
- ≥2 prototype branches with working code AND `VISION/prototypes/0X-<feature>.md` writeups
