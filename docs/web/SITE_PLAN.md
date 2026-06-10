# juicemount.com — Site Plan

**Track:** Launch Plan row W (web presence). Companion docs: `docs/README_DRAFT.md` (repo-facing copy), `docs/web/INTERACTIVE_TOOL.md` (the calculator).
**Voice source:** `VISION/brand-identity.md` — "Tailscale meets Backblaze, with a colorist's vocabulary." Dry, exact, on the editor's side. Boring on the outside, fast on the inside. No gradient buttons, no scroll-jacking, no aspirational founder quotes.
**Competitive data in this doc was researched June 2026** — every price cites its source; re-check at launch.

---

## 1. Who the site is for, and the one job it has

Primary visitor: the **Sovereign Video Engineer** (locked persona, `VISION/positioning.md`) — solo or 1-of-3 shop, owns a TrueNAS/Synology, has 10 GbE somewhere, edits in Resolve/Premiere, reads r/truenas and HN, has either quit LucidLink over the bill or never started because of it.

The one job: get them from "another storage tool, sure" to **`docker compose up` on their box** in under ten minutes of reading. Everything on the site is judged by whether it advances that.

Secondary visitors (don't optimize for, don't repel): boutique post-house leads comparing against a Suite/Shade quote; HN/Reddit traffic kicking the tires on the architecture claim.

---

## 2. Messaging hierarchy — the needle to thread

The pitch is one sentence with three load-bearing clauses, in this order:

> **The mounted-drive workflow editors get from LucidLink-class SaaS — at 10 GbE-direct-attached speed, with Dropbox-style offline resilience — on hardware you already own.**

The hierarchy (each level is the proof for the level above):

| Level | Claim | Proof asset |
|---|---|---|
| 1 — Category | "Storage layer for video editors. Self-hosted." | hero demo video |
| 2 — The hybrid thesis | Speed **and** resilience **and** ownership — competitors make you pick two | the three-way comparison section |
| 3a — Speed | ~7 Gbit/s up/down on 10 GbE (author-measured); cached reads at NVMe speed; sync-class tools cap ~1 Gbit on the same wire | benchmark page + methodology link to repo |
| 3b — Resilience | block-streaming partial reads; pin-for-offline; offline mode fails fast; write spool acks locally, uploads in background | 3 short screen recordings |
| 3c — Economics | $0 software + your hardware vs. per-seat/per-TB forever | the interactive calculator |
| 4 — Trust floor | open source, built on JuiceFS (mature, credited), plain S3 objects you can walk away with, no telemetry | GitHub link, architecture page |

Rules of engagement (from the honesty constraints):

- Every performance number carries "measured on the author's setup" + a link to `docs/PERFORMANCE_METHODOLOGY.md`. No vendor-style benchmarketing.
- Never claim to replace cloud collaboration/review (Frame.io-lane). The line is: *"S3 and cloud collaboration have their place — this changes the cost equation for small teams so speed doesn't force inferior infrastructure."*
- Concede competitor strengths plainly (AI search, managed convenience, Windows). Conceding is what makes the wedge claims believable.
- macOS-only and you-run-a-server stated above the fold, not buried in a FAQ.

---

## 3. Information architecture

Static site (no backend — matches the no-telemetry ethos and the calculator is client-side JS). Six pages + nav.

```
juicemount.com
├── /              Home — hero, demo, hybrid thesis, feature trio, calculator teaser, quick start
├── /how-it-works  Architecture for skeptics — diagram, read/write paths, what JuiceFS does,
│                  what JuiceMount adds, security model (LAN ports, sudoers scope), honest limits
├── /compare       Comparison hub (lane intro + table) 
│   ├── /compare/lucidlink     per-competitor pages, factual + dated
│   ├── /compare/suite
│   ├── /compare/shade-aspect-iconik   (one page: the AI-MAM lane)
│   └── /compare/nextcloud-seafile-mountainduck  (one page: the self-hosted sync lane)
├── /calculator    The interactive tool (see INTERACTIVE_TOOL.md) — also embedded on Home
├── /performance   The numbers + rig disclosure + methodology link + how to reproduce
└── /docs → links out to the GitHub repo docs (no separate docs site at v1; see § 7)

Nav: How it works · Compare · Performance · Calculator · Docs ↗ · GitHub ↗ (star count)
Footer: GitHub · Roadmap (repo link) · JuiceFS attribution · License · no-telemetry statement
```

Deliberate omissions at v1: no blog (changelog lives on GitHub Releases; HN/Reddit posts are the "blog"), no pricing page (it's $0 — pricing language would imply a SaaS), no testimonials page (see § 6), no email capture modal (a single low-key GitHub-Releases/RSS line in the footer is enough).

---

## 4. Hero concepts — three options

All three pair with the same primary CTA block: **[Quick start ↗ GitHub]** + secondary **[Watch the 90-second demo]**, and the same qualifier line under the CTAs: *"Open source · macOS 14+ · runs against your own server (TrueNAS, Synology, any Docker box)."*

### Option A — the locked tagline (ideology-first)

> # Your bytes are your bytes.
>
> JuiceMount mounts your own server as a Finder volume that behaves like LucidLink and costs like a NAS: scrub 100 GB files over the network touching only the blocks you play, pin projects for offline, write at local-SSD speed while uploads trickle in the background. No per-seat fee. No per-TB fee. No storage contract. Your hardware.

- **For:** the strongest unique claim — no competitor can say it without rebuilding their architecture (`VISION/brand-identity.md` rationale). Resonates hardest with r/selfhosted/HN.
- **Against:** abstract; an editor in a hurry doesn't learn what the product *does* until the subhead. Slightly ideological for a post-house lead with a Suite quote in hand.

### Option B — the speed math (engineer-first)

> # 7 Gbit/s to hardware you own.
>
> The LucidLink workflow — mounted volume, partial-file streaming, offline pinning — re-built open-source on your own NAS. On a 10 GbE LAN it moves at wire speed*; sync tools like Nextcloud and Mountain Duck cap near 1 Gbit on the same hardware. Cached reads come off local NVMe. Your cloud bill becomes $0.
>
> <small>*Measured on the author's Mac ↔ TrueNAS rig — methodology published.</small>

- **For:** the most concrete differentiated claim; numbers are the persona's native language; instantly frames both competitor lanes.
- **Against:** leads with a number that needs an asterisk; invites benchmark fights; undersells the resilience half.

### Option C — the scrub moment (workflow-first)

> # Scrub 3 seconds of a 100 GB file. Stream 3 seconds of a 100 GB file.
>
> JuiceMount is a Finder volume backed by your own server. It streams the blocks your NLE touches instead of syncing whole files, caches them on NVMe so the second play is instant, keeps pinned projects working offline, and acks renders at local-SSD speed while uploading in the background. Open source. Your hardware. No storage bill.

- **For:** the demo *is* the headline; editors feel this pain weekly (Nextcloud-class tools genuinely move the whole file); converts best with the looping video behind it.
- **Against:** longest; the economics clause arrives last.

**Recommendation:** **C as the hero** (workflow hook + video), **A as the closing section headline** (the ideological mic-drop after the proof), **B's claim as the Performance-page H1**. This uses all three where each is strongest. If only one can exist: C.

---

## 5. /compare — positioning page strategy

Two lanes, named honestly, each with a hub framing and per-competitor pages. Every page: a dated "pricing as of <date>, from <linked source>" stamp, a "what they do better" section (mandatory — it's the credibility device), and the calculator pre-loaded with that competitor's pricing model.

### Lane 1: storage SaaS for editors

Framing: *"These products proved the mounted-drive workflow. The bill and the lock-in are the product. We're the same workflow on your own hardware."*

Research snapshot (June 2026 — re-verify at launch):

| Competitor | Current positioning (their words/claims) | Public pricing | Source |
|---|---|---|---|
| **LucidLink** | "zero-knowledge encryption," AWS-backed filespaces | Starter $7/user/mo (100 GB/user, ≤1 TB); Business $27/user/mo annual (400 GB/user incl., +$8/100 GB/mo extra, "best up to 10 TB"); Enterprise custom | lucidlink.com/pricing, fetched 2026-06-10 |
| **Suite Studios** | "Cloud storage with file streaming"; also sells cloud workstations; now the streaming engine inside Frame.io Drive (BusinessWire 2026-04-17, per `VISION/positioning.md`) | Managed $75/TB/mo, 5 users included, +$10/user/mo after; BYO storage $40/TB/mo; S3-native "coming soon" | suitestudios.io/pricing, fetched 2026-06-10 |
| **Shade** | "AI media asset management" — neural search (faces, transcripts, scene descriptions) + ShadeFS streaming mount; SOC 2 / ISO 27001 / HIPAA | Growth $29.75/seat/mo (sale price from $35), 500 GB *active* storage per seat, ≤15 seats; Enterprise custom (1 TB/seat); avg. contract self-reported $10–15 K/yr for 10 users / 25 TB | shade.inc/pricing, fetched 2026-06-10 |
| **Aspect** | "Intelligent media storage for the whole team" (aspect.inc hero, fetched 2026-06-10) — YC-backed; explicitly pitches replacing "Dropbox, LucidLink, and Frame.io in one place"; streams "like a local drive," AI agent organizes/labels footage | No public pricing — free tier + enterprise contact | aspect.inc + ycombinator.com/companies/aspect-inc |
| **Iconik** (adjacent: MAM layer, not a filesystem) | "media management on storage you already have" | Collaborator $0 / Browse $9 / Standard $65 / Power $120 per user/mo + pay-per-use credits; Pro/Enterprise custom | iconik.io/pricing, fetched 2026-06-10 |

Attack surface per page (factual rows only):
- **LucidLink:** extra-storage math ($8/100 GB = $80/TB/mo); documented cost-creep complaints (Capterra quotes already collected with dates in `VISION/positioning.md`); AWS dependency. Concede: maturity, cross-platform, zero-knowledge encryption.
- **Suite:** $75/TB/mo managed vs. the calculator's one-time-hardware math; their own BYO tier ($40/TB/mo *for the mount software alone*) is the strongest framing — "the mount layer shouldn't cost $400/mo on 10 TB you already own." Concede: Frame.io Drive distribution, polish, workstations, enterprise compliance.
- **Shade/Aspect/Iconik (one page, the AI-MAM lane):** acknowledge plainly that exhaustive AI/metadata search is real value JuiceMount does not have today (roadmap-acknowledged, per the honesty constraint). The wedge: their mount/storage layer is the rental part; an editor can run JuiceMount for bytes and still buy a MAM if they need one (Iconik literally markets BYO-storage). Concede: neural search, review tools, compliance attestations.

### Lane 2: self-hosted sync

Framing: *"Right instinct — own your storage. Wrong primitive for video: these tools sync files; editors need to stream blocks."*

One combined page (Nextcloud, Seafile, Mountain Duck $49-one-time, mountainduck.io/buy, June 2026):
- The two factual hammers: **whole-file transfer semantics** (open 100 GB → move 100 GB; JuiceMount moves the scrubbed blocks) and **throughput ceiling** (~0.8–1 Gbit/s on a 10 GbE LAN in the author's testing vs. ~7 Gbit/s — present as author-measured with methodology, never as vendor specs).
- Concede: Nextcloud/Seafile are great for documents/general sync, cross-platform, huge communities; Mountain Duck is a fine $49 generalist bucket-mounter. The page is "use both" friendly: many targets already run Nextcloud — for docs.

### Comparison-hub table (the one table on /compare)

Columns: *Where files live · Streams partial files · 10 GbE LAN speed · Offline files · Price model · Open source.* Rows: JuiceMount + the seven names above. Reuse the verified table from `docs/README_DRAFT.md` (keep the two in sync manually; the site table links each price to its dated source).

---

## 6. Social proof strategy — for a v1 OSS project with zero users

No testimonials exist; do not fake any. Substitutes, in priority order:

1. **The demo is the proof.** Three ≤30 s screen recordings, real Finder/Resolve, no cuts: (a) scrub a cold 100 GB file + network monitor showing only MBs transferred; (b) flip offline mode mid-edit, pinned timeline keeps playing; (c) 2 GB Finder copy acks instantly, `/spool` shows the trickle upload. These do testimonial work better than testimonials.
2. **Numbers with receipts.** The performance page links the repo's methodology doc and regression harness — "run it on your own rig" is the most persona-credible flex available.
3. **Radical transparency artifacts.** Link the QA post-mortems (the 2000× recovery story), `OPEN_BUGS.md`, and the launch-gate ledger. For this audience, published bug honesty converts better than polish. ("The README tells you what's broken" is a feature.)
4. **Borrowed credibility, attributed:** "Built on JuiceFS" with their GitHub stars/production lineage (factual, credited per the attribution requirements); "the architecture LucidLink proved, open-sourced."
5. **Category pain as quotes.** Dated public review quotes about the *problem* (LucidLink cost-creep, Frame.io-after-Adobe complaints — already collected with sources in `VISION/positioning.md` and `landing-copy.md`). Anti-testimonials, clearly cited, never doctored.
6. **GitHub-native signals.** Star button in nav, good-first-issue labels, public roadmap, fast issue responses for the first 90 days (founder commitment — early-responder reputation is the v1 moat).
7. **Launch-thread strategy as proof-generation:** Show HN + r/truenas + r/DataHoarder + r/editors + Lift Gamma Gain (channel list from `VISION/gtm-strategy.md`), each post leading with the demo video and the measured numbers; harvest the first real-user quotes from those threads (with permission) as the site's first attributed proof.

---

## 7. Docs strategy

v1: **the repo is the docs.** The site's /docs nav item links straight to the GitHub tree (README → server/INSTALL-TrueNAS.md → MENU_BAR_APP.md → ARCHITECTURE). Rationale: one source of truth during the launch-churn period; the persona prefers reading docs in a repo anyway; zero docs-site maintenance until content stabilizes.

The only docs-ish content on the site itself: the Quick Start block on Home (mirrors README's, ends with "full guide on GitHub →") and the security-model section on /how-it-works (LAN port exposure, sudoers scope, password-prompt rationale — pre-answering the HN thread's first three questions).

Post-launch trigger for a real docs site (mkdocs/docusaurus): when install-issue traffic shows people failing at a step the README can't fix with one screenshot.

---

## 8. Launch checklist

Ordering note: the site can ship before Phase 4/5 complete, but the GitHub links go live only when the repo is public.

**Must (blocks site launch):**
- [ ] Domain: juicemount.com registered + TLS (any static host: Cloudflare Pages / GitHub Pages / Netlify). `VISION/landing-copy.md` references juicemount.io — **pick one canonical domain, 301 the other.** <!-- decision needed -->
- [ ] License decision landed in repo (site footer states it; currently NO LICENSE file exists — see README_DRAFT VERIFY note).
- [ ] Hero demo video (Option C's scrub moment) + the 3 proof recordings (§ 6.1).
- [ ] Comparison data re-verified + date-stamped on every page (prices above were fetched 2026-06-10).
- [ ] Calculator v1 functional (INTERACTIVE_TOOL.md), with its pricing JSON dated.
- [ ] OG cards: 1200×630 per page — hero card is the scrub screenshot + "Your bytes are your bytes."; compare pages get "JuiceMount vs <X>" cards (these earn the Reddit/HN link-preview clicks). Favicon + social avatars from `logos/`.
- [ ] Analytics-lite consistent with the no-telemetry ethos: **Plausible/GoatCounter (cookieless) or server-log-only — no Google Analytics.** State whichever choice in the footer; the audience checks.
- [ ] Accessibility + performance floor: static HTML, no JS required except the calculator page, Lighthouse ≥95 — "boring outside, fast inside" applies to the site itself.

**Should (first two weeks):**
- [ ] GitHub repo public, README_DRAFT merged (Phase 4), star button live.
- [ ] Show HN + subreddit posts drafted against each community's norms (HN: architecture-first; r/truenas: install-first; r/editors: demo-first).
- [ ] /performance "reproduce this" section with the exact harness commands.
- [ ] RSS/Atom on GitHub Releases linked in footer (the no-email-capture answer).

**Later (explicitly not v1):**
- Real docs site; localized pages; a blog; any pricing/Pro-tier page (per `VISION/gtm-strategy.md` Pro/Team tiers are post-launch decisions — the site should not pre-announce paid tiers it can't ship).
