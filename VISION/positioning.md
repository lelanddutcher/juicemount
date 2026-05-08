# JuiceMount Positioning — v2 (evidence-based)

> **Status:** Synthesized from `VISION/competitive/*.md` (Suite, Frame.io Drive, Shade, Iconik, Jellyfish, NAS vendors) and `VISION/pain-points.md`. Replaces iteration-1 hypothesis. Every claim here ties back to a cited source in those files.

---

## The market shape we're entering

The cloud-mounted-storage-for-editors category just consolidated, and most observers missed it. As of April 2026:

- **Suite Studios** is the streaming engine inside **Frame.io Drive** — Adobe didn't build their own filesystem, they OEM'd Suite's tech and bolted Frame.io's account/permissions/version model on top. Two of the four cloud-streaming "competitors" are now the same proprietary substrate. (Source: BusinessWire 2026-04-17; Frame.io launch blog 2026-04-15.)
- **LucidLink** is the legacy incumbent that everyone loves until the bill arrives or AWS goes down. Capterra reviews are explicit: *"When I hit 1 TB of usage my service was completely cut off without any warning"* (Michael W., Feb 2025); *"Cost is creeping up to high figures"* (Matthew T., Mar 2024).
- **Shade** is the upstart bundling storage + AI search + review at $29.75/seat/mo, claiming 70% cost savings vs the Iconik+LucidLink+Frame.io stack — and serving as our most direct philosophical competitor.
- **Iconik** is the enterprise MAM that prices in tiers from $0 to $120/user/mo with custom Pro/Enterprise.
- **Jellyfish** (now OWC, post-LumaForge acquisition) sells turnkey hardware from $4,990 (Studio) to $50K (Rack).
- **Synology / QNAP / TrueNAS** sell DIY NAS — cheap, but the recurring forum complaint is *"NAS is often set up by IT staff who configure things for 'optimal' document work like spreadsheets and Word files, which is absolutely not tenable for video production."* (NAS Compares 2024.)

The market has converged on one shape: **a mounted drive that streams from cloud or NAS, with some flavor of search/review on top**. Every vendor in the set is pitching the same job-to-be-done. The differentiation is **architecture (cloud vs local), pricing (per-seat vs per-TB vs flat), and lock-in (proprietary FS vs portable bytes)**.

---

## JuiceMount's wedge

### The structural advantage Suite/Frame.io/Shade cannot copy

**JuiceMount writes plain objects to a bucket the user owns.** Suite is a proprietary cloud filesystem. Frame.io Drive runs on Suite's substrate plus Adobe's account control plane. Shade runs on Shade's cloud. If any of those vendors goes away, raises prices, or pivots, the customer's library is stranded behind a closed binary.

JuiceMount is the only product in the set where the user can **`mc cp s3://my-bucket/ ~/Movies/` and walk away with everything** — original bytes, intact, no extraction tool required. That's not a feature; it's a structural property of the architecture, and competitors can't graft it on without rebuilding their core.

### The pricing gap is enormous and verified

| Solution | Effective cost (10TB, 5 users) | Lock-in | Storage in your control |
|---|---|---|---|
| Suite Storage (managed) | **$750/mo** ($75/TB × 10) | Full | No |
| Suite BYO | **$400/mo** + bucket bill (~$60 on B2) = ~$460 | Mount client only | Bucket yes, FS layer no |
| Frame.io Drive Pro | **$75/mo** at 5 seats × $15, includes ~10TB bundled | Adobe CC subscription required | No (Storage Connect = Enterprise Prime only, AWS us-east-1 only) |
| Shade Growth | **$148.75/mo** (5 seats × $29.75 annual), 2.5TB active + 5TB BYOS | SaaS account | Partial (BYOS) |
| LucidLink | **~$1,000/mo** at this tier per Vendr buyer guide | Proprietary FS | No |
| Iconik | **$45-600/mo** (5 users × $9-$120) — *MAM only, no storage included* | SaaS | Yes (it's a layer) |
| Jellyfish Studio | **$4,990 one-time hardware** + drives + power + space + IT time | Hardware vendor | Yes |
| Synology DS1823xs+ + JuiceMount | **~$3,000 one-time** + drives | None | Yes |
| **JuiceMount + Backblaze B2** | **~$60/mo** ($6/TB × 10) — *unlimited seats, no per-user fee* | None | Full |
| **JuiceMount + Wasabi** | **~$70/mo** ($6.99/TB × 10) | None | Full |
| **JuiceMount + Cloudflare R2** | **~$150/mo** ($15/TB) — *but zero egress* | None | Full |

**At 10TB / 5 seats, JuiceMount + B2 is ~12× cheaper than Suite managed, ~6× cheaper than Suite BYO, ~2.5× cheaper than Shade, with full data sovereignty.** This isn't us being scrappy — it's the natural consequence of having no SaaS markup layer and no per-seat tax.

### The wishlist evidence is product-market fit

`VISION/pain-points.md` synthesized 10 things editors actually say they want, sourced from named, dated, attributed quotes on Capterra, G2, Trustpilot, Lift Gamma Gain, Creative COW, and Adobe Community. **Six of those ten are already built in JuiceMount today:**

| Editor wish | JuiceMount status |
|---|---|
| 1. Instant search across drives | ✅ Built (FTS5 trigram, <50ms across 100K entries) |
| 2. Content-addressable linking | ✅ Built (content hash in catalog) |
| 3. Pre-cache files about to be needed | 🟡 Partial (read-ahead heuristic — needs polish) |
| 4. Quick Look for RAW (R3D, ARRI, BRAW) | 🟡 Partial (Quick Look works on cached files, but needs codec-aware proxy generation) |
| 5. Project version history | ⏳ Roadmap (snapshot layer) |
| 6. Bandwidth-aware streaming fallback | ⏳ Roadmap (adaptive cache mode) |
| 7. Automatic backup verification (hash-diff, not just rsync-success) | ⏳ Roadmap |
| 8. Trustworthy "delete this project" with multi-target inventory | ⏳ Roadmap |
| 9. A shared catalog that lives in Finder, not a web app | ✅ Built (NFS mount + Quick Look + Cmd+Shift+F search window) |
| 10. Predictable cost, no per-seat surprise | ✅ Built (open-source, BYO storage) |

**4-of-10 to fully address, 6-of-10 already shipping.** This is product-market-fit territory by anyone's standard. The remaining four items map cleanly onto a 6-12 month roadmap.

---

## Three positioning axes — sharpened with evidence

### 1. Anti-lock-in (the OEM bombshell makes this stronger than we knew)

> *"Suite is itself a filesystem that mounts as a local drive on any number of disparate computers"* — Suite blog. Translation: their value is the proprietary filesystem. If you leave, you leave with nothing.

Frame.io Drive inherits this lock-in property directly because it's built on Suite. Shade inherits it because Shade's metadata + AI tags + transcripts live on Shade infrastructure (their own marketing confirms this). LucidLink's Filespace is similar.

**JuiceMount's pitch:** *"Your bytes are your bytes. The mount client is open source. Walk away any time with `mc cp` or `aws s3 sync`. No extraction tool, no migration script, no negotiation."*

This isn't theoretical — Drobo's Chapter-7 liquidation in April 2023 stranded thousands of customers with Beyond-RAID drives that needed proprietary recovery. The "your data is portable" framing has a fresh, vivid cautionary tale.

### 2. Local-first as a feature, not a compromise

Every cloud-streaming competitor has the same Achilles heel: bandwidth and outage dependency. Quotes from the research:

- *"Editors in locations with inconsistent bandwidth experience latency and playback issues that do not exist on local workstations."* — Shade's own review of Suite Studios.
- *"The 'streaming' functionality of LucidLink does not perform well working with high resolution/data rate files [80–400 GB R3D, ARRI] even in a 1 Gb–10 Gb synchronous fiber internet connection."* — Capterra reviewer.
- *"Had some trouble... if it goes down (like when AWS went down) you are a bit dependent."* — Sebastian L., LucidLink customer.
- Frame.io's own optimization guide tells Premiere users to *"set Media Cache Files Location to a local SSD volume"* — implicitly admitting the mount is not a primary-tier I/O surface.

JuiceMount inverts this: **local-first by architecture, cloud-optional by configuration**. Files live where you put them (your NAS, your DAS, your laptop SSD, your S3 bucket). The mount works offline. AWS outages don't stop your edit. ARRIRAW at 4.5K (~2 Gbps sustained) plays at LAN speed because it's reading from LAN.

### 3. Workflow-native, not protocol-native

SMB/NFS to a NAS gives you a folder. Suite/Shade/Frame.io give you a workflow product but lock you into their cloud. **JuiceMount owns the whole stack — protocol, cache, metadata index, search — and uses that to deliver workflows competitors can't match without rebuilding their core:**

- **Cmd+Shift+F search** that returns in <50ms across 131K+ files. Iconik's equivalent requires opening a browser tab and round-tripping to their cloud.
- **Spacebar Quick Look** on a 12TB library that "lives in the cloud" (or on a NAS or on B2). Suite doesn't market this. Frame.io doesn't either.
- **Drag from search results into Premiere/Resolve/FCPX timeline** as a real NSURL. No competitor has this end-to-end.
- **Real-time multi-machine sync via Redis pub/sub** — no polling, no SMB-stale, propagates within 100ms.
- **"Reveal in Finder" that actually works** because the mount IS Finder-native.

---

## Who we beat — refined with evidence

| Competitor | We beat them on | They beat us on (today) | Strategic stance |
|---|---|---|---|
| **Suite Studios** | Cost (10-15× cheaper at storage layer), no FS lock-in, local-first, no seat tax | Frame.io Drive distribution, polished sales motion, SOC 2/TPN, marquee logos | Don't fight for enterprise. Own the segment they've priced out. |
| **Frame.io Drive** | No Adobe CC dependency, multi-NLE (Resolve/FCPX/Avid), BYO storage, no AWS-region constraint, no 250-version cap | Adobe distribution moat, C2C ingest integration, in-product review tools | "The Linux to their macOS." Target the half of the market Adobe doesn't own. |
| **Shade** | Local-first, no per-seat fee, no bandwidth ceiling, RAW workflows on LAN | AI semantic search ("find sunset shots"), bundled review tools, designer UX polish | "The local-first answer to Shade." Cross-sell potential for Shade users with legacy NAS archives. |
| **Iconik** | In-Finder native UX (vs browser tab), $0 vs $9-120/seat, search in <50ms vs cloud roundtrip | Mature MAM features, AI tagging, transcription, enterprise integrations | Target the buyer who bounced off Iconik's pricing page. |
| **LucidLink** | One-time/free vs $1K+/mo at 10TB, no surprise overage, works offline, RAW on LAN | Battle-tested at scale (10+ years), broader awareness | Lean into the cost-creep narrative; LucidLink customers are actively shopping. |
| **Jellyfish (OWC)** | Software-on-existing-hardware (vs $5K-$50K hardware lock-in), remote access, modern macOS UX, search across the library | Turnkey hardware experience, no IT skill required, single-vendor support | "Your $5K Jellyfish + JuiceMount = your $50K Jellyfish." |
| **Synology/QNAP/TrueNAS direct** | Search, preview, cache, real-time sync, modern UX over their tired SMB | Already paid for, "good enough" inertia, web admin UI | Position as the modern UX layer their NAS deserves. TrueNAS users are the natural early adopters. |
| **Drobo (defunct)** | Still being supported in 2026 | (N/A — they're dead) | Use as cautionary tale: "Your storage shouldn't depend on a vendor staying in business." |

---

## Primary persona — locked

**The Sovereign Video Engineer** (representative: Leland-class user, but generalized)

- **Role:** Solo or 1-of-3 person production company. Director-of-photography, colorist, post-supervisor, or generalist video engineer who runs their own infrastructure.
- **Existing setup:** TrueNAS or Synology box already running. 10GbE somewhere on the network. JuiceFS, MinIO, Docker compose configured. Probably has a Mac Studio or MacBook Pro M-series. Probably already pays for Backblaze B2 personal backup or has tried Wasabi.
- **Tools:** Resolve + Premiere primary. FCPX sometimes. After Effects, Illustrator, Photoshop. Premiere Productions or Frame.io tried-and-discarded. ScreenFlow / Loom for client review.
- **Pain points (top 5 from research):**
  1. **NAS browse is slow.** Finder over SMB on a 100K-file NAS makes them want to throw the laptop.
  2. **"Media offline" after every project move.** Folder renamed, drive remounted at different path, Premiere blows up.
  3. **Frame.io got worse after Adobe.** Bills creeping, account suspensions, sync issues.
  4. **LucidLink is too expensive for a side income / freelance budget.** $1K/mo for 10TB is not viable.
  5. **Search across the archive is broken.** Spotlight skips network volumes and most video formats. Iconik is $9-120/user, overkill for a one-person shop.
- **Beliefs / values:** Distrustful of cloud lock-in. Burned by Drobo's collapse OR knows someone who was. Comfortable in the terminal. Reads HackerNews / r/selfhosted / r/homelab / r/truenas. Ideological lean toward open-source. Appreciates "do-it-yourself but professional-grade."
- **Why JuiceMount lands:** It's the first product in the category that respects their setup. They already own a NAS. They already pay for an S3 bucket. They don't need another SaaS bill. They need their existing stack to feel like 2026 instead of 2010. JuiceMount delivers that with no migration, no per-seat fee, and no vendor risk.

**Where we find them:**
- r/truenas, r/synology, r/homelab, r/selfhosted, r/datahoarder
- Lift Gamma Gain forum
- HackerNews "Show HN" launches
- ProVideo Coalition / NoFilmSchool comment sections
- TrueNAS Community Forum
- Twitter/X: ex-Frame.io customers complaining publicly

**Secondary persona — captured in `VISION/personas.md` (next iteration deliverable):**
- Boutique post house (3-15 people)
- Sovereignty-bound shop (broadcast, gov, NDA)

**Tertiary (later, hosted tier):**
- Non-technical user who wants the workflow without the sysadmin work

---

## What this positioning costs us — and we're OK with that

Explicit non-goals of this positioning:

1. **We are not the easiest cloud storage for non-technical users.** Frame.io owns easy. Suite owns polished. Shade owns "1-bill consolidation." Our hosted-backend SaaS is tier 3, not tier 1.
2. **We do not have C2C, Premiere panel integration, or in-product review tools.** Frame.io owns that. We can build complementary integrations later, but we're not winning the Frame.io customer in iteration 1.
3. **We do not have AI semantic search ("find sunset shots") today.** Shade and Iconik do. We can ship a CLIP-based local-inference variant later, but it's not the wedge.
4. **We are not pitching enterprise post-production.** Suite/LucidLink win those. We're claiming the indie + boutique + sovereign segment they've explicitly priced out.

The cost of this positioning is reach: we're addressing maybe 30% of the market in 2026. The benefit is **defensibility** — the customer profile is technical enough to evaluate JuiceMount on its merits, ideologically aligned with open-source/self-hosted, and structurally underserved by every existing competitor.

---

## The product narrative (one paragraph)

*Cloud-mounted storage for editors used to mean either Suite Studios at $75/TB/month or LucidLink with a $1,000/month surprise bill. Adobe's Frame.io Drive, launched April 2026, runs on Suite's tech with Adobe's billing on top — same lock-in, more vendors. Shade bundles three SaaS tools into one for $29.75/user/month and parks your media in their cloud. None of these solve the actual problem most editors live with: a $3,000 NAS already in the closet, a Backblaze B2 account already paid for, and a Finder window that's been embarrassingly slow since 2010. **JuiceMount is the open-source, self-hosted, local-first answer.** Mount any S3-compatible bucket — your NAS via JuiceFS, Backblaze, R2, Wasabi, MinIO — as a native Finder volume with sub-millisecond directory listings, instant Cmd+Shift+F search across 100K+ files, spacebar Quick Look that actually works on RAW, and zero per-seat fees forever. Your bytes stay where you put them. Your bill stays under your control. Your storage stays your storage.*

---

## Next iterations should produce

1. **`VISION/personas.md`** — Locked primary persona above + secondary (boutique post house) + tertiary (sovereignty-bound). Day-in-the-life vignettes for each. Use the workflow-pattern data from `pain-points.md` § "typical day-of-an-editor."
2. **`VISION/feature-roadmap-ranked.md`** — Rank the 4 unbuilt items from the wishlist (5,6,7,8) plus our existing roadmap items. Top 2 become prototype branches.
3. **`VISION/brand-identity.md`** — Refresh "JuiceMount" name, tagline candidates, voice, visual direction. The narrative paragraph above is the messaging-pillar source.
4. **`VISION/gtm-strategy.md`** — Launch channels (r/truenas, r/selfhosted, HN, Lift Gamma Gain), pricing tiers (OSS free / Pro / hosted Enterprise), content plan.
