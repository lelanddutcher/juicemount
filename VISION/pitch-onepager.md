# JuiceMount — Investor One-Pager

> **One-page version. Use this for cold intros, cold emails, accelerator apps. Long-form deck lives elsewhere.**

---

## The problem

Every working video editor is paying too much for storage that doesn't actually solve their problem.

- **The cloud option** (Suite Studios at $75/TB/month, LucidLink at $1,000/month for 10TB, Frame.io Drive bundled into Adobe Creative Cloud) is fast and slick — but expensive, lock-in-y, and dies the moment the internet does. Trustpilot rates Frame.io at 1.4/5. Capterra reviewer Michael W., Feb 2025: *"When I hit 1 TB of usage my service was completely cut off without any warning."*
- **The hardware option** (LumaForge Jellyfish at $5K-$50K, Synology / QNAP / TrueNAS NAS) is cheap per byte but a UX disaster. NAS Compares 2024: *"NAS is often set up by IT staff who configure things for 'optimal' document work like spreadsheets and Word files, which is absolutely not tenable for video production."* Drobo went bankrupt in 2023 and stranded its customers.
- **The asset-search option** (Iconik at $9-$120/user/month, Shade at $29.75/seat/month) requires a separate subscription on top of storage. Your search lives in their browser tab; your bytes live wherever.

Working editors lose **30-60% of their day finding footage** (FrameQuery internal research). They want one bill, instant search, RAW Quick Look that works, and verified backups. **Nobody on the market today gives them that.**

## The product

**JuiceMount** is open-source pro-video shared storage. Mount any S3-compatible bucket — your own MinIO on a NAS in the closet, Backblaze B2 at $6/TB, Cloudflare R2 with zero egress, or Wasabi — as a native macOS Finder volume.

The differentiation is in the layer above the protocol:

1. **Sub-50ms full-text search** across 100K+ files via SQLite FTS5. No browser, no cloud roundtrip — Cmd+Shift+F from any app.
2. **Spacebar Quick Look on RAW** (R3D, ARRI, BRAW, ProRes RAW) via on-the-fly VideoToolbox proxy generation. Frame.io Drive's own optimization docs admit they can't do this. (**Working prototype shipping in v1.**)
3. **Content-hash backup verification** with traffic-light status per file and a "safe to delete" check. Detects silent bit-rot that rsync misses. (**Working prototype shipping in v1.**)
4. **Real-time multi-machine sync** via Redis pub/sub. Move a file on one machine, the other Macs see it within 100ms.
5. **Three-tier read path** — memory buffer → SSD cache → backend storage — so 4K media plays at LAN speed even from a remote bucket.

Open-source MIT-licensed client. No phone-home. Auditable. Works with Resolve, Premiere, FCPX, Avid — anything that reads a POSIX filesystem.

## The wedge — what makes this different from the 6 competitors above

**Two of the four cloud-streaming products (Suite Studios and Frame.io Drive) are the same proprietary engine.** Adobe didn't build their own filesystem; they OEM'd Suite's tech. That means **lock-in is the entire competitive set's structural model** — and JuiceMount is the only product where the customer can `mc cp s3://my-bucket/ ~/Movies/` and walk away with their original bytes intact.

At 10TB / 5 editors, JuiceMount + Backblaze B2 costs **$60/month**. Suite Storage managed costs **$750**. Frame.io Drive Pro costs **$75 + an Adobe CC subscription**. We're 6-12× cheaper, with full data sovereignty, and we own the workflow layer the competitors leave on the table.

## Traction (current state, May 2026)

- Working macOS menu bar app: NFS server + JuiceFS FUSE + SSD cache + memory buffer + FTS5 search + Quick Look preview + structured logging + /metrics endpoint. Codesigned. Auto-mounts on launch.
- Two production-quality prototype branches with working code:
  - `prototype/codec-aware-quicklook` — RAW proxy generation with VideoToolbox (1,078 LOC, 11/11 tests passing)
  - `prototype/backup-verification` — content-hash verification with silent-corruption detection (1,502 LOC, 18/18 tests passing)
- Comprehensive competitive intelligence: 6 deep competitor briefs (~16K words), 4,500-word user-pain-points report sourced from named editor quotes on Capterra, G2, Lift Gamma Gain, Creative COW.
- Locked positioning, three personas, brand identity, ranked feature roadmap. **Ready to launch in 30 days.**

## Market

- **TAM:** professional post-production storage globally. Suite + LucidLink + Iconik + Shade + Frame.io bookings are roughly $300-500M ARR combined (estimated from public funding + observed pricing). Hardware NAS appliances at the indie tier represent another ~$200M annual category. Combined opportunity: **$500M-1B+ ARR**.
- **SAM (year 1-2):** the indie + boutique post house segment that the cloud incumbents have priced out. Approximately **150,000 individual editors and ~20,000 small post shops in the US alone**, growing globally. At Pro pricing ($60/year): **$9M annual opportunity per 1% market share**.
- **Adjacent:** the homelab / self-hosted enthusiast crossover (TrueNAS has 800K+ active installations) — these are JuiceMount's natural early adopters, and many of them are also working video editors or have one in the family.

## Business model & financials

| Tier | Price | Target customer | Year-2 customers | Year-2 ARR |
|---|---|---|---|---|
| Free | $0 | OSS adoption funnel | ∞ | $0 |
| Pro | $50/year | Indie editor (P1 persona) | 800 | $40K |
| Team | $250/year per machine | Boutique post house (P2) | 100 × ~5 machines = 500 | $125K |
| Loft (hosted) | $50-500/mo | Non-technical / managed (P2/P3) | 700 × $200 avg = $1.68M | $1.68M |
| Enterprise | $5K-50K/year | Sovereign / broadcaster (P3) | 3-5 | $50K-200K |
| **Total Year 2** | | | | **~$2M ARR** |

Year-1 ARR target: **$180K** (conservative). Year-2: **~$2M** (with Loft hosted launch).

## Team

**Leland Dutcher (founder, solo).** Video production engineer with deep technical chops in distributed systems (built JuiceMount's NFS server, FUSE integration, SQLite FTS5 layer, Swift menu bar app, codec-aware proxy generator, backup verification engine — all while working full-time as a video engineer at his own production company). Domain expertise + customer empathy + can ship.

**Hiring plan:** stay solo through year 1. Year 2: 1 Swift/macOS engineer + 1 founder's-friend marketer/community lead.

## The ask

**Seed round: $750K — $1.5M at a $7M-$12M post.**

Use of funds:
- 12 months runway for Leland (~$200K)
- 1 senior macOS/Swift engineer hire ($250K loaded for year 1)
- Loft hosted infrastructure (initial $30K, scales with revenue)
- Marketing budget for NAB 2027 + content + sponsorships ($75K)
- Buffer for hosting, legal (open-source license review), accountant, contingencies

What this funds: the gap between "OSS launch with $725 MRR at month 3" and "self-sustaining $60K MRR at month 24." The product works today; the funding accelerates GTM and lets us ship Loft + Team without diverting Leland to fundraising every 6 months.

## Why now

- **Drobo's collapse (2023)** stranded thousands of customers and primed the market to value open / portable storage formats.
- **Suite Studios + Frame.io Drive consolidation (April 2026)** shows the cloud-mount category is real and mainstream — but consolidates lock-in. We're the open answer.
- **Apple Silicon hardware acceleration** (VideoToolbox, FSKit, fast NVMe) makes our local-first architecture structurally faster than cloud streaming for the cases that matter.
- **TrueNAS, Proxmox, and self-hosted have crossed the chasm** — there's a 1M+ developer/prosumer audience that already runs the infrastructure JuiceMount targets.
- **Frame.io review-tool fatigue + LucidLink price-creep complaints** are at peak intensity in editor communities (sourced from `pain-points.md`). The market is actively shopping.

## Why us

- **Founder is the customer.** This product exists because Leland built it for himself. Every architectural decision was forced by a real workflow, not an investor deck.
- **Open-source distribution moat.** Once 5,000+ editors install OSS and trust their masters to JuiceMount, switching cost is genuine and lock-in works in our favor (without us being predatory).
- **Cost structure dominates.** We don't run inference, we don't pay CDN egress, we don't burn AWS instance-hours. Every $50/year Pro user is ~$48/year of margin after Stripe fees. Loft tier has real infra costs but still 60-70% gross margin at scale.
- **Aligned with where the industry is going:** sovereignty, portability, anti-lock-in. Frame.io Drive's launch validates the category; JuiceMount's architecture is the only credible answer to its lock-in.

---

**Contact:** Leland Dutcher · leland@lelanddutcher.com · github.com/lelanddutcher/juicemount
