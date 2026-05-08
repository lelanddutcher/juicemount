# Landing Page Copy — JuiceMount

> **Production-ready copy for juicemount.io.** Voice: Tailscale + Backblaze + Linear (per `brand-identity.md`). Headline anchors on the locked tagline. Real editor quotes from `pain-points.md`. Pricing matches `gtm-strategy.md`.

---

## Hero

### Headline
**Your bytes are your bytes.**

### Subhead
Open-source pro-video shared storage that runs on your own NAS or any S3 bucket. Mount it like a Finder volume. Search 100,000 files in 50 milliseconds. Spacebar a 4K R3D and watch it play. No SaaS bill, no Adobe subscription, no vendor that can pull the rug.

### Primary CTAs
[Download for macOS] [⌘ Star on GitHub]

### Secondary line (small, under CTAs)
Free forever. MIT-licensed. Works with Resolve, Premiere, FCPX, Avid.

---

## Section 1 — The product (what it actually is)

### H2: A Finder volume that's smarter than Finder.

JuiceMount mounts any S3-compatible bucket — your own MinIO on a NAS in the closet, Backblaze B2, Cloudflare R2, Wasabi, AWS — as a native macOS Finder volume. Then it does three things no shared-storage product does:

**🔍 Cmd+Shift+F · search across the whole library**
Type any filename. Results in 50 milliseconds across 100,000 files. The index lives on your Mac, not in someone's browser tab. Spacebar previews. Drag results into your timeline.

**▶️ Spacebar · Quick Look on R3D, ARRI, BRAW, ProRes RAW**
Hover over a 4 GB camera RAW. Hit space. Watch it play, smoothly. JuiceMount generates a 720p H.264 proxy on first read using VideoToolbox (1280×720 in <1 second on Apple Silicon) and caches it. Frame.io Drive's own docs say their mount can't do this; we built it because the workflow demands it.

**🛡️ Backups you can actually trust**
JuiceMount walks every backup target you configure — your NAS, your USB drive, your B2 bucket, your LTO archive — and computes SHA-256 hashes. Each file gets a green/yellow/red status. Same file size on the backup with different bytes? Red. The "Safe to delete?" check refuses unless ≥2 OTHER targets verify the same content. Toy Story 2 trauma, solved.

---

## Section 2 — Why it exists (the wedge)

### H2: The cloud-mount category just consolidated. We're the open answer.

In April 2026, Adobe shipped Frame.io Drive. The streaming engine inside it isn't Adobe's — it's Suite Studios, OEM'd. So the two flagship cloud-storage-for-editors products are now the same proprietary substrate. Your masters live in someone else's filesystem. The day Suite or Adobe pivots, the bill jumps, or AWS us-east-1 goes down, your project is stranded.

JuiceMount is the only product in the category where you can run `mc cp s3://my-bucket/ ~/Movies/` and walk away with your original bytes intact. The mount client is open source. The metadata is in plain SQLite. The data is in your bucket. **No extraction tool. No migration script. No negotiation.**

> *"When I hit 1 TB of usage my service was completely cut off without any warning or explanation."*  
> — Michael W., LucidLink customer ([Capterra, Feb 2025](https://www.capterra.com/p/196912/LucidLink/reviews/))

> *"Ever since the Adobe takeover, frame.io is a total mess."*  
> — Perry Paolantonio, [Lift Gamma Gain forum](https://www.liftgammagain.com/forum/index.php?threads/does-frame-io-suck-for-you-now-too.16912/)

> *"NAS is often set up by IT staff who configure things for 'optimal' document work like spreadsheets and Word files, which is absolutely not tenable for video production."*  
> — [NAS Compares 2024 editing guide](https://nascompares.com/guide/complete-guide-to-video-editing-on-a-nas-2024-edition/)

---

## Section 3 — Pricing (real numbers, no contact-sales)

### H2: 10 TB, 5 editors, monthly cost.

| | Monthly | Storage you control |
|---|---|---|
| **JuiceMount Free + your B2 bucket** | **$60** | ✓ |
| **JuiceMount Pro × 5 + B2** | **$85** | ✓ |
| **JuiceMount Team + B2** | **$185** | ✓ |
| **JuiceMount Loft 10TB** (managed) | **$200** | Managed by us |
| Suite Storage (managed) | $750 | ✗ |
| Suite Storage BYO + B2 | $460 | partial |
| Frame.io Drive Pro × 5 | $75 + Adobe CC | ✗ |
| Shade Growth × 5 | $148.75 | partial |
| LucidLink (10 TB tier) | ~$1,000 | ✗ |
| Iconik × 5 (Pro) + storage | $325 + storage | layer only |

JuiceMount + Backblaze B2 is **6–12× cheaper** than the cheapest cloud alternative, with full data sovereignty. No per-seat tax. No surprise overage bills. The math is just storage + Stripe.

### Tier details

**Free** — $0 forever. Full NFS server, FUSE, search, standard-codec Quick Look. GitHub support.

**Pro** — $5/month or $50/year per individual. Adds RAW Quick Look proxies, backup verification, bandwidth fallback, priority bug fixes. Covers all your machines (laptop + studio + NAS = one license).

**Team** — $25/month or $250/year per active machine. Adds multi-user presence + cooperative locking, centralized config, 48-hour email support. Up to 15 machines.

**Loft (hosted)** — $50–$500/month, tiered by storage. Managed Redis + MinIO backend. Zero setup. Coming Q4 2026.

**Enterprise** — custom. Self-hosted, air-gappable, source escrow, named SE. For broadcasters, gov media, and legal-bound shops.

---

## Section 4 — How it works (for the curious)

### H2: Three layers. Two open-source. One you choose.

**Layer 1 — JuiceMount client (your Mac).** Open-source MIT-licensed. NFS server bound to localhost. SQLite metadata cache with FTS5 trigram search. SSD cache reader for direct pread bypass of FUSE. Memory buffer for small files. Codesigned for macOS Sonoma+.

**Layer 2 — JuiceFS (the protocol).** Open-source Apache-2.0 from Juicedata. Maps a POSIX filesystem onto Redis (metadata) + S3 (objects). 10+ years of production hardening at scale.

**Layer 3 — your storage.** Anything that speaks S3: Backblaze B2 ($6/TB), Cloudflare R2 (zero egress), Wasabi ($6.99/TB), AWS, GCP, Azure, MinIO running on your TrueNAS / Synology / Mac mini. Or our managed Loft tier when it ships.

The architecture is the moat. Suite + Frame.io own the layer-1 client and force you to use their layer 3. Iconik adds a fourth layer for search. Shade bundles all three but locks you in. **JuiceMount unbundles. You own every layer or pick a vendor for any one of them.**

---

## Section 5 — FAQ

**Q: Will this work with my Synology / TrueNAS / QNAP?**
Yes. JuiceMount needs S3-compatible storage; MinIO runs on every NAS as a Docker container. We have a one-page setup guide for TrueNAS Docker that takes <15 minutes.

**Q: Does it work with Premiere / Resolve / FCPX / Avid?**
Yes — anything that reads a POSIX filesystem. JuiceMount mounts as a Finder volume. The NLE doesn't know it's not local.

**Q: How does it compare to LucidLink?**
LucidLink is a closed-source SaaS that streams from their cloud filesystem and bills you per-TB-per-month. JuiceMount is open source, runs against any S3 bucket you own, and has a one-time install with no recurring fee at the Free tier. The architectures are different (we cache aggressively on local SSD; LucidLink is more streaming-first) but the user-facing UX is similar.

**Q: How does it compare to Frame.io Drive?**
Frame.io Drive is OEM'd from Suite Studios with Adobe's account/billing layer on top. It requires an active Creative Cloud subscription, locks your data into Adobe's cloud, supports one project mounted at a time, and costs $15+/seat/month bundled. JuiceMount has no Adobe dependency, no subscription required, supports any S3 backend, and runs as many concurrent projects as you have storage for.

**Q: Is my data safe? Where does it actually live?**
Wherever you put it. Your Backblaze bucket. Your TrueNAS box. Your Mac mini's external SSD. JuiceMount stores files as plain S3 objects — chunked by JuiceFS, not encrypted by us. If you ever want to leave, `mc cp` and you walk away with everything.

**Q: Does it work offline?**
Yes for cached files. JuiceMount keeps an SSD cache of recently-accessed bytes. Files in cache play offline. Files not in cache need network to fetch. Plug back in when convenient.

**Q: What macOS versions are supported?**
macOS 14 (Sonoma) and macOS 15 (Sequoia). macOS 26 support coming with FSKit when Apple ships it.

**Q: Windows? Linux?**
On the roadmap. Expected: Windows H1 2027, Linux Q3 2027. Help us prioritize — vote on the GitHub roadmap.

**Q: What happens if JuiceMount the company goes away?**
Your data stays. The OSS client keeps working — fork it on GitHub, run forever. The hosted Loft tier would be at risk, but Loft is on top of standard S3-compatible buckets you can switch away from on a day's notice.

**Q: Is there a free trial?**
The OSS Free tier is the trial. Use it forever. Upgrade to Pro when you actually want the features (RAW proxies + verification).

**Q: How do I install?**
Download the .app, drag to /Applications, open. Set the Redis URL + bucket info in Preferences. The first run does the initial sync and you're working. Setup guide at [docs.juicemount.io](https://docs.juicemount.io).

---

## Section 6 — Closing CTA

### H2: Stop renting your storage workflow.

JuiceMount is free. Always will be at the OSS tier. The whole company exists because Leland was tired of paying $1,000/month for LucidLink, $20/seat for Frame.io, and $9/user for Iconik to do what a NAS he already owned should have been doing.

**Try it for 30 days. If your editing workflow doesn't feel measurably better, delete the app.** No bill. No migration to undo. Your data stays where it always was.

[Download for macOS — 14 MB] [⌘ Star on GitHub] [📖 Read the docs]

---

## Footer

JuiceMount is open source under the MIT license. Hosted Loft tier launching Q4 2026.  
Made by [Leland Dutcher](https://lelanddutcher.com), one human who got fed up.

[GitHub](https://github.com/lelanddutcher/juicemount) · [Docs](https://docs.juicemount.io) · [Discord](https://discord.gg/juicemount) · [Pricing](#pricing) · [Twitter/X](https://twitter.com/juicemount)

---

## Notes for the designer (not on the rendered page)

- **Hero treatment:** dark background (oil/slate from `brand-identity.md`), juice-orange accent on the headline word "your" and the primary CTA button. Subhead in parchment color.
- **Code-feel for the price comparison table:** JetBrains Mono for the numbers, monospaced alignment. Use signal-green for JuiceMount rows, normal text for competitor rows.
- **Section 4 (architecture):** consider a 3-layer diagram. Layers 1 and 2 are coded green ("open"), layer 3 is parchment with vendor logos floating beside it ("yours").
- **Quotes in Section 2:** small italic, with avatar/icon for source platform. NOT testimonials yet — these are anti-competitor citations from public reviews.
- **One-line design rule from brand-identity.md** ("Boring on the outside, fast on the inside") — apply to every layout decision. No animations on hover, no scroll-jacking, no gradient buttons. Functional and direct.
- **OG image / Twitter card:** the spacebar-over-R3D moment, screenshotted from the demo. Caption: "Your bytes are your bytes."
