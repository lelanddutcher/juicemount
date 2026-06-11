# RESEARCH ROUND 2 — Competitive Facts Pack

**Date compiled:** 2026-06-11
**Purpose:** Drive comparison-table overhaul, calculator pricing, and performance-page section.
**Method:** Live web search + page fetches. Every fact carries a source URL and checked-date. Contradictions on the public web are flagged inline. "Quote-only" = vendor hides pricing (page cited as proof).

**Status:** All 8 sections complete (2026-06-11).

---

## 1. Strada (strada.tech) — what is it, does it belong in the table?

**VERDICT: Strada in mid-2026 is a peer-to-peer remote-media-access tool ("be your own cloud"), NOT storage SaaS — it sells transfer/access, not storage. Different category; put it in a "different approach" footnote row at most, not the core storage-SaaS comparison.**

### What it is today

- Hero positioning (strada.tech homepage): headline **"Access your media without moving it."** / subhead "Edit, share, and collaborate on remote media as if it lives on your desk." Explicitly: **"We don't rent storage. We offer the protocol that makes yours work."** Files stay on the customer's own drives/NAS/RAID; collaborators get remote access. (https://strada.tech/ — checked 2026-06-11)
- **Strada 2** (announced Cine Gear 2026, shipping June 2026 for Apple Silicon + Windows): peer-to-peer remote access with a Finder/Explorer-style file browser; remote media usable in DaVinci Resolve, Premiere Pro, Lightroom; supports 12K Blackmagic RAW and 8K/12K REDCODE playback over the open internet; transfer encrypted; per-file permission controls. (https://www.cined.com/strada-2-first-look-local-drives-remote-editing/ — checked 2026-06-11; also https://www.newsshooter.com/2026/06/03/strada-version-2-0/ and https://www.redsharknews.com/strada-version-2-remote-collaboration)
- History/pivot note: Strada launched 2023–24 as an "AI-enabled cloud platform" with auto-tagging/transcription (Michael & Peter Cioni, $1.9M pre-seed — https://www.hollywoodreporter.com/business/digital/michael-peter-cioni-ai-production-tools-startup-strada-1235580867/). The mid-2026 homepage no longer leads with AI features at all; the pitch is now cloud-free P2P access ("Strada Agents" was the interim remote-access product — https://nofilmschool.com/strada-agents). Treat "Strada = AI tool" as stale.

### Pricing (https://strada.tech/pricing — checked 2026-06-11)

| Tier | Price | Transfer cap | Notes |
|---|---|---|---|
| Free | $0 | share/receive up to 15 GB/mo | unlimited connected drives, no storage limit, unlimited viewing |
| Basic | **$8/user/mo** (33% off billed annually) | up to 250 GB/mo | adds "Strada Connect" remote editing; can buy licenses for up to 9 teammates |
| Unlimited | **$24/user/mo** (20% off billed annually) | unlimited | adds view-only/enhanced permissions; Enterprise quote separately |

- Pricing is metered on **transfer volume per month**, not storage — there is no $/TB stored anywhere, because Strada stores nothing. 7-day unlimited trial, no card.

### Category call for the comparison table

- Strada does NOT sell "mounted-drive-style cloud storage to editors." It sells a P2P access protocol over storage the customer already owns. No central cloud copy, no multi-site object store, no storage durability story — if the source drive is off or dies, the media is unreachable.
- It IS conceptually adjacent to JuiceMount ("stop renting storage") but solves it with P2P instead of self-hosted S3. The honest framing: Strada = remote access to one person's drives; JuiceMount = a shared always-on volume on storage you control.

**Site implications:** Do not put Strada in the per-seat storage-SaaS table rows — its $8/$24 numbers are transfer prices and would mislead. Add a short "Other approaches" note (Strada: P2P, no central storage, source machine must be online) and consider an honest "where Strada is a better fit" sentence — credibility play.

---

## 2. Suite Studios — current status; same product as Iconik?

**VERDICT: No — Suite and Iconik are entirely separate companies and different product categories. Suite Studios (Boulder, CO) is independent, operating, and freshly funded ($12.5M Series A, April 2025); Iconik is a MAM owned by Backlight (PSG-backed rollup) since April 2022. No shared ownership or technology found. "Suite is irrelevant" is not supported by the public record.**

### Suite Studios status (mid-2026)

- Operating normally and independent. Site live, support KB current; status article (2025-11-19) reports "All systems are currently 100% operational" — the only incident discussed is the Oct 20, 2025 AWS us-east-1 outage (Suite runs on AWS). (https://support.suitestudios.io/en/articles/12622709-status-of-suite — checked 2026-06-11)
- Funding: **$12.5M Series A, April 17, 2025** (Grotech Ventures, S3 Ventures, Bonfire Ventures, Range Ventures, Massive); $21.5M total across 4 rounds. No acquisitions made or received. (https://tracxn.com/d/companies/suite-studios/__TWBx31iL_vyj6j8ft6vrS9f_WWseLZzapsTArSuVKbo, https://www.crunchbase.com/organization/suite-studios — checked 2026-06-11)
- Category: "Cloud Storage With File Streaming" — a streaming storage volume for editors (closest analog: LucidLink), NOT a MAM. (https://www.suitestudios.io/ — checked 2026-06-11)

### Why it's not Iconik

- Iconik = media asset management (search/proxies/metadata/review), Stockholm-born, acquired by **Backlight** April 12, 2022 (Backlight: $200M+ PSG-backed media-tech holding co that also owns ftrack, Celtx, Gem, etc.). (https://www.iconik.io/news/iconik-announces-its-acquisition-by-backlight-a-new-global-media-technology-company, https://www.businesswire.com/news/home/20220412005463/en/ — checked 2026-06-11)
- No overlap in ownership, no partnership/tech-sharing announcements found in either company's newsroom or the trade press. Probable source of confusion: both pitch "cloud post-production workflows," and Iconik also pairs with storage via its gateway (see §3) — but Suite mounts storage, Iconik indexes it.

### Suite pricing today (https://www.suitestudios.io/pricing — checked 2026-06-11)

| Plan | Price | Seats | Notes |
|---|---|---|---|
| Managed | **$75/TB/mo**, month-to-month | 5 included, **$10/user/mo** extra | AES-256, MFA, "Instant Archive", **"Onsite Caching"** listed as a feature; 14-day trial |
| BYO storage | **$40/TB/mo** (Suite fee only) | 5 included, $10/user/mo extra | **Minimum 20 TB active storage**; customer must already have a CSP account (AWS, GCP, Azure, IBM, Cloudflare, Backblaze supported); CSP storage/egress billed separately on top (https://support.suitestudios.io/en/articles/8302694-what-is-bring-your-own-storage-byo, last updated 2025-10-10 — checked 2026-06-11) |
| Enterprise | quote-only | custom | (https://www.suitestudios.io/pricing) |

- ⚠️ Contradiction on the web: one indexed snippet says BYO minimum is 15 TB; the current KB article (updated 2025-10-10) says **20 TB** — use 20 TB. Note the main pricing page now leads with Managed and shows "S3 Native — Coming Soon"; the $40 BYO tier is documented in the KB, not prominent on /pricing.
- Site's current pricing.json values for Suite ($75 managed, $40 BYO, 5 seats incl., $10 extra seat) **check out** — add the 20 TB BYO minimum, which the calculator currently ignores.

**Site implications:** Keep Suite in the table as a live, funded LucidLink-style competitor — do not describe it as defunct or as Iconik. Add "BYO requires 20 TB minimum + you still pay your cloud provider separately" to the comparison and calculator footnotes; that double-payment structure is a strong talking point for JuiceMount's flat self-hosted story.

---

## 3. On-prem / local-cache add-ons from the SaaS vendors

**VERDICT: The founder's instinct is right in structure, mostly wrong on billing detail. All four vendors solve WAN latency by running software on hardware the CUSTOMER buys — but only LucidLink gates it behind an enterprise add-on conversation; Iconik includes the basic gateway and sells a "Pro" upgrade; Suite and Shade ship it as a built-in client feature pointed at your own SSD/NAS. The performance-page line "their answer to WAN latency is selling you back your own hardware" is defensible for all four if phrased as "you supply the server."**

### Iconik Storage Gateway (ISG)

- What: lightweight app (Mac/Windows/Linux) bridging on-prem SAN/NAS/DAS to the iconik cloud — scans local storage, transcodes proxies/keyframes locally, uploads originals or just proxies, serves full-res files to local users while iconik indexes globally. (https://help.iconik.backlight.co/hc/en-us/articles/25304282498327-Iconik-Storage-Gateway-Overview, https://app.iconik.io/docs/isg.html — checked 2026-06-11)
- Deployment: customer's own hardware; supported on macOS (3 latest, Intel + Apple Silicon), Windows, Ubuntu 22.04/24.04, EL8/EL9; even packaged as a TrueNAS app (https://apps.truenas.com/catalog/iconik-storage-gateway/ — checked 2026-06-11).
- Price: standard ISG appears **included** with iconik plans; **"ISG Pro"** (higher throughput/failover) is listed as an Add-on with **no published price — quote-only** (https://www.iconik.io/pricing — checked 2026-06-11). Note iconik's consumption "credits" also meter storage, AI, and **egress**.
- Context: iconik Starter per-user pricing now: Collaborator $0, Browse $9/mo, Standard $65/mo, Power $120/mo; Professional/Enterprise quote-only. (https://www.iconik.io/pricing — checked 2026-06-11) ⚠️ This contradicts the site's current "iconik $35-ish/user" framing — Standard is $65 now.

### Suite Studios — "Onsite Caching" / shared cache

- What: Suite's client cache can be pointed at a shared on-prem server/NAS so a whole studio shares one cache ("if you have 75 machines… point the cache location of each machine to the same local server or NAS — that on-prem hardware now becomes a 'shared cache'"). Pre-cache (proactive, whole files/folders) + on-demand block cache. (https://blog.suitestudios.io/article/the-best-hybrid-dispersed-remote-workflow-easily-integrate-on-prem-hardware-suite-cloud-storage, https://support.suitestudios.io/en/articles/8022828-how-suite-storage-caches-intelligently, https://support.suitestudios.io/en/articles/8208172-pre-caching-files-and-folders — checked 2026-06-11)
- Hardware: customer-supplied; Suite explicitly requires **SSD, not HDD** for the cache, and publishes a "best cache drives to buy" shopping guide (https://support.suitestudios.io/en/articles/8780699-best-cache-drives-to-buy-for-maximum-performance — checked 2026-06-11).
- Price: no separate fee documented — "Onsite Caching" is listed as a feature of the $75/TB Managed plan (https://www.suitestudios.io/pricing — checked 2026-06-11). So: not "pay additional money to Suite," but you DO buy the SSD/NAS yourself.

### LucidLink — TeamCache + Connect + BYO S3

- **TeamCache**: "a local caching solution that brings LucidLink performance to branch offices and on-site teams." Customer supplies "a commodity on-prem server with SSD storage"; LucidLink "advise[s] on specs during setup." Enterprise-only, early release; "Add TeamCache as part of your LucidLink subscription… Talk to us to enable" — **quote-only, no published price** (https://www.lucidlink.com/teamcache — checked 2026-06-11; status: early release for Enterprise, https://www.lucidlink.com/blog/nab-2026).
- **LucidLink Connect** (announced ~2026-03-10): mounts EXISTING S3 buckets into the LucidLink filesystem without ingest — "making it instantly usable without forcing customers to change where it lives." Enterprise plans only; AWS Marketplace listing "coming soon"; Azure Blob planned; **no pricing published**. (https://www.blocksandfiles.com/object/2026/03/10/lucidlink-connect-streams-s3-buckets-without-the-ingest-headache/5208840, https://siliconangle.com/2026/03/05/exclusive-lucidlink-enables-collaboration-directly-object-storage/ — checked 2026-06-11) ⚠️ Strategic: this is LucidLink moving directly onto JuiceMount's "your own S3" turf, but gated to Enterprise quotes.
- **BYO/on-prem object storage**: LucidLink "Custom" supports any S3-API-compliant store, cloud or on-prem; qualified: Cloudian, Nutanix, Scality, Zadara, Ceph, **MinIO**. (https://siliconangle.com/2026/03/05/exclusive-lucidlink-enables-collaboration-directly-object-storage/, https://www.lucidlink.com/pricing — checked 2026-06-11)

### Shade — local cache + pinning (client-built-in)

- ShadeFS mounts the cloud drive as a local volume with intelligent block caching; **offline pinning is a documented feature** ("When you pin a file or folder, it downloads a complete copy outside your cache, making it available offline") (https://academy.shade.inc/shadefs/pinning-files-offline, https://academy.shade.inc/shadefs/managing-the-shadefs-cache — checked 2026-06-11).
- No separate paid on-prem cache appliance documented; BYO-storage exists for enterprise/data-residency cases (https://shade.inc/ enterprise messaging — checked 2026-06-11). Cache lives on the user's own disk.

**Site implications:** Performance-page section writes itself: every vendor's latency fix = customer-purchased SSD/server running vendor software, priced from "included" (Suite/Shade/ISG-basic) to "enterprise quote" (LucidLink TeamCache/Connect, ISG Pro). JuiceMount's framing: "we agree — local cache on your hardware is the answer; we just don't charge rent on top of it." Also URGENT: update iconik seat price ($65 Standard, not ~$35) and add a LucidLink Connect awareness note — their marketing now also says "use storage where it lives."

---

## 4. Shade pricing specifics

**VERDICT: One Shade seat is buyable (no stated minimum on Growth), but a 1-seat/5 TB library is NOT purchasable at published prices — Growth includes only 500 GB active storage per seat and shade.inc publishes no $/TB add-on or cold-storage rate. Offline pinning IS documented (founder right). Shade appears to have RAISED prices between April and June 2026; the site's $29.75 figure matches the current annual price.**

### Published tiers (https://shade.inc/pricing — checked 2026-06-11)

| Tier | Price | Included storage | Seats | Notes |
|---|---|---|---|---|
| Growth | **$35/seat/mo monthly; $29.75/seat/mo annual** | **500 GB active storage per seat** | "Up to 15 seats" — no stated minimum, 1 seat OK; up to 150 guests | 1 workspace, unlimited drives, unlimited AI indexing |
| Enterprise | quote-only ("Custom") | 1 TB active/seat | unlimited; up to 250 guests | SSO/SAML, SCIM, audit logs, migration |

- **No published price for additional active storage or cold/archive storage** on shade.inc/pricing — the page lists included allocations only (checked 2026-06-11). A widely-circulated figure of **$40/TB/mo for extra active storage** appears in search aggregations but I could NOT confirm it on a primary shade.inc page — treat as unverified. Shade's own pricing calculator (https://calculator.shade.inc/) is currently unreachable (**expired TLS certificate**, observed 2026-06-11 — small credibility detail).
- Cold storage: no published cold tier today; "Shade Vault" archival product is described as on Shade's 2026 roadmap (third-party: https://toolchase.com/tool/shade-ai/ — checked 2026-06-11). The active-vs-cold distinction exists in their docs ("active storage" is what's streamed/counted) but there is no buyable cold $/TB on the public site.

### ⚠️ Web contradicts itself on Growth price (flag for table footnote)

- shade.inc/pricing today: $35 monthly / $29.75 annual (checked 2026-06-11) — authoritative.
- TechCrunch funding piece (2026-04-22): "a $20 per seat, per month plan… 500GB of active storage per seat" (https://techcrunch.com/2026/04/22/shade-lands-14m-to-let-creative-teams-search-their-video-libraries-in-plain-english/).
- BlockSentient 2026 review: "$20/seat/month (annual) or $25/seat/month (monthly)" (https://blocksentient.com/review/shade/).
- Most consistent reading: Shade raised prices ~Q2 2026 (post-raise) from $25/$20 to $35/$29.75. Cite shade.inc as of 2026-06-11 in the table.

### Other facts

- Funding: **$14M round led by Khosla Ventures, Construct Capital, Bling Capital (closed March 2026, announced 2026-04-22); $20M total** — Shade is well-funded and pushing AI search ("natural language search… exact moment in the video"), not just storage. (https://techcrunch.com/2026/04/22/shade-lands-14m-to-let-creative-teams-search-their-video-libraries-in-plain-english/ — checked 2026-06-11)
- **Offline pinning / local sync: documented.** "When you pin a file or folder, it downloads a complete copy outside your cache, making it available offline." (https://academy.shade.inc/shadefs/pinning-files-offline — checked 2026-06-11; cache management: https://academy.shade.inc/shadefs/managing-the-shadefs-cache)
- Worked example for the calculator: 1 editor with a 5 TB library on Growth = $29.75/mo seat + 500 GB included + **4.5 TB at an unpublished rate** → effectively "call sales." If the unverified $40/TB held, that's ~$210/mo for one editor — versus self-hosted B2/Wasabi at ~$30-35/mo for 5 TB raw. Use only with the "unverified" caveat or omit the $40 number.

**Site implications:** pricing.json's `shade_growth.seat: 29.75` is still correct (annual). Add `seat_monthly: 35` and an explicit "active storage only — extra TB unpublished/quote" flag; the calculator currently implies Shade scales linearly with storage, which understates Shade's real cost story for >500 GB/seat libraries.

---

## 5. Dropbox / Google Drive / Nextcloud (familiar anchors)

**VERDICT: All three confirmed as whole-file-download systems — none can scrub a 100 GB file without fully hydrating it locally. Dropbox Advanced $24/u/mo (3-user min, 15 TB pooled start); Google Workspace Business Plus $22/u/mo (5 TB/user pooled); Nextcloud free to self-host, Enterprise support ~€68/user/yr with a 100-user floor.**

### Dropbox (https://www.dropbox.com/business/plans-comparison — checked 2026-06-11)

- **Standard: $15/user/mo**, storage "starts at 3,000 GB for the team" (pooled), 1+ users.
- **Advanced: $24/user/mo** (annual; ~$30 monthly per aggregators — https://www.cloudwards.net/dropbox-pricing/, checked 2026-06-11), "starts at 15,000 GB for the team" (pooled), **3+ user minimum** (≈$864/yr floor). Storage scales ~5 TB per active license (https://www.vendr.com/marketplace/dropbox — checked 2026-06-11).
- **Streaming semantics: whole-file.** Dropbox's own help: online-only files keep only metadata locally; "When you open an online-only file… it will automatically download" — the ENTIRE file downloads to disk before the app can use it (https://help.dropbox.com/sync/make-files-online-only, https://help.dropbox.com/sync/online-only-mac — checked 2026-06-11). Known annoyance: background processes (antivirus, backup, file indexers) can trigger full downloads of online-only files (https://www.dropboxforum.com/discussions/101001012/smart-sync-keeps-downloading-online-only-files/384338 — checked 2026-06-11).
- 100 GB scrub test: **fail** — full 100 GB hydration before first frame.

### Google Drive / Workspace (https://workspace.google.com/pricing — checked 2026-06-11 via aggregators)

- Business Standard ≈ **$14/user/mo** (2 TB/user pooled); **Business Plus $22/user/mo annual / $26.40 flexible, 5 TB/user pooled**; storage pools across the org. (https://www.cloudwards.net/google-workspace-plans-and-pricing/, https://www.emailtooltester.com/en/blog/google-workspace-pricing/ — checked 2026-06-11). Enterprise tiers: quote-only.
- **Streaming semantics: whole-file on open.** Drive for desktop "Stream files" mode keeps files in the cloud and downloads them into a local cache when opened — there is no block-level partial read for arbitrary apps; mirror mode downloads everything (https://support.google.com/drive/answer/13401938, https://support.google.com/drive/answer/10838124 — checked 2026-06-11). Community reports of stream mode downloading aggressively: https://support.google.com/drive/thread/255451275 (checked 2026-06-11). Offline requires explicit per-file "Available offline."
- 100 GB scrub test: **fail** — file fully downloads to the Drive cache on open.

### Nextcloud (https://nextcloud.com/pricing/ — checked 2026-06-11)

- **Server license: free** (AGPL, self-hosted; you pay only your own hardware).
- **Nextcloud Enterprise** (support subscription): **Standard ≈ €68/user/yr, Premium ≈ €100/user/yr, Ultimate ≈ €195/user/yr, subscriptions start at 100 users** — i.e., the paid tier is irrelevant to a 3-editor shop. ⚠️ Aggregators disagree on exact cents (€67.89 vs €68.94 Standard) — cite nextcloud.com/pricing live. (https://nextcloud.com/pricing/, https://toolradar.com/tools/nextcloud/pricing — checked 2026-06-11)
- **Performance ceiling class: sync-and-share, not a streaming filesystem.** Desktop client syncs whole files with 100 MiB default chunking (https://docs.nextcloud.com/server/stable/admin_manual/configuration_files/big_file_upload_configuration.html — checked 2026-06-11). Documented large-file pain: hardcoded-timeout sync failures on big files (https://github.com/nextcloud/desktop/issues/5394), macOS Virtual Files failing uploads >~100 MB (https://github.com/nextcloud/desktop/issues/9883), chunked-upload throughput penalties (https://github.com/nextcloud/server/issues/47682) — all checked 2026-06-11.
- 100 GB scrub test: **fail** — VFS placeholders hydrate the entire file; no range-read streaming.

**Site implications:** These three make a great "familiar anchor" table row trio: cheaper per seat than the video-SaaS tools but architecturally whole-file — perfect foil for the block-streaming pitch. Use Dropbox's and Google's OWN help-page language ("will automatically download") in the comparison footnotes for defensibility. Nextcloud row protects the flank ("isn't self-hosting just Nextcloud?" — no: wrong architecture for video, with linked GitHub issues as receipts).

---

## 6. S3 egress / exit economics ("leave whenever" story)

**VERDICT: Pulling a 20 TB library out costs ≈ $1,741 on AWS S3, $0 on Backblaze B2 (within 3x-storage allowance), $0 on Cloudflare R2 (always), $0 on Wasabi (within reasonable-use). Among the SaaS tools, iconik explicitly bills egress via credits; LucidLink bundles egress into plan price; Suite BYO pushes egress to your CSP; no documented exit fee at Shade.**

### Provider egress rates (checked 2026-06-11)

| Provider | Egress policy | Source |
|---|---|---|
| AWS S3 | First 100 GB/mo free (account-wide), then **$0.09/GB to 10 TB, $0.085/GB next 40 TB, $0.07/GB next 100 TB, $0.05/GB beyond** | https://aws.amazon.com/s3/pricing/ (rates corroborated: https://www.cloudzero.com/blog/s3-pricing/, https://leanopstech.com/blog/aws-data-transfer-pricing-2026/) |
| Backblaze B2 | **Free up to 3x average monthly storage; $0.01/GB beyond**; unlimited free via partner CDNs (Cloudflare, Fastly, bunny.net, …) | https://www.backblaze.com/cloud-storage/pricing |
| Cloudflare R2 | **$0 egress, any volume, period** (policy holding as of 2026) | https://onidel.com/blog/cloudflare-r2-vs-backblaze-b2, https://leanopstech.com/blog/cloudflare-r2-pricing-2026/ |
| Wasabi | **Free egress under "reasonable use"**: monthly egress should not consistently exceed total stored volume; one-time migrations fine, egress-heavy serving is not | https://wasabi.com (policy described at https://apiscout.dev/guides/backblaze-b2-vs-wasabi-api-2026) |

### Worked example: pull 20 TB out (one-time exit; 1 TB = 1,000 GB)

| From | Math | Cost |
|---|---|---|
| AWS S3 | 100 GB free + 9,900 GB × $0.09 + 10,000 GB × $0.085 | **≈ $1,741** |
| Backblaze B2 | 20 TB stored → up to 60 TB/mo egress free | **$0** |
| Cloudflare R2 | zero egress | **$0** |
| Wasabi | one-time 20 TB pull vs 20 TB stored = within policy | **$0** (watch 90-day minimum-storage charge if deleting young data) |

### Vendor-side exit/egress fees (checked 2026-06-11)

- **Iconik: egress is explicitly billed** through its $1 credits — "egress (download from the cloud service) costs… transfers from iconik GCS to third party are egress charged" (https://help.iconik.backlight.co/hc/en-us/articles/25304358922519-Controlling-Costs; credits model: https://www.iconik.io/pricing). Getting media OUT of iconik-managed storage costs real money — quantify only via sales.
- **LucidLink: no separate egress/exit fee documented** — "storage and streaming are included in a single monthly price with no separate egress line items"; Enterprise per-TB price "includes egress" (https://www.lucidlink.com/blog/cloud-egress-fees, https://blocksandfiles.com/2023/01/30/lucidlink-egress-charges-and-direct-bucket-access/ — checked 2026-06-11). ⚠️ Caveat: on seat-based plans the included-storage allotment is what you're pulling from; no documented fee to download your data out, but also no documented bulk-export tool.
- **Suite: Managed plan — no egress fee documented** on /pricing or KB (https://www.suitestudios.io/pricing — checked 2026-06-11). **BYO plan: your CSP bills egress directly** ("Your CSP will charge you separately for storing the data and any other associated fees" — https://support.suitestudios.io/en/articles/8302694-what-is-bring-your-own-storage-byo). Exit cost = your provider's egress rate on the whole library.
- **Shade: no egress, export, or exit fee documented anywhere public** (https://shade.inc/pricing, https://academy.shade.inc/administration-and-security/billing — checked 2026-06-11). Record as "none documented," not "free" — billing docs say only "seats and storage."

**Site implications:** The "leave whenever" page can show the 20 TB worked example as a 4-row table — $1,741 vs $0/$0/$0 — and the punchline that JuiceMount on B2/R2/Wasabi makes exit literally free, while iconik *charges credits* to hand your media back. Calculator: add an optional "exit cost" line using these rates. Keep AWS in the picture — it's the honest worst case and many users start there.

---

## 7. Design reference notes (homepage humanization pass)

**VERDICT: Nobody in this set leads with a laptop mockup; the strongest pages humanize via real customer work (Suite) or real faces (iconik), and the sharpest copy is a category-reframing one-liner. NLE logos appear only in iconik's integrations section — an open lane for JuiceMount to show Resolve/Premiere/FCP right in the hero context.**

All checked 2026-06-11.

- **shade.inc** — Hero: "Media storage that just works better." / "Intelligent file streaming, review and approval, automated metadata, AI search… All in one platform." Visual: cinematic lifestyle footage of people, not product UI. No NLE logos, no customer logos visible; instead NINE industry vertical cards (sports, agencies, post, real estate, podcasts, houses of worship…) plus a heavy compliance-badge wall (SOC II, TPN, ISO 27001, HIPAA) and a "Days 1/14/21" onboarding timeline. **Idea worth adapting: the 30-day onboarding timeline** — converts "switching is scary" into a concrete schedule. (https://shade.inc/)
- **lucidlink.com** — Hero: "Large-file workflows without the wait" / "No downloads, no syncing, no VPNs." Visual: rotating SVG illustration carousel, no screenshots/mockups in hero; G2/Capterra ratings as trust strip; tool names (Adobe CC, Final Cut, Figma, AutoCAD) in body copy only. **Idea worth adapting: the "traditional vs LucidLink" workflow comparison table** ("Work changed, file access didn't") — a before/after table is exactly the format JuiceMount's comparison page already owns; do it with receipts (their own help-page quotes from §5). (https://www.lucidlink.com/)
- **iconik.io** — Hero: "Introducing the creative operations super-platform" / AI-forward subhead. Visual: overhead lifestyle photo of four colleagues at a desk — actual humans, the only vendor doing this. Customer logos up high (Canva, Vice, Complex, Houston Rockets, MGM); **the only site showing NLE logos** (Premiere, After Effects, Resolve, FCP) — but buried in an integrations grid. Trust metrics ("900M+ assets, 1,000+ years of video"). **Idea worth adapting: concrete scale metrics + real faces near the hero.** (https://www.iconik.io/)
- **suitestudios.io** — Hero: "Cloud Storage Meets Local Performance" + watch-video CTA; announcement banner for "S3 native file streaming beta" (note: Suite is also drifting toward S3-native — strategic echo of §2/§3). No logo wall; instead name-drops actual productions: Kendrick Lamar "Squabble Up," Netflix's "Simone Biles: Rising," the Eagles at Sphere. Tagline "File Streaming is the new sync." **Idea worth adapting: cultural credibility via named real projects instead of logo soup, and the category-reframing one-liner.** (https://www.suitestudios.io/)
- **strada.tech** — Hero: "Access your media without moving it." / "Edit, share, and collaborate on remote media as if it lives on your desk." Text-led hero, Sign-up + Watch-the-Video CTAs, single high-trust testimonial (Ryan Connolly, Film Riot). Manifesto line: "We don't rent storage. We offer the protocol that makes yours work." **Idea worth adapting: the one-sentence manifesto against renting storage — closest voice to JuiceMount's own; differentiate by showing the always-on shared volume Strada can't do.** (https://strada.tech/)

**Site implications:** (a) hero = one reframing sentence + real editor at a real timeline (photo or honest screen capture), not an abstract SVG; (b) put Resolve/Premiere/FCP logos near the hero — only iconik shows them at all, and not there; (c) steal the formats: before/after table (LucidLink), onboarding timeline (Shade), named-projects credibility (Suite), anti-rent manifesto line (Strada).

---

## 8. Proposed pricing.json deltas

**VERDICT: Existing values all verified correct as of 2026-06-11 (LucidLink $27 — note it's a promo off $32 — Suite $75/$40, Shade $29.75 annual). Proposed: add the constraints the calculator silently ignores (Suite 20 TB BYO floor, Shade unpublished extra-TB, LucidLink seat caps), plus new anchor vendors and egress rates.**

Verification of existing entries (all checked 2026-06-11):
- `lucidlink_business` ✓ — $27/member/mo currently shown as a DISCOUNT from a $32 regular price; 400 GB/member pooled; $8/100 GB extra; 25-member max; "best for up to 10 TB" (https://www.lucidlink.com/pricing). Consider `seat_regular: 32` so the calculator can show "promo pricing."
- `suite_managed` / `suite_byo` ✓ — $75 and $40/TB/mo, 5 seats incl., $10/user extra (https://www.suitestudios.io/pricing); BYO has a **20 TB minimum** the file omits (https://support.suitestudios.io/en/articles/8302694-what-is-bring-your-own-storage-byo).
- `shade_growth` ✓ annual price — $29.75 annual / **$35 monthly**, 500 GB active/seat, 15-seat cap; extra storage UNPUBLISHED (https://shade.inc/pricing).
- `object_storage` — b2 $6/TB ✓, wasabi $6.99/TB ✓, r2 $15/TB ✓ (R2 standard storage $0.015/GB) (https://www.backblaze.com/cloud-storage/pricing — checked 2026-06-11).

Proposed additions/corrections (same schema style; do NOT apply automatically — review first):

```json
{
  "_proposed": "2026-06-11 research round 2 — see RESEARCH_ROUND2.md for full sourcing",

  "lucidlink_business": { "seat": 27, "seat_regular": 32, "included_gb_per_seat": 400, "extra_per_100gb": 8, "soft_cap_tb": 10, "max_seats": 25,
    "_source": "https://www.lucidlink.com/pricing", "_checked": "2026-06-11" },

  "lucidlink_starter": { "seat": 7, "included_gb_per_seat": 100, "extra_per_100gb": 7, "max_seats": 10, "cap_tb": 1,
    "_source": "https://www.lucidlink.com/pricing", "_checked": "2026-06-11" },

  "suite_byo": { "per_tb": 40, "included_seats": 5, "extra_seat": 10, "min_tb": 20, "csp_fees_extra": true,
    "_source": "https://support.suitestudios.io/en/articles/8302694-what-is-bring-your-own-storage-byo", "_checked": "2026-06-11" },

  "shade_growth": { "seat": 29.75, "seat_monthly": 35, "active_gb_per_seat": 500, "max_seats": 15, "extra_storage": "unpublished",
    "_source": "https://shade.inc/pricing", "_checked": "2026-06-11" },

  "iconik_starter": { "browse_seat": 9, "standard_seat": 65, "power_seat": 120, "collaborator_seat": 0, "storage": "credits, quote-only", "egress_billed": true,
    "_source": "https://www.iconik.io/pricing", "_checked": "2026-06-11" },

  "strada": { "category": "p2p-transfer-not-storage", "basic_seat": 8, "unlimited_seat": 24, "basic_transfer_gb_mo": 250,
    "_source": "https://strada.tech/pricing", "_checked": "2026-06-11" },

  "dropbox_advanced": { "seat": 24, "min_seats": 3, "pooled_start_tb": 15, "tb_per_extra_license": 5, "streaming": "whole-file",
    "_source": "https://www.dropbox.com/business/plans-comparison", "_checked": "2026-06-11" },

  "gworkspace_business_plus": { "seat": 22, "seat_flexible": 26.40, "pooled_tb_per_user": 5, "streaming": "whole-file",
    "_source": "https://workspace.google.com/pricing", "_checked": "2026-06-11" },

  "nextcloud": { "license": 0, "enterprise_per_user_year_eur": 68, "enterprise_min_users": 100, "streaming": "sync-and-share",
    "_source": "https://nextcloud.com/pricing/", "_checked": "2026-06-11" },

  "egress_per_gb": { "aws_s3_first_10tb": 0.09, "aws_s3_next_40tb": 0.085, "b2": 0, "b2_beyond_3x_storage": 0.01, "r2": 0, "wasabi": 0,
    "_note": "b2 free to 3x avg monthly storage; wasabi reasonable-use (egress <= stored)",
    "_source": "https://aws.amazon.com/s3/pricing/ + https://www.backblaze.com/cloud-storage/pricing", "_checked": "2026-06-11" },

  "exit_20tb_usd": { "aws_s3": 1741, "b2": 0, "r2": 0, "wasabi": 0,
    "_source": "computed from egress_per_gb, RESEARCH_ROUND2.md §6", "_checked": "2026-06-11" }
}
```

**Site implications:** Calculator gains three honest levers — Suite's 20 TB BYO floor (makes Suite BYO unavailable to small shops), Shade's unpublished overage (forces "call sales" branch above 500 GB/seat), and the exit-cost line. Comparison table iconik row must change from "$35-ish/user" to the real $9/$65/$120 split.
