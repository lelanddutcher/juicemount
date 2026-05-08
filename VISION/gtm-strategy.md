# Go-to-Market Strategy

> **Source material:** `positioning.md` (locked positioning), `personas.md` (3 personas), `brand-identity.md` (voice + tagline), `feature-roadmap-ranked.md` (12 features ranked).
> **Audience:** Leland. Decision-ready, not exploratory.

---

## TL;DR

**OSS launch on Show HN with the spacebar-Quick-Look-on-RAW demo as the wow.** Free forever for solo users. Paid tiers unlock the Pro features (proxy generator, backup verifier) at $5/mo individual / $25/mo per-machine team. Hosted backend SaaS (Loft) ships 6 months later for non-technical users. NAB 2027 is the inflection event we're shipping toward.

**Don't try to fight Adobe-distribution Frame.io Drive. Don't try to land Suite Studios' enterprise logos. Land the segment they've explicitly priced out: TrueNAS-running indie editors, boutique post houses on existing NAS hardware, and sovereignty-bound shops who can't put masters in third-party clouds.**

---

## Pricing

Three tiers + one community tier. Anchor on per-machine, not per-seat — editors run JuiceMount on their laptop AND their studio AND their NAS, and we don't tax that.

### Free / Community
- $0 forever
- Open-source MIT/BSD-licensed core
- Full NFS server + JuiceFS FUSE + SSD cache + memory buffer + FTS5 search
- Quick Look on standard codecs (H.264, ProRes, etc.)
- Direct support: GitHub issues, community Discord, no SLA
- **Limit:** none. The free tier is a complete product, not crippleware.
- **Why:** the OSS launch needs a real product, not a sales funnel. Anyone can use the free tier forever and never see a paywall. This is the trust foundation for paying customers later.

### Pro — $5/month or $50/year per individual
- All Free features
- **Codec-aware Quick Look proxies** (R3D, ARRI, BRAW, ProRes RAW) — prototype #1
- **Content-hash backup verification + traffic-light inventory** — prototype #2
- Bandwidth-aware streaming fallback
- Priority bug fixes (within 1 week vs whenever)
- "Support the developer" warm-fuzzy
- License covers all machines you personally use (laptop + studio + NAS = 1 license)
- **Anchor:** the price of a single beer or a Spotify Premium. Editors who lose 90 minutes a day to storage friction (per `pain-points.md` workflow patterns) will pay this without thinking.

### Team — $25/month or $250/year per active machine
- All Pro features
- **Multi-user real-time presence + cooperative locking layer** (NLE bin sharing)
- Centralized configuration (push prefs to all team machines from a single admin console)
- Direct email support, 48-hour SLA
- License covers up to 15 active machines per team
- **Anchor:** $25/machine vs Suite Studios at $75/TB managed + $10/seat = a 5-editor team with 10TB pays $800/month on Suite vs $125/month on Team. **6× cheaper, no per-seat creep.**

### Loft (Hosted) — $50-500/month, tiered by storage
- Managed Redis + MinIO backend hosted on Hetzner / Backblaze / Cloudflare R2
- Zero setup, zero sysadmin
- Same JuiceMount client, just pre-configured
- 1TB / $50, 10TB / $200, 100TB / $500
- **Anchor:** Suite BYO is $40/TB + the bucket bill = $46/TB total. Loft 10TB tier is $20/TB. **2× cheaper than Suite BYO with zero ops.**
- **Ships 6 months after OSS launch.** This is the broadening play, not the launch wedge.

### Enterprise — custom, ≥$5K/year
- Self-hosted, air-gappable
- TPN/SOC 2 attestations (defer until customer #1 demands them)
- Roadmap influence
- Direct support contract with named SE
- License covers unlimited machines, optional source-code escrow
- **Target:** broadcasters, government media, agencies handling embargoed content (P3 from `personas.md`)
- **Ships:** opportunistically; not a launch focus

---

## Pricing math vs competitors (10TB / 5 editors baseline)

| Solution | Monthly cost | What's included | Storage in your control |
|---|---|---|---|
| **JuiceMount Free + your own B2** | **$60** | All core features, no support SLA | Yes |
| **JuiceMount Pro × 5 + B2** | **$85** | + proxies + verification + bandwidth fallback | Yes |
| **JuiceMount Team + B2** | **$185** ($125 + $60) | + locking + admin console + SLA | Yes |
| **JuiceMount Loft 10TB** | **$200** | All features, zero setup | Loft (managed by us) |
| Suite Storage (managed) | $750 | 5 users included, $10/seat after | Suite |
| Suite BYO + your own B2 | $460 | 5 users, 20 TB minimum | Bucket yes, FS no |
| Frame.io Drive Pro × 5 | $75 + Adobe CC | bundled review + C2C | No (Adobe cloud) |
| Shade Growth × 5 (annual) | $148.75 | bundled MAM + AI tags + review | Partial (BYOS) |
| LucidLink (per Vendr) | ~$1,000 | streaming filesystem | No |
| Iconik × 5 (Pro tier) | $325 | MAM only — needs separate storage | Yes (broker) |
| Jellyfish Studio | $4,990 one-time | hardware + your drives | Yes |

**Headline:** at the indie/boutique tier (≤10TB, ≤5 editors), JuiceMount lands at **6-10× cheaper than the cheapest cloud alternative** and **without the hardware lock-in** of the NAS appliances.

---

## Launch sequence (T-minus chronology)

The launch is a gated 4-week sequence, not a single day. Each stage has a specific channel, audience, and goal.

### T-30 to T-7 days: Pre-launch
- **Goal:** get 5-10 friendly TrueNAS power users on early-access builds.
- **Channels:** DM ~10 people in r/truenas you've replied to before; post in TrueNAS Community Forum's "Apps & Plugins" subforum asking for testers.
- **Output:** 5-10 installs, 1 GitHub issue, 1 honest review of "what's still rough."
- **Fixes:** triage the rough edges. The launch must work end-to-end on a fresh TrueNAS install in <15 minutes. Document EVERY step.

### T-Day (Tuesday morning, US Pacific): Show HN launch
- **Goal:** maximum HN front page time.
- **Channel:** Show HN with title `Show HN: JuiceMount – Open-source, self-hosted media storage for video editors`.
- **Body:** 2-paragraph intro + a 30-second demo GIF (RAW Quick Look) + the pricing table + the "compared to Suite/Frame.io/Shade" table + GitHub link.
- **Why Tuesday morning:** highest-engagement Show HN slot historically. Avoid Mondays (Cmd-Tab competition with weekend backlog) and Fridays (everyone tuning out).
- **Pre-warm:** post in 4-5 Discord/Slack communities you participate in 60 minutes before, to get the first 5-10 upvotes that kick the algorithm.

### T+1 day: Lift Gamma Gain + Reddit cross-post
- **Channel:** Lift Gamma Gain (colorist forum, exact subforum: "Toolbox") with a longer technical post.
- **Channel:** r/truenas + r/selfhosted + r/datahoarder cross-post — each with the angle that fits ("Self-hosted alternative to LucidLink/Frame.io for TrueNAS users" / "Open-source NFS-via-S3 mount for macOS" / "Mount any S3 bucket as a Finder volume").
- **Avoid:** spamming r/editors / r/premiere / r/davinciresolve on launch day. Those audiences are less forgiving of launches; come back at T+30 with a real customer story.

### T+3 days: NoFilmSchool + ProVideo Coalition outreach
- **Channel:** cold-email 6-8 specific writers (use bylines from your competitive research files — Kevin P. McAuliffe at PVC, etc.).
- **Pitch:** "Open-source alternative to Suite Studios + Frame.io Drive — built by an indie video engineer. Free to download, here's a 90-second screencast of the Quick Look magic on R3D. Happy to give you a private hands-on if useful."
- **Conversion target:** 1 published article in 30 days.

### T+7 days: First retrospective post
- **Channel:** your blog (set up a Hugo/Astro site at juicemount.io if not already).
- **Post title:** "1 week of JuiceMount in the wild — N installs, M GitHub issues, here's what I'm fixing"
- **Why:** signals momentum, gets HN front-page redux, builds the "Leland is doing this seriously" reputation.

### T+30 days: First paid tier opens
- Pro tier ($5/mo) ships behind a feature flag in v1.1.
- Email all early-access testers + GitHub stargazers (>50, hopefully): "Pro is live, here's what it unlocks, free trial 30 days."
- **Conversion target:** 20 paying users in month 1 = $100 MRR. Tiny but real.

### T+90 days: Team tier opens
- After enough Pro users have referred their boutique-shop friends.
- Sales motion: founder-led, no funnel, just personal email + Calendly.
- **Conversion target:** 5 Team customers = $625 additional MRR. $725 total MRR.

### T+180 days: Loft (hosted backend) launches
- The bridge to non-technical persona 2 customers.
- Press tier: a second Show HN with the angle "JuiceMount now has a 1-click hosted version."
- **Conversion target:** 50 Loft customers in month 7-8 = $5K-15K MRR depending on tier mix.

### NAB 2027 (April): the inflection
- Goal: 1 enterprise pilot signed (P3 persona — broadcaster or studio compliance shop).
- Booth presence: probably not in year 1; instead, attend and meet 20 people with a demo on a MacBook.
- Speaking slot if achievable: "Why Open-Source Storage is the Next Wave in Post-Production."

---

## Channels — specific URLs and engagement plans

### Reddit (primary OSS funnel)
- **r/truenas** (140K) — primary. Native build is "TrueNAS user runs `docker compose up` and gets JuiceMount."
- **r/selfhosted** (450K) — primary. Frame: "self-hosted alternative to LucidLink."
- **r/homelab** (700K) — secondary. Frame: "make your existing NAS feel like 2026."
- **r/datahoarder** (700K) — secondary. Frame: "content-hash verified backups across N targets."
- **r/editors** (380K) — wait until T+30 with a customer testimonial in the post.
- **r/premiere** (170K) — same — wait for legitimacy.
- **r/davinciresolve** (260K) — same.
- **r/VideoEditing** (180K) — same.

### Hacker News
- **Show HN day 1.** Tuesday 8-10am Pacific. Single shot — don't re-Show-HN.
- **Posts at T+7 and T+30** about lessons + customer stories. These are normal HN posts (not Show HN), submitted from the blog URL.

### Forums (lower-volume but high-trust)
- **Lift Gamma Gain** — Toolbox subforum. Colorist crowd, includes Perry Paolantonio (the LucidLink customer quoted in pain-points.md). Big trust capital here.
- **Creative COW** — Premiere Pro forum. More skeptical audience.
- **TrueNAS Community Forum** — already engaged during T-30.
- **DaVinci Resolve forum (forum.blackmagicdesign.com)** — tread carefully; Blackmagic moderators are protective of Resolve discussion.
- **Adobe Community** — only after we have a Premiere panel integration to discuss.

### Direct outreach (T+3)
Cold-email 6-8 specific writers:
- Kevin P. McAuliffe (ProVideo Coalition — wrote Suite Studios review)
- Iain Anderson (CineD — Apple/FCPX angle)
- Jim Garrett (NoFilmSchool tech editor)
- Larry Jordan (LarryJordan.com — Premiere expert, has 100K+ newsletter subs)
- Casey Faris (YouTube Resolve channel, 200K subs)
- Cullen Kelly (color community)
- Wandering DP (Patrick O'Sullivan)
- Bill Sharpsteen (PostMagazine)

### Podcasts (T+30 onward)
- **Office Hours Live** (post-prod sponsorship, ~10K listeners)
- **Mixing Light** (color community)
- **Workflow.show** (Adobe-aligned but covers competitors)
- **The Cinematography Podcast** (broader cinematography reach)

### YouTube (T+60 onward)
- DIY: Leland records a 5-min "this is what JuiceMount does" video.
- Sponsored mentions: target 2-3 mid-tier creators ($500-2000 each) for honest reviews.

---

## Content calendar (first 90 days)

| Week | Content | Channel | Goal |
|---|---|---|---|
| W-2 | "I got fed up with LucidLink and Frame.io" — backstory blog post | juicemount.io | SEO + emotional hook |
| W-1 | Demo GIF + screencast assets | Twitter, prep for launch | Build anticipation |
| **W0** | **Show HN launch** | HN | Front page, 100+ stars |
| W1 | Lift Gamma Gain + Reddit cross-posts | Forums | First user installs |
| W2 | First retrospective: "Week 1 in the wild" | juicemount.io + HN | Momentum signal |
| W3 | "How JuiceMount finds bit-rot Backblaze missed" — backup verification deep-dive | juicemount.io | Lead with prototype #2 |
| W4 | Pro tier launch + email to all stargazers | Email + GitHub release | First MRR |
| W6 | Customer story #1 (whichever early adopter is willing) | Blog + HN | Social proof |
| W8 | "Your NAS is fine; you just need a better client" — comparison post | Blog + r/truenas | Conversion content |
| W10 | Team tier launch | GitHub release + sales emails | $625 add'l MRR |
| W12 | "90 days in: $725 MRR, 50 Pro users, here's what surprised me" | HN + blog | Compounding momentum |

---

## Founder-led sales (no SDR, no funnel — for first 12 months)

**Solo founders win on personal connection. Don't replicate the Suite/Shade SaaS playbook.**

- Every Pro customer gets a personal email from Leland within 48 hours of signup.
- Every Team customer gets a 30-min Zoom onboarding (your time, no support staff).
- Every churn email (cancel) gets a personal "what would have made this work?" reply.
- Public roadmap on GitHub with explicit "vote for what you want" issues.
- Quarterly "founder office hours" — public Discord call, anyone joins, asks anything.

This doesn't scale forever, but it's the only way to win in year 1 against incumbents with sales teams. By year 2, the customer evangelists you've cultivated do the selling for you.

---

## Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| HN launch flops (no front page) | Medium | Have a backup retrospective post ready for T+7. Treat HN as a slot, not the only one. |
| Frame.io adds the missing features (RAW Quick Look) before we get traction | Medium | Adobe ships slowly. We have ~12 months of head start. By the time they catch up, the open-source/sovereign angle is our moat. |
| Suite Studios open-sources their client to commoditize us | Low | Suite raised $21.5M to NOT do this. They're locked into a closed model by their cap table. |
| TPN/SOC 2 demanded by an Enterprise prospect early | Medium | Have a "we can sign an MNDA + offer source escrow + commit to attestation by date X" answer ready. Don't actually commit to TPN until the contract is signed. |
| Apple deprecates the macFUSE/FSKit underpinning | Medium | We're on macOS-native NFS, not FUSE, on the client side. This is the structural advantage of our architecture. JuiceFS's FUSE is on the server side and they own it. |
| ffmpeg shellout breaks on a future macOS release | Low | Ship a bundled ffmpeg with the .app (~80MB). One-time work. |
| GitHub issue queue overwhelms Leland | High | Use GitHub Discussions for support questions, Issues only for confirmed bugs + roadmap items. Triage weekly. |
| Hosted Loft tier blows up our ops budget | Medium | Cap launch at 100 customers and waitlist after. Re-evaluate margin before opening floodgates. |

---

## What success looks like at 6 / 12 / 24 months

| Milestone | Month 6 | Month 12 | Month 24 |
|---|---|---|---|
| GitHub stars | 1,500 | 5,000 | 15,000 |
| OSS installs (estimated) | 500 | 3,000 | 12,000 |
| Pro paying users | 50 | 250 | 800 |
| Team paying customers | 5 | 30 | 100 |
| Loft customers | 50 | 200 | 700 |
| Enterprise pilots | 0 | 1 | 3-5 |
| MRR | $3K | $15K | $60K |
| ARR | $36K | $180K | $720K |

If the 24-month MRR hits $60K, Leland has a viable solo business with margin and optionality (raise, hire, stay solo, sell). If it stalls at $15K, there's still no failure — the OSS lives on, the brand exists, future pivots are easier.
