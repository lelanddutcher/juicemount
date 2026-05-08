# JuiceMount Personas

Three personas in priority order. Each one has: a profile, day-in-the-life, the specific JuiceMount features they care about, where to find them, and what they'll pay.

Sourced from `VISION/positioning.md` (locked positioning) and `VISION/pain-points.md` (named editor quotes).

---

## Persona 1 (PRIMARY): The Sovereign Video Engineer

> **Representative:** Leland-class. Solo or 1-of-3 technical operator at a small production company.

### Profile

- **Age/career stage:** 28-45. 5+ years in post. Title is fluid: "video engineer," "post-supervisor," "DIT," "DP/colorist," or just "the technical one."
- **Income:** $80K-180K. Either self-employed (freelance day rate $500-1500) or salaried at a 3-15 person production company.
- **Hardware they own:** A Mac Studio or M-series MacBook Pro. A 10GbE switch. A NAS with 8-24 drive bays — TrueNAS, Synology, or QNAP. Probably 30-100TB of usable storage. An external Thunderbolt SSD or two for current projects. An LTO-7/8 deck (rare but the technical ones do).
- **Software stack:** Resolve Studio + Premiere primary. After Effects sometimes. FCPX for personal projects. Photoshop, Illustrator. Maybe Pro Tools or Logic for sound. Frame.io tried and partially abandoned. LucidLink considered and rejected (too expensive). Iconik considered and rejected (too enterprise).
- **Already pays for:** Adobe Creative Cloud (resentfully). Backblaze B2 personal backup. Domain registration. GitHub. Maybe a cheap VPS for personal projects.
- **Self-image:** "I run my own infrastructure." Reads HN, lurks r/selfhosted, has set up Docker compose stacks unprompted, can write a bash script. Has opinions about Drobo's collapse. Distrustful of cloud lock-in. Bought into the homelab ethos before it was cool.

### Day-in-the-life (composite from `pain-points.md`)

**08:30** — Coffee, opens MacBook at standing desk. Fires up Resolve to continue yesterday's color session. **First friction:** three clips show "Media Offline" because last night's sync moved a folder. Spends 8 minutes relinking. Mentally curses Frame.io.

**09:15** — Ready to work. Pulls 12 takes from the bin. **Second friction:** can't find the wide shot from the Tuesday shoot. Bin search by name fails (clips are named A001_C034_R5K3). Opens Finder, navigates the NAS over SMB. Finder beachballs — there are 800 clips in the dailies folder. Eventually scrubs through a dozen .R3D files in QuickTime to find the right one. Lost 18 minutes.

**12:00** — Director Slack-pings asking for the latest cut. Editor exports H.264, uploads to Frame.io, sends link. Director's review takes 25 minutes; comments arrive in chunks. Editor parses, makes fixes, exports again.

**14:30** — VFX freelancer in another city needs the 4K plate. Editor copies 18GB to a portable SSD, kicks off WeTransfer Pro upload (max file size). 40 minutes of "do not put computer to sleep." Or LucidLink would solve this if not for the $300/mo bill.

**17:00** — End of session. Project file syncs to NAS via Carbon Copy Cloner. Editor *thinks* the backup ran. Goes home not entirely sure.

**Friction events tally:** 4 in a single day. Time lost: ~90 minutes. Multiplied across a 5-day week: ~7.5 hours/week of pure tax on storage workflow.

### What they care about

In rank order based on how much it'd improve their day:

1. **Search across the entire archive in <1 second.** "I know I had a clip with X" reduced from 20 min of scrolling to 50ms of typing. *(Already shipping — Cmd+Shift+F.)*
2. **Spacebar Quick Look that works on RAW.** Stop opening QuickTime. Stop transcoding for review. Just preview. *(Prototype #1 from feature-roadmap.)*
3. **NAS browse that doesn't beachball Finder.** Sub-millisecond directory listings. *(Already shipping.)*
4. **A backup state they can trust.** Green/yellow/red verification, not "did rsync exit 0." *(Prototype #2 from feature-roadmap.)*
5. **No per-seat surprise bills.** Owned, not rented. Predictable. *(Already shipping — open source.)*
6. **Files that survive moves.** Content-hash linking so renames don't break Premiere. *(Already shipping — content hash in catalog.)*

### Where to find them

- **r/truenas** (140K members) — they live here
- **r/selfhosted** (450K) — adjacent
- **r/homelab** (700K) — adjacent
- **r/datahoarder** (700K) — adjacent  
- **TrueNAS Community Forum** — the ones who've already configured a Mini X
- **Lift Gamma Gain forum** — colorists, including the Perry Paolantonio cohort
- **HackerNews "Show HN"** — single most leveraged launch venue
- **ProVideo Coalition / NoFilmSchool comment sections** — they read trade press but rarely post
- **Twitter/X:** ex-Frame.io customers complaining publicly (search "frame.io billing" / "lucidlink cost")
- **Discord:** various post-production servers (DaVinci Resolve, Color Forum)
- **YouTube comments:** under videos by Casey Faris (Resolve), Color Grading Central, Cullen Kelly (color), Wandering DP

### What they'll pay

**Today: $0.** They're using JuiceMount's open-source build because they trust the code, not the vendor.

**In 6 months, when JuiceMount has paid tiers:**
- They will pay $5-15/month for a "Pro" license that adds: codec-aware Quick Look proxies (#1), backup verification scheduler (#2), bandwidth fallback (#3), and "support the developer" warm-fuzzy.
- They will NOT pay $50+/month. That's competing with their own NAS amortization math.
- They will absolutely NOT pay per-seat. They might be running it on three machines (laptop, studio, NAS-mounted instance).

**Conversion path:** OSS install → uses for 30-90 days → upgrades to Pro for the proxy magic and the backup peace of mind.

---

## Persona 2 (SECONDARY): The Boutique Post House

### Profile

- **Team size:** 3-15 people. Editor(s), colorist, sound designer, producer/manager, sometimes a junior assistant.
- **Org type:** Independent post house, in-house brand video team at a Series A-C startup, sports highlights team, regional production company, indie feature dailies team.
- **Annual revenue:** $500K-5M.
- **Hardware they own:** A Jellyfish Studio ($4,990) OR a TrueNAS R-series ($8K+) OR an OWC ThunderBay 8 ($3K + drives). 50-300TB total. 10GbE office network. Mac mini server or Mac Studio handling the central role.
- **Software stack:** Mixed NLEs — usually Premiere primary with Resolve for color and FCPX for some editors. Frame.io for client review. iconik considered, rejected as too enterprise. Currently rotating between LucidLink trial, ChronoSync, WeTransfer Pro, and an internal Slack channel called #where-is-the-file.
- **Self-image:** "We're a real shop." Has a "the IT person" — could be the producer's spouse, the colorist's brother, a freelance sysadmin who comes by Tuesdays. They are NOT enterprise IT. They are a small business that needs storage to work.

### Day-in-the-life

The shop has 2 editors on Project A, 1 colorist on Project B, 1 sound designer on Project C, and a producer reviewing all three. Storage interactions:

- 09:00 — Editor 1 opens Premiere. Project lock from yesterday's editor 2 session is still showing. Editor 2 is in a Slack channel with director. Editor 1 messages "are you in the project?" Editor 2 doesn't reply for 12 min. Editor 1 gives up, opens a different project to bill against in the meantime.
- 10:30 — Producer wants to see "all the takes of the talking-head intro from the Tuesday shoot." Has to ask editor 1, who has to ssh into the NAS or scroll in Finder. 15 min lost.
- 13:00 — Sound designer needs the picture-locked sequence. "When are you locking?" / "We're locked at 12:45 last night." Sound designer tries to open it from the NAS — finds the wrong version. Five-message Slack thread to find the right one.
- 15:00 — Director asks for a review link. Producer has to figure out which Frame.io account the project is in (the agency uses one for clients, one for internal). Uploads. Bills the wrong account.
- 17:30 — Sysadmin (remote) gets a Slack from producer: "the Backblaze backup keeps failing." Tries to remote in. Finds a 2TB R3D folder that the editor moved without telling anyone, breaking the rsync.

**Friction events tally:** 5+ per day, distributed across the team. Cumulative hours lost per week: ~20-40.

### What they care about

In rank order:

1. **One source of truth that everyone sees.** No more "where is the latest cut?" Real-time multi-machine sync. *(Already shipping — Redis pub/sub.)*
2. **Cooperative locking that works.** "Editor 2 is in this project" presence + atomic save mediation. *(Roadmap #5 — second tier.)*
3. **Search across all projects, all clients, all archive years.** *(Already shipping.)*
4. **Backup state that the producer can verify in 15 seconds.** *(Prototype #2.)*
5. **No per-seat fees.** A 10-person team paying $30/seat/mo is $3,600/year forever. They will pay $50-200/mo for a Team license that includes the seats.
6. **Easy onboarding for a freelance editor.** Today: ship a hard drive. Tomorrow: send a JuiceMount config link.

### Where to find them

- **NoFilmSchool**, **ProVideo Coalition**, **Studio Daily** — they read these
- **Premiere Pro Beta program** — many beta-test, gives a contact list
- **Adobe MAX, NAB, IBC** — physical events
- **Specialty Slack/Discord:** Color Forum, Editors Coffee, EditCorp
- **Vimeo Staff Picks community** — many indie features here
- **Recurring sponsorships on small post-prod podcasts** (e.g., Office Hours Live)
- Targeted outbound: scrape LinkedIn for "Post Producer" + "Production Coordinator" titles at studios with 5-50 employees in major media markets

### What they'll pay

**Today: $0** for the OSS, but they'll pilot only after a referral from a sovereign engineer. They don't install random GitHub releases.

**In 6 months:**
- $50-200/month for a Team license (5-15 seats) including the proxy + verification + locking features.
- They WILL pay this because $200/mo replaces $1,500/mo of LucidLink + Frame.io + WeTransfer Pro.
- They WILL NOT pay per-seat — they negotiate everything that's per-seat.
- The pitch is "consolidate three SaaS bills into one local-first install."

**Conversion path:** referral from a sovereign engineer at one of their freelancers OR a Show HN that the producer's "technical person" forwards.

---

## Persona 3 (TERTIARY, future): The Sovereignty-Bound Shop

### Profile

- **Org type:** Broadcaster, government media department, agency handling embargoed content (financial, legal, political), insurance company processing claim videos, healthcare company processing patient education, a record label sitting on unreleased masters.
- **Team size:** 10-100+ in the media department.
- **Compliance posture:** Contractually or legally cannot put primary masters in third-party clouds. Maybe TPN-required. Maybe SOC 2 / HIPAA / FedRAMP-required. Air-gapped or VPN-only network.
- **Hardware they own:** A SAN. Or a serious NAS array. Or both. They have actual IT.
- **Software stack:** Premiere or Avid Media Composer (Avid is overrepresented in this segment). Resolve for color in some shops. NO Frame.io (or only for non-master client review). NO LucidLink. iconik in some cases (it has the enterprise security story).
- **Self-image:** "We have rules." They are the compliance officer's first call, not their last. They're not impressed by VC funding stories.

### Day-in-the-life

Honestly, less friction than the other two — they have IT. The pain is different:

- The IT vendor stack is rigid and expensive.
- The Avid Media Composer + Avid Nexis combo is $50K+ per year and works.
- They spent 18 months getting iconik through procurement.
- Adding ANYTHING new is a security review + procurement cycle.

Their friction is **innovation velocity**, not daily workflow. They want better tools but the buying process punishes them for trying.

### What they care about

1. **Self-hosted, air-gappable.** Period. *(Already shipping — JuiceMount runs entirely behind the firewall.)*
2. **Auditable open-source code.** Their security team can read the code. They can fork it. *(Already shipping.)*
3. **No phone-home telemetry.** They will network-trace it.
4. **Long-term vendor stability** — they need a 5-year buying horizon. *(JuiceMount's OSS-with-paid-support model lets them keep running even if we go away.)*
5. **TPN/SOC 2 attestations** eventually — but later, after the OSS lands and the indie market is captured.

### Where to find them

Hardest to reach. They do NOT read HN. They DO read trade press. They go to NAB and IBC. They lurk on PostMagazine and SVG (Sports Video Group).

- Direct outreach via post-production trade associations
- Conference presence at NAB / IBC after we have a real product
- Compliance-focused content marketing ("how to keep your masters out of the cloud without giving up modern UX")
- Partnerships with system integrators who serve broadcast (Diversified, IDS, etc.)

### What they'll pay

**Today: $0** but irrelevant — they won't even evaluate yet. Come back in 12-18 months with TPN attestation and a real support contract.

**In 18 months:**
- $5K-50K/year per deployment for a self-hosted Enterprise license + support SLA + roadmap influence.
- They WILL absolutely pay this because the alternatives (Avid Nexis annual maintenance, iconik enterprise) cost more and don't solve the local-first need.

**Conversion path:** The trade press launch + a public reference customer (probably another tier-3 shop) + a procurement-friendly pricing sheet.

---

## What this means for product decisions

| Decision | Persona 1 weight | Persona 2 weight | Persona 3 weight | Verdict |
|---|---|---|---|---|
| Build hosted SaaS first | Low (they self-host) | Medium (would buy if priced right) | Zero (they cannot use cloud) | **Defer hosted to tier 3, OSS first** |
| Add Premiere panel | Medium | High | Medium (Avid users mostly) | **Build Resolve workflow integration first; Premiere panel after** |
| Pursue TPN/SOC 2 now | Zero | Low | Critical | **Defer until persona 3 revenue justifies it** |
| Build Windows client | Low (Mac primarily) | Medium (mixed shops) | High (broadcasters often Windows) | **Build after macOS is rock-solid** |
| Build mobile companion | Medium (review on iPad) | High (producers want it) | Low | **Persona 2-driven feature, schedule after prototypes ship** |
| Build collaborative locking | Low (solo) | Critical | Low | **Persona 2-gated. Skip unless we want to expand to teams.** |

**Strategic conclusion:** Persona 1 unlocks the OSS launch and the bottoms-up adoption. Persona 2 unlocks first revenue (Team license $50-200/mo). Persona 3 unlocks scale revenue but requires 12-18 months of OSS-credibility-building first.

**Iteration 4-5 prototypes (codec-aware Quick Look + backup verification) are explicitly chosen for Persona 1 because they're the bottoms-up adoption engine.** Once 1,000+ Persona 1s are using JuiceMount daily, Persona 2 referrals start happening organically.
