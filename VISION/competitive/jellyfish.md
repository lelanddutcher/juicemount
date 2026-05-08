# Competitive Brief: OWC Jellyfish (formerly LumaForge)

## Overview

The Jellyfish line is the gold-standard turnkey shared-storage server for video post-production teams. Founded as **LumaForge** by Sam Mestman and a small team of former post-production pros, the company shipped its first Jellyfish around 2015, marketed as "shared storage built by editors, for editors." Apple began reselling it through Apple's enterprise sales channel in late 2018 for up to $50,000. **OWC (Other World Computing) acquired LumaForge in January 2021**, and the product is now sold under the OWC Jellyfish brand. The current lineup, post-acquisition and after the 2024 V3 refresh, includes:

- **Jellyfish Nomad / Mobile** — portable carry-handle unit for on-set DITs and small remote teams
- **Jellyfish Studio** — newer 2024 desktop NAS for boutique/freelance teams (entry tier)
- **Jellyfish Tower** — whisper-quiet desktop server for in-office boutique post houses
- **Jellyfish R24 / Rack** — 4U rack-mount unit for broadcast-scale shops with 12+ editors
- **Jellyfish S** (older designation) — solid-state Jellyfish for finishing/VFX

Target buyer: 3- to 25-seat post houses, documentary teams, branded-content shops, broadcast facilities, and boutique color/finishing rooms — especially Apple-centric Final Cut Pro and Premiere shops that don't want to hire an IT staffer to run a SAN.

## Product capabilities

**Storage architecture.** All Jellyfish units run **OpenZFS RAID** on a custom Linux distribution. The Tower and Rack ship with the equivalent of RAID Z2: a default Tower can sustain the failure of up to 2 hard drives per 10-disk virtual device, and the Tower/Rack will tolerate up to 4 drive failures across the full pool. Bay counts run from 8 (Mobile/Nomad) up to 24+ in the R24, and capacity scales from 32TB raw on the Mobile to ~800TB on a fully loaded Tower.

**Network connectivity.** Direct-connect 10GbE is the baseline — the Tower and Rack expose up to 16 onboard 1GbE/10GbE ports so editors plug straight into the chassis without a switch. The V3 refresh (2024) added optional **25GbE and 50GbE direct connections** for VFX and color workflows, paired with a CPU upgrade. Macs without 10GbE rely on Sonnet or AKiTio Thunderbolt-to-10GbE adapters.

**Software stack.** Custom Linux + OpenZFS, fronted by four OWC-built apps: **Jellyfish Connect** (client mounting tool), **Jellyfish Manager** (admin/permissions/cloud-backup), **Jellyfish Remote** (remote-access proxy), and **Jellyfish Media Manager** (light proxy generation/asset organization).

**Sharing protocols.** SMB, NFS, CIFS, iSCSI, plus FTP and SSH for remote login. Every unit ships pre-configured with both SMB and NFS volumes — the Connect app does under-the-hood SMB tuning per client.

**Multi-user features.** Up to 14 simultaneous HD editors or 6 4K editors on a Mobile; up to 20 directly connected on Tower/Rack. ZFS snapshots provide point-in-time recovery.

**NLE integrations.** Tested and certified for Final Cut Pro, Premiere Pro, DaVinci Resolve, and Avid Media Composer. The product positions itself around the FCP shared-library workflow.

**Asset management.** Jellyfish Media Manager handles light cataloging and proxy generation, but it is not a Kyno/Iconik replacement. Most shops still pair Jellyfish with Iconik or Frame.io.

**Backup/redundancy.** ZFS snapshots + optional cloud-backup config in Jellyfish Manager. Customers typically pair the unit with an LTO deck or a second offsite Jellyfish.

## Pricing

- **Jellyfish Studio**: starts at **$4,990** for the 32TB HDD base
- **Jellyfish Mobile/Nomad**: starts at **~$9,500** for 32TB raw / 23TB usable
- **Jellyfish Tower & Rack**: starts at **~$30,000** for 80TB raw / 53TB usable
- **Top-end configurations**: up to **$50,000** for 200TB units (Apple resold these directly in 2018)

Drive cost is **bundled** — Jellyfish does not sell empty enclosures. Software is included in the hardware price; there is no separate licensing tier. **Support contracts** ("Care plans") are sold annually after the first year and run roughly 10–15% of hardware cost. Larger Tower/Rack configurations easily reach $80,000–$120,000 with 800TB and dual-CPU upgrades.

**5-year TCO estimate (50TB usable, 8-editor shop):** Roughly **$45,000–$55,000** (Tower hardware + 4 years of Care + drive replacements). Comparable Frame.io Camera-to-Cloud + storage at the same scale runs $25K–$35K/year for the same team — so cloud crosses Jellyfish TCO around year 2 and is significantly more expensive over 5 years if you already have a fast LAN.

## Strengths

- **One-time capex** vs ongoing cloud subscription. After 18–24 months a Jellyfish is cheaper than equivalent cloud egress.
- **LAN-speed performance**: 4,400 MB/s aggregate on a Tower, fast enough for 4K ProRes RAW streams without proxies.
- **Data sovereignty**: footage never leaves the building — appealing to feature-film and broadcast clients with NDA constraints.
- **Pre-tested, pre-configured**: this is Jellyfish's biggest moat. Synology/QNAP make you tune SMB; Jellyfish ships tuned. "Once setup and connected it just worked. Editors were able to pound away without even thinking about it." [(ProVideo Coalition)](https://www.provideocoalition.com/real-world-day-editing-lumaforge-jellyfish-adobe-premiere-pro/)
- **Vendor support staffed by editors**, not generic NAS L1 — they know what a dropped frame in Premiere actually looks like.

## Weaknesses

- **Capital expense barrier**. $30K is a lot for a 3-person freelance shop, and a $9.5K Mobile is overkill for solo editors.
- **Physical maintenance**: drive swaps, cooling, UPS, and rack space are the customer's problem. The Tower is "whisper-quiet" but still a 24/7 machine in your edit bay.
- **Single-site by design**. Remote teams need Jellyfish Remote (a proxy/VPN-style add-on) — and remote-edit experience is still markedly worse than local. There is no native multi-site sync.
- **Capacity ceiling = bay count**. Once a Tower's 16 bays are full, the only path forward is a second Tower or migration to a Rack — meaning a forklift upgrade.
- **Vendor lock-in to a single SKU vendor.** OWC owns Jellyfish entirely. If OWC pivots (the way StorCentric did with Drobo), customers are stranded on a custom Linux distro with no upgrade path.
- **No remote/cloud-native story** beyond a bolt-on remote-access proxy.

## What JuiceMount can credibly beat them on

- **Adds remote access and cloud-bridging on top of an existing Jellyfish.** JuiceMount mounts the Jellyfish via SMB/NFS and exposes its content to a remote editor's Mac as if it were local — without requiring a Jellyfish Remote license per seat.
- **Search across the whole library**. The Jellyfish Connect app browses via SMB, which is slow over Mac Finder for libraries with thousands of clips. JuiceMount's local SQLite index returns results instantly.
- **Quick Look across the network**. Previewing media via SMB on a mounted Jellyfish volume is painful (Finder fetches thumbnails on demand). JuiceMount caches thumbnails locally.
- **Multi-machine sync**. Jellyfish has no real-time multi-site mirror — JuiceMount can act as the cache layer and keep two offices coherent.
- **Modern macOS-native client.** Jellyfish Manager is a cross-platform Electron-style admin tool. JuiceMount is a Swift menu-bar app written for editors.
- **Works with existing Jellyfish, not against it.** JuiceMount sits in front of the Jellyfish as the access layer. The Jellyfish is the bulk-storage backend; JuiceMount is the experience.

## Direct quotes / evidence

**Pricing & specs from OWC:**
- *"Starting at about $9,500 for 32 terabytes raw, which is about 23 terabytes usable"* — Mobile entry tier ([OWC Jellyfish lineup](https://www.owc.com/blog/the-jellyfish-collaborative-editing-server-lineup))
- *"about 4,400 megabytes per second aggregate speed"* — Tower/Rack throughput ([OWC](https://www.owc.com/blog/the-jellyfish-collaborative-editing-server-lineup))
- *"All OWC Jellyfish utilize ZFS RAID"* — architecture ([OWC](https://www.owc.com/blog/the-jellyfish-collaborative-editing-server-lineup))
- *"Up to 200TB Storage and Prices Up to $50,000"* ([MacRumors, 2018, Apple resale](https://www.macrumors.com/2018/12/12/apple-now-selling-lumaforge-shared-storage/))

**Editor / customer reviews:**
- *"SMB is the better connection for Adobe Premiere Pro CC"* — ProVideo Coalition's real-world test ([Scott Simmons, ProVideo Coalition](https://www.provideocoalition.com/real-world-day-editing-lumaforge-jellyfish-adobe-premiere-pro/))
- *"Once setup and connected it just worked. Editors were able to pound away without even thinking about it."* (same source)
- Larry Jordan's review of the Nomad calls it *"high-speed shared-storage for small workgroups"* ([Larry Jordan](https://larryjordan.com/articles/review-owc-jellyfish-nomad-high-speed-shared-storage-for-small-workgroups/))

## Verdict

Jellyfish's $5K–$50K hardware-included model is the **right answer for shops that want zero IT involvement and have a capex budget**, but it is the **wrong answer for shops that already have a NAS** or shops that want recurring opex over a $30K cheque. Jellyfish's premium is ~3–5× the cost of equivalent Synology/QNAP/TrueNAS hardware, and what you're paying for is the pre-tuned SMB/NFS config and the editor-fluent support — not the silicon.

JuiceMount can credibly compete by **delivering the "it just works" experience as software on top of any NAS**. A user who buys a $3,000 Synology DS1823xs+ and runs JuiceMount on top spends ~$3,300 and gets:
- The pre-tuned mount experience (JuiceMount handles the SMB/NFS quirks Finder hates)
- Library-wide instant search and Quick Look (Jellyfish has neither natively)
- Remote access without a separate Jellyfish Remote box
- A modern macOS-native client

That's a plausible 5–10× cost-per-seat advantage at the boutique tier (1–8 editors). At 12+ editors with mission-critical 4K finishing, Jellyfish's bundled support and tuned hardware still wins — JuiceMount should not chase that buyer initially. **The natural JuiceMount target on this axis is the freelance / small-studio editor priced out of Jellyfish**, plus the existing Jellyfish customer who needs remote-team access bolted on.

## Sources

- [OWC Jellyfish lineup overview](https://www.owc.com/blog/the-jellyfish-collaborative-editing-server-lineup)
- [OWC Jellyfish solutions page](https://www.owc.com/solutions/jellyfish)
- [Apple selling LumaForge servers up to $50,000 (MacRumors)](https://www.macrumors.com/2018/12/12/apple-now-selling-lumaforge-shared-storage/)
- [OWC acquires LumaForge (9to5Mac, 2021)](https://9to5mac.com/2021/01/14/owc-acquires-lumaforge-jellyfish-video-workflows/)
- [Real-world editing with Jellyfish + Premiere (ProVideo Coalition)](https://www.provideocoalition.com/real-world-day-editing-lumaforge-jellyfish-adobe-premiere-pro/)
- [Mixing Light: Shared storage case study](https://mixinglight.com/color-grading-tutorials/shared-storage-for-the-small-studio-case-study-with-the-lumaforge-jellyfish-mobile/)
- [Larry Jordan: Jellyfish Nomad review](https://larryjordan.com/articles/review-owc-jellyfish-nomad-high-speed-shared-storage-for-small-workgroups/)
- [CineD: OWC Jellyfish Studio explained](https://www.cined.com/owc-jellyfish-studio-explained-fast-massive-desktop-nas-for-your-editing-team/)
- [Newsshooter: OWC Jellyfish Studio](https://www.newsshooter.com/2024/09/18/owc-jellyfish-studio/)
