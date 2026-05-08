# Competitive Brief: General-Purpose NAS Vendors

Synology, QNAP, TrueNAS Mini (iXsystems), OWC ThunderBay, and the cautionary case of Drobo. These are the boxes a budget-conscious or self-hosting video editor actually buys when Jellyfish is too expensive.

## Overview

- **Synology** — Taiwanese vendor, founded 2000. The "iPhone of NAS" — polished DSM web UI, very large app catalog, the default recommendation for prosumers and small business. Latest video-editor flagship: **DS1823xs+** (2023, 8-bay AMD Ryzen, 10GbE built-in).
- **QNAP** — Taiwanese vendor, founded 2004. More feature-aggressive than Synology — first to ship 25GbE, ZFS, and PCIe expansion. Flagship for editors: **TVS-h874** (2022, 8-bay Intel Core i7/i9, ZFS, dual 10GbE).
- **TrueNAS Mini (iXsystems)** — California-based, the commercial arm behind FreeNAS / TrueNAS open-source. Targets engineers and prosumers who want enterprise-grade ZFS in a small box. Models: **Mini E, Mini X, Mini X+, Mini R**.
- **OWC ThunderBay** — Direct-attached Thunderbolt RAID, not a true network NAS. Sold as the cheap path to fast local storage for solo editors.
- **Drobo (defunct)** — Founded 2005 as Data Robotics. Pioneered "Beyond RAID" auto-managed storage. Filed Chapter 11 in 2022, converted to Chapter 7 liquidation in April 2023. **Cautionary tale for the entire category.**

Target buyer for the live vendors: solo editors, 2–8 person boutiques, IT-minded freelancers who want to save $20K vs Jellyfish and don't mind tweaking SMB themselves.

## Product capabilities

### Storage architecture

| Vendor | Filesystem / RAID | Bays | Max raw |
|---|---|---|---|
| Synology DS1823xs+ | Btrfs + SHR/RAID 0/1/5/6/10/F1 | 8 (expand to 18) | 160 TB |
| QNAP TVS-h874 | **ZFS** (QuTS hero) or ext4 | 8 | ~176 TB |
| TrueNAS Mini X+ | **OpenZFS** | 5x 3.5" + 2x 2.5" | ~110 TB |
| TrueNAS Mini R | **OpenZFS** | 12 | 200+ TB |
| OWC ThunderBay 8 | SoftRAID (host-based RAID 0/1/4/5/10) | 8 | 128 TB |

### Network connectivity

- **Synology DS1823xs+**: 1× 10GbE built-in + 2× 1GbE; expandable to **25GbE** via PCIe NIC.
- **QNAP TVS-h874**: depends on SKU — i9-64G ships with **dual 10GbE** standard; lower SKUs are 2.5GbE with optional cards. PCIe Gen 4 slots support 25GbE/50GbE add-in cards.
- **TrueNAS Mini X+**: **dual 10GbE** built in.
- **TrueNAS Mini R**: dual 10GbE; rack form factor with PCIe expansion.
- **ThunderBay 8**: **Thunderbolt 3 only** (40 Gb/s), single host — not a network device.

### Software stack

- **Synology**: DSM 7.x (Linux + custom). Closed-source app catalog, polished web UI.
- **QNAP**: QTS (ext4) or QuTS hero (ZFS). Linux-based. App store + Container Station.
- **TrueNAS**: TrueNAS SCALE (Debian) or CORE (FreeBSD). Open-source. Apps via TrueCharts/Helm.
- **ThunderBay 8**: macOS-side **SoftRAID Premium** application — no on-device OS.

### Sharing protocols

All three NAS vendors support **SMB, NFS, AFP (legacy/deprecated), iSCSI, FTP, WebDAV, rsync**. Synology DSM lists "SMB, AFP, NFS, FTP, WebDAV, Rsync" with up to **340 concurrent SMB connections**. ThunderBay is host-attached only — sharing happens via the Mac's own File Sharing.

### Multi-user features

- Synology/QNAP: full ACL, AD/LDAP integration, snapshots, quotas. Mature.
- TrueNAS: ZFS snapshots, dataset-level permissions, ACL. Less polished UI but more robust under the hood.
- ThunderBay: single user / single host (it's DAS).

### NLE-specific integrations

None of these are NLE-certified the way Jellyfish is. They expose generic SMB/NFS volumes. Synology has tested setups for Premiere/Resolve in marketing copy. TrueNAS has no NLE certification at all. QNAP markets HDMI output for "video production/editing" but it's a generic claim.

### Asset management

Synology has **Synology Photos** (photos only, video metadata is shallow) and **Video Station** (media server, not editorial). QNAP has **Qsirch** (full-text search) and the Multimedia Console. TrueNAS has none. **None of them ship a video-editorial DAM** — that gap is real.

### Backup/redundancy

- Synology: Hyper Backup, Snapshot Replication, USB/cloud destinations.
- QNAP: HBS 3, ZFS snapshots (QuTS hero), immutable snapshots vs ransomware.
- TrueNAS: ZFS snapshots, replication to a second TrueNAS, S3/B2 cloud sync.
- ThunderBay: SoftRAID parity only — no remote replication.

## Pricing

| Product | Diskless / barebone | With drives |
|---|---|---|
| **Synology DS1823xs+** | **$1,799.99 MSRP** (diskless) | ~$3,300 with 8× 4TB drives |
| **QNAP TVS-h874-i5-32G** | ~**$2,165** (barebones) | i9-64G typically £3,000+ ($3,800+) barebones |
| **TrueNAS Mini X** | from **$899** (diskless) | $1,499 with 4× 4TB |
| **TrueNAS Mini X+** | $1,499 (diskless) | $2,949 (50TB) / under $3,500 (70TB) |
| **TrueNAS Mini R** | "starts at under $2,000" diskless | varies |
| **OWC ThunderBay 8** | **$749.99 MSRP** (enclosure only) | $1,219.99 (16TB) → $5,299.99 (128TB) |

Drives are sold separately for Synology/QNAP/TrueNAS. ThunderBay sells both empty and bundled.

**Software licensing**: Synology and QNAP are bundled — no per-seat license. TrueNAS Community Edition is **free open-source**; iXsystems sells optional support contracts. ThunderBay's SoftRAID Premium is **bundled with the array**.

**Support contracts**: Synology has tiered "Synology Care" (a few hundred dollars/year). QNAP's standard 3-year hardware warranty extends with paid plans. iXsystems sells silver/gold/platinum support starting at hundreds per year. None come close to Jellyfish-tier hand-holding.

**5-year TCO (50TB usable, 4-editor shop):**
- Synology DS1823xs+ + 8 drives + extended warranty + 1 cold-spare drive replacement: **~$5,000–$6,500**
- QNAP TVS-h874 equivalent: **~$5,500–$7,500**
- TrueNAS Mini X+ + drives: **~$4,500–$5,500**
- vs Jellyfish Tower 53TB usable at ~$30,000+: **5–6× cheaper**
- vs cloud (Frame.io/Iconik + storage at ~$15K–$25K/yr): **~$75K–$125K over 5 years** for the same team

## Strengths

- **One-time hardware cost** dominates cloud over any 18+ month horizon.
- **LAN-speed performance**: Synology DS1823xs+ does 3,100 MB/s read / 2,600 MB/s write; QNAP TVS-h874 has dual 10GbE (2.5 GB/s) and PCIe Gen 4 for 25/50GbE cards; TrueNAS Mini X+ does ~2 GB/s.
- **Data sovereignty**: same as Jellyfish — your footage stays in the building. NDA-friendly.
- **Open ecosystems**: TrueNAS in particular runs ZFS that any other ZFS host can import — no proprietary lock-in.
- **Far cheaper than Jellyfish** for equivalent specs.

## Weaknesses

- **Capex up front, even if smaller than Jellyfish.** $3K minimum is still a barrier for true freelance.
- **DIY tuning required.** Synology/QNAP both require Mac SMB tuning that Jellyfish ships pre-done. Common Mac fix: `defaults write com.apple.desktopservices DSDontWriteNetworkStores -bool TRUE` ([Synology KB](https://kb.synology.com/en-af/DSM/tutorial/smb_speed_up_finder_browsing)).
- **Single-site by default.** Remote editor access requires Tailscale/Wireguard/QuickConnect — none are NLE-grade.
- **Capacity ceiling = bay count + expansion units** (Synology adds DX517 expansion shelves, QNAP has REXP, TrueNAS adds JBODs).
- **Drobo's collapse is the elephant in the room.** StorCentric (Drobo's parent) filed Chapter 11 in **June 2022** and converted to **Chapter 7 liquidation in April 2023**. Drobo's proprietary "Beyond RAID" filesystem is now a permanent liability for surviving customers — *"the main challenge with recovering files from Drobo devices is its proprietary Beyond RAID technology, which is incompatible with standard recovery tools."* ([ACE Data Recovery](https://www.datarecovery.net/articles/discontinued-drobo-arrays-self-service-recovery.aspx))
- **NAS app stores have UX from 2010.** Synology's web UI has aged; QNAP's is denser and more confusing; TrueNAS SCALE looks better but has fewer apps.
- **Security exposure.** QNAP has been hit by **DeadBolt, Qlocker, eCh0raix** ransomware campaigns; CVE-2022-27593 affected Photo Station and allowed remote unauthenticated ransomware deployment ([Help Net Security](https://www.helpnetsecurity.com/2022/09/12/cve-2022-27593/)).

## What JuiceMount can credibly beat them on

- **Adds remote access on top of an existing NAS.** Tailscale + SMB over WAN is brittle; JuiceMount can present a far-side NAS as a local volume without VPN gymnastics.
- **Search across the whole library.** Synology Mac users frequently report *"Finder was reported to be very slow opening folders on mounted SMB shares"* — JuiceMount's local SQLite index sidesteps the SMB-browse bottleneck entirely.
- **Quick Look across the network.** Generating thumbnails on demand over SMB is the single most-cited Synology Mac complaint. JuiceMount caches thumbnails and previews locally, asynchronously.
- **Multi-machine sync.** None of the NAS vendors offer real-time cross-machine cache coherence. JuiceMount can.
- **Modern macOS-native client.** Synology Drive and QNAP Qsync are cross-platform Electron apps. JuiceMount is a Swift menu-bar app written for Mac editors.
- **Works WITH the existing NAS.** JuiceMount is not a NAS replacement — it's the access layer. The Synology stays the bulk-storage backend.

## Direct quotes / evidence

**Synology DS1823xs+:**
- *"sequential read/write speeds: 3,100 MB/s / 2,600 MB/s"* ([Synology product page](https://www.synology.com/en-us/products/DS1823xs+))
- *"SMB, AFP, NFS, FTP, WebDAV, Rsync"* with *"maximum 340 SMB connections"* (same)
- MSRP **$1,799.99** diskless ([B&H product page](https://www.bhphotovideo.com/c/product/1754153-REG/synology_ds1823xs_8_bay_nas_enclosure.html))

**QNAP TVS-h874:**
- *"Dual 10GbE ports provide 2.5 GB/s bandwidth (23x faster than 1GbE)"* ([Club386 review](https://www.club386.com/qnap-tvs-h874-8-bay-smb-nas-review-one-nas-to-rule-them-all/))
- *"Highly-reliable ZFS-based storage with PCIe Gen 4 expandability for 10/25GbE connectivity"* ([QNAP](https://www.qnap.com/en-us/product/tvs-h874))
- i9-64G "expected to fetch comfortably in excess of £3,000" ([QNAP US](https://www.qnap.com/en-us/product/tvs-h874))

**TrueNAS Mini X+ / Mini R:**
- *"5+2 Drive Bays, 32GB RAM, Eight Core CPU, Dual 1/10 Gigabit Network"* ([Amazon Mini X+ listing](https://www.amazon.com/iXsystems-Mini-X-Diskless/dp/B08FCWBVWX))
- *"12 lockable and hot-swappable 3.5" drive bays for more than 200TB of raw capacity when fully populated with 18TB drives"* ([TrueNAS blog](https://www.truenas.com/blog/meet-the-mini-r/))
- *"the self-healing, enterprise OpenZFS file system"* ([TrueNAS Mini page](https://www.truenas.com/truenas-mini/))

**OWC ThunderBay 8:**
- *"sustained data transfer speeds of up to 2586 MB/s"* and "**up to 128 terabytes of data**" ([OWC](https://www.owc.com/solutions/thunderbay-8))
- $749.99 enclosure / $5,299.99 fully populated ([OWC / B&H](https://www.bhphotovideo.com/c/product/1635298-REG/owc_owctb38jbkit0_thunderbay_8_0tb_thunderbolt.html))

**Drobo (defunct):**
- *"StorCentric, the holding company for Drobo and Retrospect, initially filed for Chapter 11 in late June 2022, but the bankruptcy shifted to Chapter 7 liquidation in late April 2023"* ([Slashdot](https://hardware.slashdot.org/story/23/05/16/2013226/drobo-having-stopped-sales-and-support-reportedly-files-chapter-7-bankruptcy))
- Photographer Raoul Pop in 2019: *"the time a Drobo lost over 30,000 of my photos and videos"* — and Drobo Sales never responded ([Raoul Pop](https://raoulpop.com/2019/01/11/the-time-drobo-lost-over-30000-of-my-photos-and-videos/))
- Scott Kelby, 2012: *"I'm done with Drobo"* — his Drobo "became a brick" four times ([Scott Kelby](https://scottkelby.com/im-done-with-drobo/))

**Mac SMB performance pain (the recurring NAS complaint):**
- *"Finder was reported to be very slow opening folders on mounted SMB shares, and mounted SMB shares would sometimes disappear randomly"* ([SynoForum thread](https://www.synoforum.com/threads/mac-clients-smb-cache-issues.7009/page-2))
- *"Apple Finder requires a full refresh every time you change a folder, whereas Windows boxes can cache that and retrieve just the delta"* (same forum)

## Verdict — who are JuiceMount's natural early adopters?

**TrueNAS users are the clearest early adopters.** They are already self-hosting, already comfortable with SSH and ZFS, and already frustrated by the lack of a polished macOS client for their NAS. iXsystems ships zero NLE-tuned tooling and no first-party Mac browse/search experience — JuiceMount fills that gap precisely. The TrueNAS demographic also overlaps strongly with the "expensive editor / cheap storage" cohort: a freelancer or small studio that spent $3,000 on a Mini X+ specifically to avoid Synology's lock-in. They will pay for software that makes the NAS pleasant on macOS.

**Secondary early adopters are Synology Mac users with an existing slow-SMB pain point** — there is a multi-year history of forum complaints about Finder browsing speed against Synology shares ([SynoForum thread on slow SMB/NFS](https://www.synoforum.com/threads/very-slow-smb-nfs.12497/), [Synology community: extremely slow share browsing](https://community.synology.com/enu/forum/17/post/92767)). These users have already paid for the storage; they want a better client.

**Tertiary: ThunderBay 8 owners who want to extend a single-machine DAS into a shared workflow** — JuiceMount on a host Mac can re-share a ThunderBay over the network with editor-grade performance and search, without buying a second box.

**Avoid (initially): QNAP customers** — they are price-sensitive, often Windows-leaning, and burned out on QNAP-specific security incidents. The first $0 they have to spend on third-party software will go to a backup tool, not a Mac client. **Avoid: existing Drobo refugees** — they have low remaining trust in third-party storage software and need a hardware answer, not a software layer, before they will trust any storage tooling again.

The Drobo collapse is the strongest argument for JuiceMount's positioning: **"do not lock your media to a vendor's proprietary stack."** JuiceMount runs on top of standard SMB/NFS — if JuiceMount disappears tomorrow, every byte is still readable on the underlying NAS. That is the opposite of Beyond RAID.

## Sources

- [Synology DS1823xs+ product page](https://www.synology.com/en-us/products/DS1823xs+)
- [Synology DS1823xs+ at B&H](https://www.bhphotovideo.com/c/product/1754153-REG/synology_ds1823xs_8_bay_nas_enclosure.html)
- [Videomaker: NAB 2023 Best Storage award](https://www.videomaker.com/news/nab-2023-synology-ds1823xs-takes-home-best-storage/)
- [Dong Knows Tech: DS1823xs+ review](https://dongknows.com/synology-ds1823xs-powerful-2023-smb-nas-server/)
- [QNAP TVS-h874 product page](https://www.qnap.com/en-us/product/tvs-h874)
- [Club386: QNAP TVS-h874 review](https://www.club386.com/qnap-tvs-h874-8-bay-smb-nas-review-one-nas-to-rule-them-all/)
- [StorageReview: QNAP TVS-h874](https://www.storagereview.com/review/qnap-tvs-h874-nas-review)
- [Help Net Security: QNAP DeadBolt CVE-2022-27593](https://www.helpnetsecurity.com/2022/09/12/cve-2022-27593/)
- [TrueNAS Mini configure & buy](https://www.truenas.com/configure-and-buy-truenas-mini/)
- [TrueNAS blog: Meet the Mini R](https://www.truenas.com/blog/meet-the-mini-r/)
- [TrueNAS Mini X+ on Amazon](https://www.amazon.com/iXsystems-Mini-X-Diskless/dp/B08FCWBVWX)
- [OWC ThunderBay 8 solutions page](https://www.owc.com/solutions/thunderbay-8)
- [OWC ThunderBay 8 at B&H](https://www.bhphotovideo.com/c/product/1635298-REG/owc_owctb38jbkit0_thunderbay_8_0tb_thunderbolt.html)
- [Drobo Chapter 7 liquidation (Slashdot)](https://hardware.slashdot.org/story/23/05/16/2013226/drobo-having-stopped-sales-and-support-reportedly-files-chapter-7-bankruptcy)
- [PetaPixel: Drobo files Chapter 11](https://petapixel.com/2022/07/18/storage-company-drobo-files-for-chapter-11-bankruptcy/)
- [Raoul Pop: Drobo lost 30,000 photos/videos](https://raoulpop.com/2019/01/11/the-time-drobo-lost-over-30000-of-my-photos-and-videos/)
- [Scott Kelby: I'm done with Drobo](https://scottkelby.com/im-done-with-drobo/)
- [ACE Data Recovery on Drobo Beyond RAID](https://www.datarecovery.net/articles/discontinued-drobo-arrays-self-service-recovery.aspx)
- [Synology KB: speed up Mac Finder over SMB](https://kb.synology.com/en-af/DSM/tutorial/smb_speed_up_finder_browsing)
- [SynoForum: Mac clients SMB cache issues](https://www.synoforum.com/threads/mac-clients-smb-cache-issues.7009/page-2)
- [SynoForum: very slow SMB/NFS](https://www.synoforum.com/threads/very-slow-smb-nfs.12497/)
