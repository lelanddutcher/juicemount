# Brand Identity — v1

> **Audience for this doc:** Leland.
> **Status:** Opinionated draft. Picks favorites, justifies them, ships. Source material: `positioning.md` (v2), `pain-points.md`, `competitive/suite.md`, `competitive/shade.md`. Trademark spot-checks done via web search; full WHOIS/USPTO before launch is a separate ticket.

---

## 1. Naming

### The frame

The product is, technically: an open-source NFS loopback mount with a SQLite catalog over JuiceFS-style object storage, with a Mac menu bar app on top. The audience is the Sovereign Video Engineer described in `positioning.md` — a TrueNAS user who already self-hosts, runs Docker compose, and reads HackerNews. That audience has high tolerance for technical names (kubectl, ffmpeg, helm, restic, MinIO) but the *secondary* audience — the boutique post house, the freelancer who doesn't run their own NAS — does not. The hosted tier described in the positioning doc is tier 3, but it exists, and the name has to survive it.

The other constraint: this is a product about **trust with the master files of someone's career**. A name that sounds like a 2014 Stripe-clone YC startup (TrustlyKit, FlowDrive, Mediora, Cinemind) loses on the first read. A name that sounds like infrastructure — like something a sysadmin would deploy and not have to explain to the CFO — wins.

### Candidates

**1. Strata** — geological layers, archive depth.
- Feel: technical, neutral, evocative, slightly serious.
- Trademark risk: **High.** Strata 3D (creative-adjacent), Strata.io (identity), Azenta's Strata (storage, since renamed). Crowded.
- Launch test: "Strata mounts your S3 bucket…" works, but you immediately have to disambiguate from Strata 3D.
- **Verdict: Don't.**

**2. Reel** — the thing every editor knows.
- Feel: warm, on-the-nose, descriptive.
- Trademark risk: moderate. OpenReel exists; "reel" appears in dozens of marks but none own it for storage.
- Launch test: "Reel is the Finder for your media library." Reads fine, but the name does no narrative work.
- **Verdict: Skip.** Too generic to defend, too obvious to be memorable.

**3. Loft** — where you keep what matters; warm, architectural.
- Feel: warm, neutral, literary, Mac-design coded.
- Trademark risk: **Low-moderate.** Loft.sh is Kubernetes tooling — ideologically adjacent (devs, OSS) but a different product. Nobody owns "Loft" for storage/media.
- Launch test: "Loft turns the bucket you already pay for into a Finder drive — your bytes, where you put them, fast as your LAN." Strong. The attic metaphor is intuitive without being precious.
- **Verdict: Strong runner-up.** Survives the trip from OSS to hosted without losing dignity.

**4. Cobble** — *Kill.* ASUS sells the "Cobble SSD Enclosure" — literal storage hardware. Fatal conflict.

**5. Anchor** — *Kill.* Anchor Point Studio (anchorpoint.studio) is version control for creative teams — same exact buyer. Will cause confusion.

**6. JuiceMount** — the current internal name. JuiceFS + Mount.
- Feel: technical, slightly playful, OSS-coded, mildly clumsy when said out loud.
- Trademark risk: low. The OSS-derived compound name is precedented (Helm/Helmsman, Kube/Kubectl).
- Domain heuristic: juicemount.com / .app almost certainly available.
- Launch test: "JuiceMount is the open-source Finder mount for object storage…" reads as a tool by tool-people. HackerNews-friendly. Loses points if you imagine a non-technical buyer.
- **Verdict: Defensible for OSS v1.** Will carry through tier 1 and 2. The tier 3 hosted product can be **Loft**.

**7. Vault** — *Kill.* HashiCorp Vault owns this in software-the-engineer-trusts.

**8. Cue** — minimal, monosyllabic.
- Trademark risk: low-moderate in our space.
- Launch test: "Cue mounts your storage and gets out of your way." Clean, but the name does very little narrative work.
- **Verdict: Better as a feature name than a parent brand.**

### Recommendation

**Ship as JuiceMount for the OSS v1.** Rename the moment the hosted/Pro product needs its own identity — most likely candidate: **Loft.** Justification:

1. **JuiceMount is honest about what it is.** It's a derivative of JuiceFS that mounts object storage. The audience for OSS v1 — the Sovereign Video Engineer — *prefers* technical names. They will install it on a TrueNAS and tell their friends about it on r/selfhosted, and "JuiceMount" reads as a tool by tool-people. That's the trust signal.
2. **The cost of renaming is real but bounded.** Once the OSS project has the GitHub org, the ecosystem is named. Tailscale didn't rename its OSS roots (WireGuard) because it didn't need to. JuiceMount can carry the OSS forever; the SaaS/hosted layer can be **Loft** with JuiceMount as the engine ("Powered by JuiceMount" footer) — the same way Tailscale powers Headscale, or the way Vercel sits on top of Next.js.
3. **Loft beats Strata, Reel, Cue, Anchor, Vault, Cobble.** It's not crowded in our space, the metaphor is warm and intuitive (an attic where the good stuff lives), and it survives the trip from "$60/mo open-source" to "$X/mo hosted" without losing dignity. The runners-up either had hard trademark blocks (Cobble, Anchor, Vault) or were too generic to defend (Reel, Cue).
4. **Don't pre-pay the rename cost.** v1 is a public launch on HN and r/truenas. The audience there likes JuiceMount as a name. Spending the team's first six weeks on naming-and-branding instead of shipping the search/Quick Look polish would be a tactical error. Get product-market fit signals from JuiceMount; rename to **Loft** when the hosted tier is real.

**Bottom line:** JuiceMount v1 (OSS), Loft v2 (hosted/Pro). Both names belong to the same company. The visual identity below is designed to survive the JuiceMount → Loft transition without rework.

---

## 2. Tagline candidates

Picking from the three angles in the brief:

**Anti-lock-in:**
1. *Your bytes are your bytes.* — clean, six words, ideologically loaded, lands on the OSS audience
2. *Cloud-mounted storage, without the cloud lock-in.* — descriptive but flat; nine words, too long
3. *Walk away with `mc cp`.* — true to the technical audience, hostile to the non-technical one

**Price/sovereignty:**
4. *$60 a month, not $750.* — the most provocative; lands like a punch on the comparison page; risky if pricing shifts
5. *No per-seat tax. No vendor lock-in. Bring your own bucket.* — three-clause structure; works for a hero subhead, not a tagline

**Workflow:**
6. *The Finder for your media library.* — does the most narrative work; instantly legible; pairs with "It's where Quick Look, Spotlight, and Cmd+Shift+F just work."
7. *Open-source storage, professional-grade.* — establishes both halves of the audience promise

### Recommended

**Primary tagline: "Your bytes are your bytes."**
**Backup: "The Finder for your media library."**

The primary is ideologically the strongest claim the product can make and the one *no competitor* can copy. Suite, Frame.io, Shade, and LucidLink can all match a workflow tagline ("the Finder for your media library") with marketing copy. None of them can claim "your bytes are your bytes" without rebuilding their architecture. The tagline names the structural advantage from `positioning.md` § "JuiceMount's wedge" and turns it into five syllables.

Use "The Finder for your media library" as the hero subhead, because it does the descriptive work the primary tagline doesn't. Pair them on the landing page like:

> # Your bytes are your bytes.
> The Finder for your media library — open source, self-hosted, and 12× cheaper than Suite.

---

## 3. Voice & tone

The voice is **Tailscale meets Backblaze, with a colorist's vocabulary.**

Tailscale is the closest reference. The Tailscale blog reads like an engineer who has shipped real things, tells you what the product *is* before what it *means*, and refuses to over-promise. They'll say in the same paragraph that something is hard, that they did it anyway, and how it works — and the reader trusts them more for the candor. The audience is technical enough to detect marketing-speak immediately and to penalize you for it.

Backblaze is the secondary reference, for one reason: pricing transparency. They publish a quarterly drive-failure report and their margin economics. That radical transparency is the right move for a product whose pitch is "you won't be surprised by your bill." Adopt the habit early — publish the cost calculator, publish what it costs us to host the hosted tier, publish what fraction of users overflow which limit.

Linear is the model for **clarity over cleverness.** Sentences are short. The word "delightful" never appears. Default to less.

What the voice is *not*: Adobe (corporate), Suite (founder-quote-heavy aspirational), Frame.io (post-acquisition glossy), Shade (designer-forward, bordering on whimsical). The brand is **dry, exact, and on the editor's side.** Editors don't need a friend in their tooling — they need a tool that doesn't lie to them.

### Sample paragraphs

**Explaining the product to a stranger (~30 words):**
> JuiceMount mounts an S3-compatible bucket as a native Finder volume on macOS. Search, Quick Look, and drag-into-Premiere all work. Your bytes stay in your bucket. The mount is open source.

**Acknowledging a limitation honestly (~30 words):**
> JuiceMount does not do AI semantic search. If "find every sunset shot" is the feature you need, Shade is better at that than we are. We're better at owning your storage.

**Pricing transparency (~30 words):**
> 10 TB on Backblaze B2 costs $60/month. JuiceMount adds nothing. There is no per-seat fee, no surprise overage, and no SaaS subscription. Walk away any time with `aws s3 sync`.

---

## 4. Visual direction

The visual brand is **Apple Pro app meets self-hosted infra dashboard.** Logic Pro's Color Picker, the dark side of Final Cut Pro, Linear's dark mode, the syntax highlighting in a sensible Vim colorscheme. *Not* the marketing site of a Series A SaaS.

### Primary palette

A six-color palette built around a deep neutral, a warm accent, and three workflow-state colors. All hexes tested for AA contrast on dark and light backgrounds.

| Token | Hex | Role |
|---|---|---|
| `oil` | `#0D0F12` | Primary background, dark mode. The color of a Resolve viewer with no clip loaded. |
| `slate-700` | `#1E2128` | Surface, dark mode. Cards, panels, menu bar popover. |
| `slate-300` | `#A6ADBB` | Secondary text on dark. |
| `parchment` | `#F4F1EA` | Primary background, light mode. Warmer than pure white — the off-white of a printed shot list. |
| `juice` | `#E8743C` | Single brand accent. A burnt orange — between Alpenglow and a Resolve clip-color tag. Used sparingly: the menu bar icon, primary CTA, the active state of the search bar. |
| `signal-green` | `#5DBF8C` | "Cached and ready" / online / verified. |
| `signal-amber` | `#E0A458` | "Streaming from cloud" / partial / warning. |

Rules: dark mode is the primary surface. Light mode exists and is supported. The accent (`juice`) appears at most twice on any screen — restraint is the point. No gradients except on the menu bar icon's subtle inner-light. No glassmorphism. The palette resolves debates about color: if a designer asks "can we add purple here?" the answer is no.

### Typography

- **Display:** **Söhne** (if budget) or **Inter Display** (free, Google Fonts). Used at hero scale only — landing page H1, app onboarding header. Tight tracking, slightly tall x-height. *Not* Inter Tight, *not* Geist (overused).
- **Body:** **Inter** (regular and medium weights). Boring, readable, free, looks correct on macOS. Linear, Plausible, Vercel use it; we use it too.
- **Monospace:** **JetBrains Mono** for code blocks, file paths, hashes, and the cost-calculator readout. Slightly more personality than SF Mono. The product surface uses mono for any byte-level number — file sizes, hashes, transfer rates, mount points. This is a deliberate signal: "we are honest about what these numbers are."

### Iconography

The menu bar app already uses SF Symbols (`externaldrive.*`). Stay there — SF Symbols match the macOS visual language for free, and native-feeling apps are expected to use them. Five motifs to build around on non-symbol surfaces:

1. **Mount-arrow** — arrow plugging into a horizontal line. "This connects to that." Brand mark candidate (§ 7).
2. **Cube-with-folder** — object storage made legible as a folder. The duality the product collapses.
3. **Terminal cursor** — opt-in. Signals "you can also drive this from the CLI" without alienating GUI-first users.
4. **Thumbnail-with-spinner** — cache state, for diagrams about Quick Look.
5. **Horizontal stack (strata)** — your library as layers of projects, layers of years.

Avoid: cloud-with-arrow icons (Adobe, Suite, Frame.io all do this), gear icons, anything that looks like an enterprise dashboard.

### Imagery direction

When we shoot or source imagery for the landing page, the rule is: **show the thing, not the people.**

Yes:
- A real Finder window with a real 100K-file library and a real Cmd+Shift+F search returning real results in 47ms (with the timestamp visible in the corner).
- Macro photography of a Synology DS1823xs+ with the front bezel off, drives visible. (Or a Jellyfish, or a Mac Studio with a Thunderbolt RAID. Hardware is allowed and welcome.)
- The waveform of a R3D file in Quick Look. The thumbnail grid of an ARRI shoot.
- Color-graded stills from real customer shoots (with permission and credit).

No:
- Stock photo of two people pointing at a laptop in a sunlit office.
- A "post-production team huddled around a confidence monitor" hero image.
- Aerial drone shots of a city skyline with no relationship to the product.
- AI-generated anything. Not for cost reasons — for trust reasons. The audience can spot it and will deduct.

### One-line rule

**"Boring on the outside, fast on the inside."**

This resolves arguments. When somebody pitches an animated hero illustration, a marquee testimonial carousel, a "playful 404 page" — answer with the rule. The audience came here because the post-production tooling industry is full of shiny products that hide their bills behind marketing copy. JuiceMount is the product that doesn't do that. The visual brand should under-promise on the surface and over-deliver in the workflow.

---

## 5. Three messaging pillars

### Pillar 1 — Your bytes are your bytes.

JuiceMount writes plain objects to a bucket the user owns. There is no proprietary filesystem, no extraction tool, no migration script. If JuiceMount disappears tomorrow, the user runs `aws s3 sync s3://my-bucket/ ~/Movies/` and walks away with every file intact. Suite, Shade, LucidLink, and Frame.io Drive cannot say this — their value lives in a closed filesystem layer. This is a structural property of the architecture, not a feature, and it is what every cautionary tale about Drobo's 2023 liquidation tells the audience to demand.

### Pillar 2 — 12× cheaper, with no seat tax, ever.

10 TB of media on Backblaze B2 costs $60/month total. The same workload on Suite Storage costs $750/month. Even Suite BYO costs $400/month plus the bucket bill. Frame.io Drive's Pro tier is $75/month at five seats and inherits Adobe's "we'll suspend your account if you go one byte over" billing posture (Elie O., Capterra, May 2024). JuiceMount has no per-seat fee, ever. Add the 11th editor without a calendar invite from sales. The pricing isn't us being scrappy — it's the natural consequence of having no SaaS markup layer.

### Pillar 3 — Local-first, by architecture.

Cloud-streaming products have a structural ceiling: ARRIRAW at 4.5K runs ~2 Gbps sustained, and even a 1 Gbps fiber line drops frames on it (per Capterra LucidLink reviews). JuiceMount inverts the model. Files live where you put them — your NAS, your DAS, your laptop SSD, optionally a cloud bucket — and the mount runs at LAN speed for cached reads. AWS outages don't stop your edit. Hotel WiFi doesn't cap your timeline. The cloud is a *configuration option*, not the architecture.

---

## 6. Brand do/don't guardrails

| Do | Don't |
|---|---|
| Quote real editors with names and Capterra/forum citations. | Manufacture testimonials or use stock-photo-coded quotes. |
| Publish the actual pricing table on the landing page, including the per-TB math for B2, R2, Wasabi. | Say "contact sales for pricing." |
| Show the cost calculator as code the user can read. | Put the cost calculator behind an email-gated form. |
| Honor cancellation in one click. Refund pro-rated subscription days. | Suspend accounts for going over a limit. (Editors talk about this — see Frame.io Trustpilot.) |
| Document failure modes and what happens when the mount disconnects. | Hide failure modes behind "it just works." |
| Cite competitors by name when comparing. Link to their pricing pages. | Use coy rivals like "the leading cloud-streaming product." Editors know who you mean. |
| Ship a public CHANGELOG with dates and authors, even for the OSS. | Backdate releases or stealth-edit blog posts. |
| Publish the full hosted-tier margin model when we launch the hosted tier. | Treat the hosted tier as a black box. |

---

## 7. Logo direction

**Direction 1 (recommended): The mount arrow.** A wordmark plus a single glyph: a horizontal line with a small arrow plugging into it from below. The visual primitive for "this connects to that." Compare to Tailscale's mesh dot pattern or Cloudflare's cloud-with-arrows — one geometric idea, instantly recognizable at favicon size. Arrow in `juice` orange, line in body-text color, single-color fallback for footers and READMEs. Pros: scales from 16px favicon to billboard, survives the JuiceMount → Loft rename without redesign. Cons: minimal abstract marks risk being forgettable — differentiation has to come from typography and color discipline.

**Direction 2 (runner-up): The labeled cube.** Isometric cube with a folder-tab cut out of one face. Pictorial, friendlier, tells you what the product is in one image. Compare to Backblaze's lit-cube or DigitalOcean's drop. Pros: more memorable, sticker-able. Cons: skeuomorphic, risks looking like enterprise infra (Veeam, Cohesity, Pure) — the audience we *don't* want.

**Direction 3 (don't): A juice-themed pun.** Citrus slice, juice glass, drop. **Reject.** Undermines "your bytes are your bytes" with a visual that says "we're a fun startup." Save the juice metaphor for the wordmark color and an Easter-egg April 1 favicon — not the primary mark.

**Recommended path:** ship direction 1 in week one. Wordmark: "JuiceMount" in Söhne (or Inter Display) all-lowercase, slightly tracked, with the mount-arrow glyph as a separable mark. The wordmark text can later swap to "Loft" without redesigning the glyph.

---

## Deliberate non-decisions

- **No motion language yet.** Animation strategy comes when there's an app to animate.
- **No social/template system yet.** OG images, Twitter cards — fine, but only after the fundamentals ship.
- **No mascot.** The audience does not want this.

The brand identity exists to ship the launch, not to delay it. Every decision here was picked to be reversible after we get product-market fit signals — but reversible decisively, not paralytically.
