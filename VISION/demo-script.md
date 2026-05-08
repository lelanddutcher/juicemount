# Demo Script — JuiceMount in 3 Minutes

> **Purpose:** the demo a Show HN reader, an investor, an editor friend, or NoFilmSchool's video team watches once and immediately understands the product.
> **Length target:** 2:50 — long enough to land 3 wow moments, short enough that nobody bails.
> **Voice:** Tailscale + Backblaze + Linear (per `brand-identity.md`). Dry, exact, no hype, no music swells.
> **Set:** A real Mac with a real JuiceMount-mounted volume. Real R3D and audio in `/Volumes/zpool`. No staged data.

---

## Cold open (0:00–0:10)

**[Hard cut to a Finder window at /Volumes/zpool. Visible: a folder tree of Film Projects / Video Editing Assets / Footage / SFX. Realistic — what an actual editor would have.]**

**Voice:**
> "This is a 12-terabyte media library. It doesn't live on this machine. It lives in an S3 bucket on a server in my closet. Watch what happens when I do normal editor things to it."

**[On-screen text overlay, top-left, small caption font:]**
*JuiceMount · /Volumes/zpool · backed by MinIO on a TrueNAS box · cost: $0/month*

---

## Beat 1 — Spacebar Quick Look on R3D (0:10–0:50)

### What you do
1. Navigate to `Footage/Camera Master/A002_C015_240315.R3D` — a 4 GB RED RAW file.
2. Hit **spacebar**.
3. Quick Look opens, plays at 24 fps within ~200 milliseconds.
4. Scrub the timeline. Smooth.
5. Press space again to dismiss.
6. Spacebar another file (a `.braw`). Same — instant play.

### What you say
> "This is a 4-gigabyte RED RAW file. Frame.io Drive's own optimization docs say their mount can't handle this — they tell you to disable analyzers and route media cache to local SSD. Suite Studios doesn't even market spacebar preview as a feature. JuiceMount detects the codec, generates a 720p H.264 proxy on first read using VideoToolbox hardware acceleration, caches it, and serves it to Quick Look. Second read is instant."

### What's on screen
- Quick Look panel shows the R3D playing smoothly
- Caption: *"R3D · 4.2 GB · proxy generated in 870ms · played in 230ms"*
- Top-right of screen, a small "Proxy ready · cached" status bubble

### Why this beat works
This is the one thing nobody else can demonstrate. Suite + Frame.io can't because their architecture is bandwidth-bound and they don't own the encoder. Editors who've fought RED Quick Look on a NAS for years will sit forward in their chairs.

---

## Beat 2 — Cmd+Shift+F search (0:50–1:30)

### What you do
1. Hit **Cmd+Shift+F** from anywhere — even outside Finder.
2. A clean search window opens. Search field has focus.
3. Type **"explosion"**. Results stream in as you type.
4. After "explos" — 47 results across the library, all in <50ms. Show the count in the corner.
5. Arrow-key down to a `.wav` (sound effect). **Spacebar.** It plays.
6. Arrow-key down to a `.mov`. **Spacebar.** Inline preview, smooth.
7. **Drag** that .mov out of the search window into a Premiere bin. Drop. Done.

### What you say
> "Search across the entire library, in 47 milliseconds, across — let me show you — 131,000 files. This is SQLite FTS5 with a trigram tokenizer running on the metadata index. Spotlight skips network volumes; Iconik's equivalent search opens a browser tab and round-trips to their cloud and costs $9 to $120 per user per month. This is in Finder, free, and it's already on your machine."

### What's on screen
- Search window with juice-orange accent on the result count
- Caption: *"131,247 files indexed · 47 results · 41ms"*
- The drag-out gesture into Premiere visible briefly

### Why this beat works
Sound designers, colorists, and editors all live and die by "find the take I'm thinking of." Iconik and Shade have made this a $$$ feature. We're showing it integrated into Finder for $0.

---

## Beat 3 — Backup verification + Safe to Delete (1:30–2:30)

### What you do
1. Click the menu bar icon. Open the popover.
2. Click **"Backups"** tab. The traffic-light view loads.
3. Show the headline number: **131,247 files · 🟢 124,891 green · 🟡 5,902 yellow · 🔴 454 red**.
4. Click **🔴 454 red**. Filter view, sorted by file size.
5. Show the top entry: a 2.4 GB R3D file. **Detail panel shows:** 3 targets verified. Local NAS hash matches B2 hash. **USB backup hash differs.**
6. Open Activity Monitor for a beat — show the verification ran in the background, hashing 1.2 GB/sec.
7. Right-click the file → **"Safe to delete?"** Modal pops up: 🔴 **No — 1 verified copy on 2 other targets, but the third has silent corruption. Refusing.**
8. Click another file (this one has 4 verified copies). Right-click → "Safe to delete?" 🟢 **Yes — 3 verified copies on 3 targets.**

### What you say
> "Backups are a lie until the bytes are verified. JuiceMount walks every backup target you configure — your NAS, your USB drive, your B2 bucket, your LTO archive — computes SHA-256 of every file, and surfaces a per-file traffic light. Same file size on the backup but different bytes? Red. That's silent bit-rot — the kind your rsync didn't catch because it never read the file back. The 'Safe to delete?' check refuses unless at least two OTHER targets verify the same content. Toy Story 2 trauma, solved."

### What's on screen
- Backups panel — clean traffic-light counts at top
- Detail row showing the corruption diff with target names
- Caption when SafeToDelete refuses: *"only 1 verified copy on 2 other targets — refusing"*
- Visible target list: `local:/Volumes/zpool · local:/Volumes/usb-backup · b2://my-bucket`

### Why this beat works
The Toy Story 2 incident is the universal editor trauma reference. Every editor knows someone whose drive failed. Nobody else demos a "hash-verified backup" UI; this is genuinely first-of-kind for the segment.

---

## Beat 4 — The pricing reveal (2:30–2:50)

### What you do
1. Cut to a clean slate (single visual: a price comparison table from the landing page).
2. Highlight a specific row: 10 TB, 5 editors.

### What you say
> "Everything you just saw runs against any S3-compatible bucket you own. Backblaze B2 at six dollars per terabyte. Cloudflare R2 with no egress fees. Wasabi. AWS. Or your own MinIO on a Mac mini in your closet. At ten terabytes for five editors, JuiceMount plus Backblaze costs sixty dollars a month. Suite Studios costs seven hundred and fifty. Frame.io Drive needs an Adobe Creative Cloud subscription on top of seventy-five dollars a month per user. JuiceMount is the same workflow — better, in some places — for six to twelve times less, with no vendor lock-in. The mount client is open source."

### What's on screen
- **The pricing table from `landing-copy.md` Section 3.** Specifically these rows highlighted in juice-orange:
  - JuiceMount Free + B2: **$60**
  - Suite Storage: **$750**
  - LucidLink: **~$1,000**

---

## Closing (2:50–3:00)

### What you do
- Cut to the JuiceMount logo (mount-arrow glyph) on a dark background.
- Tagline below: **"Your bytes are your bytes."**
- URL: **juicemount.io**

### What you say
> "Free download. MIT-licensed. Ships today. juicemount dot io."

### What's on screen
- Logo + tagline + URL
- Optional: a single line of follow-on text: *"Made by one person who got fed up."*

---

## Production notes

### Pacing
- Average beat = 40 seconds. Don't go over 50. If a beat needs more, it's two beats.
- One pause per beat is fine. Two pauses is too many. Keep moving.

### Music
- **No music.** Or if you must: a single sustained drone, very low, only under cold-open and closing. Music distracts from the point and dates the demo.

### Voice direction (if narrated by Leland)
- First-take energy. Not over-rehearsed. The "I made this" tone.
- Confidence without selling. Don't say "amazing" or "powerful." Say what it does and let the editor's brain do the math.
- One short pause after each wow moment. Let it land.

### Visual direction
- macOS dark mode throughout.
- Real Finder windows, real menu bar, real Cmd+Shift+F window. Nothing reskinned for the demo.
- Captions in JetBrains Mono, parchment-colored, lower-third position. Brief — never more than one line at a time.
- Avoid: zoom-ins on the cursor, glowing rings around clicks, "watch this!" animations.

### Format
- **Primary version: 1080p, 30fps, H.264, ~50 MB.** Embeddable in HN comments and on the marketing site.
- **Vertical 9:16 cut for social:** beats 1 and 3 only, ~45 seconds total. R3D Quick Look + the corruption-detected modal. The two highest-shock moments.
- **GIF version:** the spacebar-on-R3D moment alone, ~6 seconds, looped. For the Show HN body and Twitter post.

### What NOT to show
- Settings / preferences screens (the user can find those after they care).
- Architecture diagrams (they're in the landing page; the demo is for "this works" not "here's how").
- Comparison tables (already in beat 4 — don't dwell).
- Customer logos (we don't have them yet; faking it would feel desperate).
- Founder interview / face-to-camera (the voiceover is the founder's voice; that's enough).
- Any UI element that isn't shipping in v1 (the editor's bullshit detector is excellent).

### Variations

For different channels you cut down or expand specific beats:

| Channel | Cut/Expand | Length |
|---|---|---|
| Show HN body (animated GIF) | Beat 1 only, soundless | 0:06 |
| Twitter / X | Beats 1 + 3 | 0:45 |
| Landing page hero | Beats 1 + 2 | 1:20 |
| YouTube full demo | All 4 beats + a 30-sec install walkthrough at the end | 3:30 |
| NoFilmSchool / PVC embed | Full demo + 90-sec interview tail | 5:00 |
| Investor deck embed | Beats 1 + 4 (the wow + the price kill) | 1:00 |

---

## Backup plan if something fails on stage

- **R3D Quick Look fails to play:** have a pre-cached proxy already on disk. The point is "Quick Look works"; the audience won't know whether it generated live or was warm.
- **Search returns 0 results:** index a known-good test library before the demo. Don't search a fresh sync.
- **Backup verification panel is slow to load:** pre-run the verification before the demo so the manifest is hot.
- **Network drops mid-demo:** all three beats work entirely from local cache. JuiceMount's whole pitch is "works offline" — if the wifi dies on stage, lean into it. *"That's a feature."*

---

## What this demo is NOT

- A feature tour. We have ~30 features; we showed 3.
- A tutorial. Show the magic, not the buttons.
- A pitch. The pitch lives in `pitch-onepager.md`. The demo proves the pitch is real.
- An ad. No "limited time offer" energy. The product is permanent and free at the OSS tier; no urgency theater needed.

The demo's only job is: **the editor watching it thinks, "I want this on my Mac right now."** Everything else follows from there.
