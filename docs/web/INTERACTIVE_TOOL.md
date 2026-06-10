# juicemount.com — Interactive Quantifier Spec

**Track:** Launch Plan row W. Companion: `SITE_PLAN.md` (§ 3 /calculator page).
**Constraint:** the site is static — everything here is client-side vanilla JS, one HTML page, no backend, no accounts, no tracking. Pricing constants live in a dated `pricing.json` so updates are a data commit, not a rewrite.
**Honesty rules carried over:** competitor prices cited with fetch dates (June 2026); JuiceMount performance figures only from the author's published measurements; the calculator must be able to output "self-hosting doesn't pay back for you" when that's the math.

---

## The three concepts

### Concept 1 — "Rent vs. Own" payback calculator  ⭐ recommended

**The question it answers:** *"My team pays $X/month for storage SaaS (or just got a quote). What does the same capability cost on my own hardware, and when does the hardware pay for itself?"*

**One-line formula:**
`payback_months = capex_one_time / (saas_monthly − selfhost_monthly)`

This is the founder's pick and the right centerpiece: it's the decision the target persona is actually sitting on, it produces a number they'll screenshot into a group chat, and every /compare page can deep-link into it pre-loaded with that competitor's pricing model.

#### Inputs

| Input | Control | Default | Notes |
|---|---|---|---|
| Editors (seats) `S` | stepper 1–15 | 3 | >15 → "you're enterprise; the math only gets better, but talk to the SaaS vendors' sales teams and compare" |
| Library size `T` (TB) | slider 1–100 | 10 | |
| Monthly growth `G` (TB/mo) | slider 0–10, step 0.5 | 1 | collapsed under "advanced" |
| Compare against | dropdown | Suite (managed) | LucidLink Business · Suite managed · Suite BYO · Shade Growth · **"My bill / my quote"** (free-form $/mo — covers Aspect, Iconik, LucidLink Enterprise, anything without public pricing) |
| Server hardware | dropdown w/ editable $ | "DIY / used build — $1,200" | options: "Already own a NAS — $0" · DIY $1,200 · "New NAS (TrueNAS Mini / Synology 8-bay class) — $2,500" · custom |
| Drive cost per usable TB | editable $ | $25/TB | tooltip: "new NAS drives after RAID-Z2/SHR-2 parity overhead; refurb enterprise runs lower" |
| 10 GbE upgrade | checkbox + $ | off, $300 | NIC + switch port; off = "your existing 1/2.5 GbE still beats a WAN" |
| Electricity | $/kWh + watts | $0.15, 120 W | collapsed under "advanced" |
| Offsite backup mirror | checkbox | **on**, B2 $6/TB/mo | on by default — self-hosting honestly includes paying for 3-2-1; leaving this on is what makes the comparison fair |

#### The math (run as a 36-month simulation loop, not closed-form — growth makes SaaS cost time-dependent)

Library at month *t*: `T(t) = T + G·t`

**SaaS monthly** `saas(t)`, per vendor model (all prices `pricing.json`, fetched 2026-06-10):

- **LucidLink Business:** `27·S + 8 · max(0, T(t)·1000 − 400·S) / 100`
  (=$27/user/mo annual + $8 per 100 GB beyond the included 400 GB/user — lucidlink.com/pricing. UI badge when `T(t) > 10`: "LucidLink labels Business 'best for up to 10 TB' — beyond this they push Enterprise/custom.")
- **Suite managed:** `75 · T(t) + 10 · max(0, S − 5)`  (suitestudios.io/pricing: $75/TB/mo, 5 users included, +$10/user after)
- **Suite BYO:** `40 · T(t) + 10 · max(0, S − 5) + 6 · T(t)`  (their $40/TB/mo mount fee + you still pay for a bucket — modeled at B2's $6/TB. Killer framing the UI should surface: *"$40/TB/mo is the software alone, on storage you already bought."*)
- **Shade Growth:** `29.75 · S` while `T(t) ≤ 0.5·S` TB (500 GB *active* per seat); beyond that show a warning instead of fake math: "your library exceeds Growth's active storage — Shade pushes Enterprise (custom pricing) here" (shade.inc/pricing)
- **My bill:** user's number, optionally `+ $·G·t` if they enter a per-TB rate.

**Self-host:**

- `capex = hardware + ceil(provisioned_TB) · drive_cost + (gbe ? 300 : 0)`
  where `provisioned_TB = (T + 12·G) · 1.25` (a year of growth + 25 % headroom — shown in the receipt, editable)
- `selfhost(t) = watts/1000 · 730 · kwh_rate  +  (backup ? 6 · T(t) : 0)`
  (default ≈ $13/mo power + $6/TB offsite)
- JuiceMount software line item: **$0**, displayed literally as a $0 row in the receipt — that's the point.

**Outputs:**

1. **Headline:** "Hardware pays for itself in **N months**." (`payback = capex / (avg36(saas) − avg36(selfhost))`; if denominator ≤ 0 → honest output: *"At this size, SaaS is cheaper for you. Self-hosting wins on ownership here, not dollars."*)
2. **Cumulative-cost chart, 36 months** — two lines (SaaS area vs. capex-step + opex slope), crossing at the payback point. Inline SVG, no chart library.
3. **The receipt** — itemized both columns, every assumption editable in place, with the shareable one-liner: *"$750/mo at Suite buys a 30 TB drive shelf — every month."* (`bill / drive_cost` TB-equivalent.)
4. **Share-state URL** — all inputs serialize to query params (`/calculator?vs=suite&s=5&tb=20`); this is the Reddit-thread mechanic and lets /compare pages deep-link pre-configured.
5. Footer disclaimer (always rendered): *"Competitor prices fetched June 2026 from public pricing pages (linked). Hardware estimates are editable defaults, not quotes. JuiceMount is free software; you supply and operate the server."*

#### Worked example (defaults: 5 seats, 10 TB, +1 TB/mo, vs. Suite managed)

- Suite month 0: `75·10 + 10·0 = $750/mo`, month 36: `75·46 = $3,450/mo`
- Capex: `$1,200 + ceil((10+12)·1.25=27.5→28)·$25 = $1,200 + $700 + $300 = $2,200`
- Self-host monthly: `$13 + 6·T(t)` → $73 at month 0
- Payback ≈ `2,200 / (750−73) ≈ 3.3 months` (and the gap widens monthly with growth) — the chart makes this visceral.

### Concept 2 — "First read vs. every read after" scrub illustrator

**The question it answers:** *"What does block-streaming actually mean for me, in seconds?"*

A draggable playhead over a filmstrip representing one big file. User picks: file preset (ProRes 422 HQ UHD ≈ 0.7 Gbit/s, ~110 GB/hr · R3D/ARRI OCF preset ≈ 2 Gbit/s <!-- VERIFY: settle exact preset bitrates with the founder; use camera-vendor data-rate tables -->), link speed (hotel 20 Mbit / WAN 200 Mbit / 1 GbE / 10 GbE), and scrubs N seconds.

Three result rows animate:

| | bytes moved | time before frame 1 |
|---|---|---|
| **JuiceMount, first read** | `bitrate × scrub_s` (rounded up to 4 MiB blocks) | `bytes / link_speed` |
| **JuiceMount, cached read** | 0 network | served at the measured 226–571 MB/s cached path — "NVMe, not network" |
| **Full-file sync tool** (Nextcloud-class) | whole file | `filesize / link_speed` — e.g. 100 GB at 1 GbE = **~13 minutes** before playback |

Formulas: `t_first = (rate_MBps · s) / link_MBps`, `t_sync = filesize / link_MBps`, blocks = `ceil(bytes / 4 MiB)`.
Worked default: scrub 3 s of a 100 GB R3D on 200 Mbit WAN → ~750 MB streamed, ~30 s; sync tool: 100 GB, ~67 min.

Cheap to build (one SVG + 6 numbers), and it's the *speed/resilience* half of the story where Concept 1 is the *economics* half.

### Concept 3 — "Your bill in drives" converter

Single input (monthly storage bill $) → two sentences: TB of NAS drives that money buys outright each month (`bill / 25`), and the B2-equivalent TB (`bill / 6`). Maximum shareability, near-zero build cost — but it's a punchline, not a decision tool, and its best line already lives in Concept 1's receipt.

---

## Recommendation

**Build Concept 1 as the /calculator centerpiece, with Concept 2 as a second tab on the same page** (they share the link-speed input and split the two halves of the site's thesis: own-the-economics + feel-the-speed). Fold Concept 3's one-liner into Concept 1's results headline rather than shipping it separately.

Build order: Concept 1 logic + receipt (a day) → share-URL state (hours) → SVG chart (a day) → Concept 2 tab (a day). All vanilla JS; total payload target < 50 KB, no dependencies.

```
pricing.json (committed, dated) — single source for both tabs:
{
  "fetched": "2026-06-10",
  "sources": { "lucidlink": "https://www.lucidlink.com/pricing", "suite": "https://www.suitestudios.io/pricing",
               "shade": "https://shade.inc/pricing", "b2": "https://www.backblaze.com/cloud-storage/pricing" },
  "lucidlink_business": { "seat": 27, "included_gb_per_seat": 400, "extra_per_100gb": 8, "soft_cap_tb": 10 },
  "suite_managed":      { "per_tb": 75, "included_seats": 5, "extra_seat": 10 },
  "suite_byo":          { "per_tb": 40, "included_seats": 5, "extra_seat": 10 },
  "shade_growth":       { "seat": 29.75, "active_gb_per_seat": 500, "max_seats": 15 },
  "object_storage":     { "b2_per_tb": 6, "wasabi_per_tb": 6.99, "r2_per_tb": 15 },
  "defaults":           { "drive_per_usable_tb": 25, "diy_server": 1200, "new_nas": 2500,
                          "tengig_addon": 300, "watts": 120, "kwh": 0.15 }
}
```

**Maintenance contract:** the calculator renders `pricing.json.fetched` next to every result; a quarterly 10-minute re-check of four pricing pages keeps it honest. If a price can't be re-verified, the vendor drops to the "My bill / my quote" path rather than showing stale numbers.

**Pre-launch QA for the tool:** verify each vendor formula against its pricing page once more; confirm the "SaaS is cheaper for you" branch triggers correctly at small sizes (e.g. 1 seat / 1 TB vs. LucidLink Starter); have one outside person sanity-check the worked examples; screenshot the defaults for the OG card.
