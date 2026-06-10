# JuiceMount brand guide

v1 — 2026-06-10. The working reference for the app, README, site, and
social assets. Short on theory, long on rules you can apply.

## The idea

JuiceMount's brand is the product's honesty made visible. The same four
colors that tell an editor whether their mount is healthy are the brand
palette; the same plain-spoken voice that refuses to oversell in the
comparison table writes the homepage. Nothing decorative that the product
wouldn't say itself.

## Name

- **JuiceMount** — one word, capital J and M. Never "Juice Mount",
  "juicemount" (except in URLs/handles), or "JM" in public copy.
- The domain is juicemount.com; the module/repo is
  `github.com/lelanddutcher/juicemount`.
- Pronunciation/metaphor: JuiceFS underneath; the mount is the product.
  "Juice" carries the citrus identity; never extend the pun in copy
  ("squeeze", "fresh-squeezed", "pulp fiction") — the mark does the
  playfulness so the words don't have to.

## The mark

`logos/color.svg` — a citrus slice with a juicer arm, drawn with a heavy
`#1B102A` outline, cream flesh, green segments.

- **Primary**: full-color mark on light (Rind Cream or white) or dark
  (Pulp Ink) backgrounds. The outline carries it on both.
- **Mono**: `logos/black.svg` on light, `logos/white.svg` on dark — for
  single-color contexts (favicons at tiny sizes, engraving-style uses).
- **State variants** (`logos/state-*.svg`) are FUNCTIONAL assets for the
  app's menu bar, not decoration. On the web they appear only when
  explaining the state system itself.
- **Clearspace**: keep a margin of one segment-width (≈12% of the mark's
  width) free on all sides.
- **Minimum sizes**: 18 px (menu bar, proven legible), 32 px favicon,
  64 px anywhere in marketing.
- **Don'ts**: don't recolor outside the approved palette, don't rotate,
  don't drop the outline, don't put the green mark on the green brand
  color, don't add drop shadows or gradients to it.

## Palette

The state language is the brand language. One green, one ink, one cream,
three semantic accents.

| Token | Hex | Role |
|---|---|---|
| Juice Green | `#AACD58` | Primary brand. Healthy state. CTAs on dark. Never body text. |
| Pulp Ink | `#1B102A` | Near-black. Body text on light, dark surfaces, the mark's outline. |
| Rind Cream | `#FAFDE8` | Warm light surface. Page backgrounds on light; text on Pulp Ink. |
| Amber | `#EF9F27` | Degraded/warning. Sparingly: badges, callouts, charts. |
| Stream Blue | `#378ADD` | Offline-files mode; informational accents, links on light. |
| Fault Red | `#E24B4A` | Errors/danger only. Never decorative. |
| Slate | `#393839` | Secondary dark: muted text on light, borders on dark. |

Derived tints (for surfaces, computed not hand-picked): mix the base
color with the page background at 8–12% for fills, 25–35% for borders.

Accessibility floors: body text is Pulp Ink on Cream/white (≥12:1) or
Cream on Pulp Ink. Juice Green on white fails contrast for text — use it
for fills, marks, and large display type only (≥32 px, 600 weight), with
Pulp Ink text on top of green fills.

Dark mode: Pulp Ink page, Cream text, Juice Green accents — the mark's
outline already separates it from the background.

## Typography

- **Inter** — everything: display, UI, body. Self-host woff2 (400, 600).
  Fallback stack: `Inter, -apple-system, "SF Pro Text", system-ui,
  sans-serif`.
- **JetBrains Mono** — numbers that matter (throughput figures, latency
  tables, prices in the calculator), terminal/code blocks. Fallback:
  `"JetBrains Mono", ui-monospace, "SF Mono", Menlo, monospace`.
- Two weights per page maximum: 400 (text) and 600 (headings/emphasis).
- Display sizes: tracking −1% to −2%; sentence case ALWAYS — headings,
  buttons, nav. Never Title Case, never all-caps (small-caps labels at
  11–12 px with +6% tracking are the one exception).
- Scale (web): 15/17 px body, 20 px lead, 28 px h3, 40 px h2, 56–72 px
  hero. Line-height 1.6 body, 1.1 display.

## Voice

Direct, technically credible, video-editor-native. Write like the
engineer who built it explaining to a colleague at a render bay.

Rules:
1. Claims carry their evidence ("author-measured on a 10GbE LAN,
   methodology linked") or they don't ship.
2. Name what competitors do better. Honesty is the differentiator;
   hedging reads as marketing, specificity reads as truth.
3. Editor vocabulary, not infra vocabulary: scrub, bins, proxies,
   conform, offload — not "leverage object storage primitives."
4. No exclamation marks. No "simply"/"just"/"blazingly". No citrus puns
   in copy.
5. Numbers in mono, with units, rounded honestly (7 Gbit/s, not
   "7.21984").

Calibration examples:
- ✗ "Blazingly fast cloud-native storage for creators!"
- ✓ "Scrub a 100 GB file three seconds in; only those blocks stream."
- ✗ "Enterprise-grade reliability you can trust."
- ✓ "kill −9 mid-write, relaunch, and the metadata store reopens clean —
  that's a test we run, not a promise."
- ✗ "Say goodbye to expensive cloud storage!"
- ✓ "Suite charges $40/TB/mo to mount storage you already own. This is
  the part we couldn't accept."

## Iconography & illustration

- UI icons: SF Symbols inside the app (platform-native); on the web,
  simple 1.5 px-stroke line icons in Pulp Ink/Cream — no filled blobs,
  no emoji in product surfaces.
- Diagrams: flat, outlined (2 px Pulp Ink strokes echoing the mark),
  brand-palette fills at tints, JetBrains Mono labels. No 3D, no
  isometric, no gradients except the mark's own juicer-arm gray.
- Motion (web): restrained; one purposeful interaction per page (the
  calculator, the scrub explainer). No parallax. 200 ms ease-out.

## Asset inventory

| Asset | Path | Use |
|---|---|---|
| Color mark | `logos/color.svg` (+`.png`) | Primary everywhere |
| Mono marks | `logos/black.svg`, `logos/white.svg` | Single-color contexts |
| State marks | `logos/state-{healthy,degraded,offline-files,fault}.svg` | App menu bar; state-system docs |
| App icon | built from color.svg by `scripts/build-app.sh` | macOS |
| SVG→PNG pipeline | `scripts/svg2png.swift` | Any raster export (proven 18→1024 px) |
| Site tokens | `site/assets/tokens.css` | Single source for web colors/type |

## Applying it (quick reference)

- README/GitHub: light-first; hero banner uses the mark + one-liner with
  a dark variant via `prefers-color-scheme` picture sources.
- Site: Cream/Ink theme pair, Juice Green CTAs on Ink, Stream Blue
  links, mono numbers. The four state colors appear together ONLY in the
  state-system explainer and the menu-bar screenshots.
- Charts: bars/lines in Juice Green on neutral grids; comparisons may
  use Stream Blue vs Slate (never green-vs-red competitor framing).
