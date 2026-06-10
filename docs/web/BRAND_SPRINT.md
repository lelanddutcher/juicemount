# JuiceMount brand + web sprint — operating doc

Mission: brand style, a publication README with graphics, and a deployable
juicemount.com static site with interactive widgets. Founder-requested
autonomous loop ("churn for several hours"), 2026-06-10.

## Operating rules (the loop follows these VERBATIM each wake)
1. Read this doc top to bottom. The Journal at the bottom is the state.
2. Pick the FIRST unchecked backlog item. Do it COMPLETELY (or split it
   and note the split here). Prefer finishing one item over starting two.
3. Sub-agents allowed for big builds (site pages, widgets); keep their
   briefs self-contained; verify their output yourself before commit.
4. Every completed item: verify (open/lint/render-test as applicable) →
   `git add` the relevant paths → commit locally with a focused message.
   NEVER push. NEVER touch nfs//bridge//health//app/ Go/Swift code in
   this sprint (the launch-hardening campaign is sealed at 8eb308c).
5. The LIVE app/mount are off-limits except read-only screenshots.
6. Quality bar: everything must look intentional — no lorem ipsum, no
   placeholder gray boxes, no broken links. Honest claims only (the
   Phase-4 fact-check standard applies to web copy too; reuse README
   numbers with their attribution).
7. Site must be fully static (no build step required to view; plain
   HTML/CSS/JS, ES modules fine), self-hosted fonts or system stack,
   no external CDNs, works file:// and on any static host.
8. After each item: append a Journal line (date, item, what, commit),
   update the checkbox, then ScheduleWakeup (20–40 min; pass the same
   loop prompt). Stop when the backlog is done or the founder says stop
   — then write the final sprint report as the last Journal entry and do
   NOT schedule another wake.

## Brand foundation (decided — build on this, don't re-litigate)
- Name: **JuiceMount** (one word, capital J + M). The mark: the citrus
  slice with the juicer arm (logos/color.svg).
- Palette ("the UI state language IS the brand"):
  - Juice Green `#AACD58` — primary brand + healthy state
  - Pulp Ink `#1B102A` — near-black ink, outlines, body text on light
  - Rind Cream `#FAFDE8` — light surface
  - Amber `#EF9F27` — degraded/warning accent
  - Stream Blue `#378ADD` — offline-files mode / informational accent
  - Fault Red `#E24B4A` — error/danger accent
  - Slate `#393839` — secondary dark
- Type: Inter (UI/body/display, self-host woff2 or system-ui fallback
  stack), JetBrains Mono for numbers/code/terminal. Tracking tight on
  display sizes. No more than two weights per page (400/600).
- Voice: direct, technically credible, video-editor-native (scrub,
  bins, proxies, conform, 10GbE, NVMe). Honesty is a brand value —
  every comparison names what competitors do better. No marketing fluff,
  no exclamation marks.
- One-liner: "The mounted-drive workflow editors get from LucidLink-class
  SaaS — at 10GbE-direct-attached speed, with Dropbox-style offline
  resilience — on hardware you already own."
- Inputs to reuse: docs/web/SITE_PLAN.md (IA + hero options + launch
  checklist), docs/web/INTERACTIVE_TOOL.md (calculator spec + formulas +
  pricing.json schema), README.md (verified claims + numbers),
  logos/state-*.svg (state-tinted marks), scripts/svg2png.swift
  (SVG→PNG pipeline, proven).

## Backlog
- [x] A. Brand style guide → docs/web/BRAND.md (palette w/ usage rules,
      type scale, logo usage/clearspace/don'ts, state-color semantics,
      voice examples good/bad, asset inventory)
- [ ] B. Site scaffold → site/: tokens.css (brand tokens, light+dark),
      base layout, index.html hero + nav + footer (per SITE_PLAN hero
      Option C: the scrub-3-seconds hook), responsive, no-JS-required
      baseline
- [ ] C. Rent-vs-own calculator → site/calculator.html + js (per
      INTERACTIVE_TOOL.md: payback months, 36-month simulation, honest
      "SaaS is cheaper for you" branch, shareable URL state,
      pricing.json dated 2026-06)
- [ ] D. "How it works" architecture diagram SVG (brand-styled: Mac app
      → NFS loopback → FUSE/JuiceFS → Redis+MinIO on your hardware;
      cache/spool/pin flows) → site/assets/ + embedded in README
- [ ] E. README v2 graphics + detail: hero banner SVG (logo + one-liner,
      GitHub light/dark variants), badges row, architecture diagram
      embed, expanded FAQ + troubleshooting section, screenshots section
      with docs/screenshots/CAPTURE.md script+instructions (founder runs
      capture; placeholders committed with exact filenames)
- [ ] F. Comparison page → site/compare.html (SaaS suites vs sync tools
      vs JuiceMount; the honest table from README, expanded; "what they
      do better" sections)
- [ ] G. Performance page → site/performance.html with SVG charts
      (author-measured numbers + methodology link; chart the cached-read
      MB/s, dir-open ms, search ms, offline fail-fast ms)
- [ ] H. Scrub-streaming explainer widget → site/ (interactive: a film-
      strip timeline; drag the playhead — "only these blocks stream";
      first-read vs cached-read toggle showing network vs NVMe speeds)
- [ ] I. OG/social card (1200×630) + favicon set from the mark via
      scripts/svg2png.swift → site/assets/, wired into page heads
- [ ] J. Deploy story → site/README.md (GitHub Pages + Cloudflare Pages
      steps, custom-domain DNS for juicemount.com, cache headers)
- [ ] K. Final pass: link check across site+README, lighthouse-style
      sanity (no console errors, images sized), brand consistency sweep,
      final sprint report in Journal
- [ ] L. (user-assisted, schedule last) Screenshots: founder runs
      docs/screenshots/CAPTURE.md steps; loop wires real PNGs into
      README/site and re-commits

## Journal
<!-- loop appends: [date time] item — summary — commit -->
- [2026-06-10 15:12] A — BRAND.md v1 (palette/type/mark/voice/assets) — this commit
