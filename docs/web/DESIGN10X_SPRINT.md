# Design 10x sprint — founder redirect, 2026-06-11 overnight

Mission: 10x the site's quality by morning. The v1 site reads like "a
designed markdown file" / "somebody's passion project diatribe". Rebuild
it as something an editor, filmmaker, or post-production person
IDENTIFIES WITH in five seconds and finds visually convincing.

## The founder's direction (verbatim intent — this overrides v1 choices)
1. IDENTITY FIRST, pain second. The homepage must say, near-literally:
   this is **the open-source alternative to Suite / Shade / Iconik /
   LucidLink** — a revolutionary, cost-saving workflow for indie
   filmmakers and small post teams. "Scrub 3 seconds of 100GB" is a
   supporting proof, NOT the hero hook.
2. WHAT IT'S NOT, honestly: not (yet) a team-wide multi-department
   collaboration/file-sharing platform. It's an indie filmmaker /
   creative tool: raw performance of the enterprise SaaS tools, none of
   the cost, your own hardware to tinker with.
3. ANIMATIONS THAT ILLUSTRATE. At least TWO animated + interactive +
   explanatory visuals PER PAGE. Approved concepts:
   - HOME (a): a pseudo-Finder window — a drive MOUNTS (volume appears
     in sidebar), files populate; copy lands on: a real mount means
     paths don't change — teammates never relink media in
     Premiere/Resolve. Works like Dropbox, on your hardware, but it's a
     MOUNT, unlike Dropbox.
   - HOME (b): a macOS menu-bar mock using the real state marks
     (logos/state-*.svg): the citrus icon cycling/clickable through
     healthy / uploads-pending (badge) / offline-pinned / degraded with
     a small dropdown showing pending uploads, cached GB, pinned items.
   - HOME (c): the chunk story — a file is uploaded, visibly SPLIT into
     4 MB chunks flowing to the server; then chunks read back
     INDEPENDENTLY (random access pulls just 3 chunks). Below it, the
     sync-vs-block-storage explainer (whole-file sync tools vs block
     streaming).
   - PERFORMANCE: the scrub/playhead widget MOVES HERE from home (the
     founder likes it: "drag the playhead and watch what moves is kind
     of cool") — reorganized: show local-vs-server sides with tiny
     dashed transfer lines as blocks page in; future-ready note: founder
     will supply real video assets so the strip becomes real frames —
     structure the markup so a real <video> + frame strip can drop in.
     Plus a second visual (e.g. animated throughput race or cache
     warm-up).
   - COMPARE: be MORE EXPLICIT with costs — pricing front and center,
     deliberate (e.g. an animated annual-cost meter per vendor at a
     chosen team size; the "what you'd pay" strip). Plus a second
     visual (e.g. lock-in/exit visual or seat-cost stepper).
   - CALCULATOR: the receipts section gets a PRINTED RECEIPT look
     (paper, mono, perforated edge, subtle) — "that could be way
     cooler". Chart can animate its draw-in. Payback math is CORRECT
     (founder verified) — improve the explanation clarity around the
     receipts, don't change formulas.
4. KILL the purple read: the flat #1B102A bands read as "purple
   gradient" and the founder dislikes it. Site dark surfaces move to a
   neutral cinema-dark (e.g. #141416–#191A1C range, pick once, token
   it). The MARK's outline stays #1B102A (it's the logo). No gradients
   on bands at all. Update tokens.css; BRAND.md gets an addendum noting
   the site-surface override (do not rewrite the whole brand doc).
5. MORE COHESIVE + CONVINCING overall: motion with purpose everywhere
   (still honoring reduced-motion), consistent component quality,
   nothing that reads as a markdown page with CSS.

## Iteration protocol (founder-specified — DO NOT overwrite failures)
- Every iteration of a page is COMMITTED as its own reviewable file:
  site/iterations/<page>-v<N>.html (v2, v3, …; v1 = the current
  canonical pages at sprint start). Iterations may share ../assets/ but
  must be openable standalone via file://.
- Each iteration gets a self-QA verdict in the Journal: PROMOTE /
  KEEP-FOR-REVIEW / SUPERSEDED, judged against the VALUE GATE below.
- When an iteration clearly beats canonical, COPY it over the canonical
  page (canonical always = current best) — the vN file stays for the
  founder's morning review. Never delete an iteration.
- Naming: home-v2.html, home-v3.html, performance-v2.html,
  compare-v2.html, calculator-v2.html, …

## Value gate (every iteration is judged against this, in the Journal)
1. Five-second test: does an editor know WHAT THIS IS (open-source
   Suite/Shade alternative) and WHO IT'S FOR (indie film/post) above
   the fold?
2. Are there ≥2 interactive animated explanatory visuals that TEACH
   (mount-no-relink, chunking, state system, cost)?
3. Does it look designed (intentional motion, rhythm, hierarchy) — not
   a styled document?
4. Honest claims only (frozen README numbers + attributions; the
   Phase-4 standard). New copy may anchor the category ("open-source
   alternative to X") — that's positioning, not a feature claim.
5. Static, no CDNs, file://-safe, light+dark, 360px, reduced-motion,
   keyboard accessible.

## Constraints carried forward
- Tech rules from BRAND_SPRINT.md §7 (static, no build step, no
  external resources). Sub-agents allowed; verify before commit; commit
  locally; NEVER push. Don't touch app/Go/Swift code. logos/ are
  read-only inputs.
- Voice rules from BRAND.md still apply to COPY (no hype words, no
  exclamation marks, honest comparisons) — but messaging is now
  identity-first per the founder; "open-source alternative to
  Suite/Shade/Iconik" is the sanctioned frame.
- Animation tech: CSS keyframes + rAF JS + inline SVG; no libraries.
  200ms-400ms purposeful moves; longer narrative loops OK for the
  explainer animations (with pause-on-reduced-motion + replay buttons).

## Backlog (loop: first unchecked item each wake; split as needed)
- [x] R0. tokens.css dark-surface retoken (kill the purple read) +
      BRAND.md addendum + verify all four canonical pages still pass
      contrast/structure gates with the new surface.
- [x] R1. home-v2: new identity-first hero (open-source alternative to
      Suite/Shade/Iconik; for indie filmmakers + small post teams;
      cost story up front) + pseudo-Finder mount animation (HOME a) +
      menu-bar state mock (HOME b). Keep page structure coherent;
      scrub widget REMOVED from home (moves to performance in R3).
- [x] R2. home-v3: add the chunk-upload/read-back animation (HOME c) +
      sync-vs-block explainer section + what-it's-not section; refine
      v2 weaknesses noted in Journal. Promote best to index.html.
- [x] R3. performance-v2: scrub widget transplanted + reorganized with
      local/server split + dashed paging lines + video-asset-ready
      markup; second visual; promote if better.
- [x] R4. compare-v2: explicit-cost animated visual(s) (annual-cost
      meter at team-size slider), pricing more deliberate, what-it's-
      not woven in; promote if better.
- [x] R5. calculator-v2: printed-receipt restyle + chart draw-in +
      clarity pass on the payback explanation; promote if better.
- [x] R6. Cross-page cohesion pass: shared motion language, nav, OG
      descriptions match new messaging; update og:title/descriptions;
      re-run all v1 gates (links/anchors/voice/structure) on canonical.
- [ ] R7+. Further iteration rounds as quality demands (vN+1 per page,
      Journal-driven): keep iterating until the value gate scores
      strong on every page or the founder wakes up. Aim: every page has
      had ≥2 iterations reviewed against the gate.

## Journal
<!-- [date time] item — what changed — verdict vs value gate — commit -->
- [2026-06-11 02:05] R0 — cinema-dark #17181B replaces ink on all dark surfaces (tokens, band-ink, og-card re-rendered); BRAND.md addendum (surface override + identity-first messaging + show-dont-tell mandate) — committed
- [2026-06-11 02:40] R1 — site/iterations/home-v2.html (1210 ln, self-contained): identity hero ("The open-source alternative to Suite, Shade, and LucidLink." / indie-film kicker / $0-seat lead), Finder mount narrative (scroll/replay, You-vs-teammate toggle, path-holds-still lesson), menu-bar state tablist w/ real state marks + auto-cycle + popover mocks, what-its-not block, scrub removed. Agent gates all PASS incl. live browser light/dark/360; orchestrator re-verified structure/paths/voice/cinema-dark. Verdict: KEEP-FOR-REVIEW, PROMOTE-candidate pending R2 comparison (chunk animation still to add).
- [2026-06-11 03:20] R2 — site/iterations/home-v3.html (1766 ln) = v2 + chunk story (HOME c): A003_C014_0212TP.braw · 8 GB splits into chunk squares (24 stand in for 2,000 × 4 MB — labeled) flowing to a "your NAS" rack; phase two reads three seconds back — exactly 3 chunks return, counters "8 GB — once" / "12 MB, not 8 GB"; Upload/Read-back step buttons + Replay + IO autoplay-once; markup ships the END state so no-JS/reduced-motion get a static diagram with working step buttons; sync-vs-block two-column explainer underneath (whole-file class vs open-chunk-format + exit story). R1's noted refinement applied: pillars onto the ink band with kicker/stat/rule lifted cards (strict band alternation ink→light×3 pairs→ink), arch figure in a plate frame w/ legend chips, what-its-not as not/is card pair; demo 01/02/03 kickers tie the course together. State marks vendored to site/assets/state-*.svg (byte-identical copies) so canonical resolves from the site/ web root when deployed. Gates: tag balance, refs (16/16 from each root), zero external resources, voice, cinema-dark (#1B102A = 0), claims byte-match incl. chunk math, node --check, live browser light+dark+360+1280 (story timeline, step takeover, console clean). Value gate vs old canonical: identity 5-s test WIN, 3 teaching animations vs 1 WIN, designed-not-styled WIN, honesty PASS=, tech PASS=. Verdict: PROMOTE — copied over site/index.html with path rewrites (../assets→assets, state marks via assets/), canonical link + og:url/og:image:alt restored, og:title/description moved to the identity messaging (og:image/url unchanged), iteration badge dropped; scrub.js absent from canonical home. home-v2/compare/performance/calculator untouched. Files left uncommitted for orchestrator review per R2 handoff.
- [2026-06-11 03:25] R2 — home-v3 (1766 ln) + PROMOTED to index.html (3 wins/2 ties on the gate): chunk story demo 03 (8GB braw → 24 stand-in chunks → 3 fly back, "12 MB, not 8 GB"), sync-vs-block explainer, ink/light band rhythm with lifted pillars + framed arch plate + not/is card pair; canonical got path rewrites, vendored state marks (production-correct deviation), new og identity copy. Orchestrator re-verified both files: structure/refs/scrub-absence/v2-untouched all clean.
- [2026-06-11 (R3)] R3 — site/iterations/performance-v2.html (1721 ln) = the performance page rebuilt around two demos. DEMO 01: the scrub widget transplanted from the retired home #scrub and REORGANIZED per the founder — one forced-dark screening panel split into LOCAL · YOUR MAC (16-frame film strip w/ playhead + an 80-cell SSD-cache row) and SERVER · YOUR NAS (80-cell block store, always full — your NAS keeps its copy); all rows share one gutter grid so columns align, and when the playhead touches an un-cached block a tiny dashed Stream-Blue transfer line + riding packet chip animates UP the wire zone from the server block to its cache cell, which fills Juice Green and STAYS (cache persists across mode flips, as before). Frames under-line green once all 5 of their blocks are cached ("plays from SSD"); sync-tool mode keeps the everything-must-move contrast as a single crawling frontier line + sequential blue fill at the labeled 100× compression. All v1 honesty kept: illustrative truth line (80 cells ≈ 25,000 × ~4 MB blocks, line-rate waits), 226–571 MB/s cached attribution, link selector repricing, ARIA slider + arrows/PageUp/Home/End, reduced-motion = instant settles, noscript hand-worked math. VIDEO-READY: .pv-frame cells carry data-frame 0–15, a commented <video>/<canvas> monitor slot + drop-in doc sits in the markup, and the script dispatches a "pv:frac" CustomEvent per move. DEMO 02: the warm-cache race — the same 200 MiB read three ways (cold at link line rate / warm at the author-measured 431 MB/s fully-cached figure / a sync tool that must land all 100 GB first), three lanes on one compressed wire-time clock (compression printed, ≈114× at 1 GbE), mono timers freeze at honest finishes (1.7 s / 487 ms / 13 m 20 s at 1 GbE), link selector two-way-synced with demo 01, autoplay-once on scroll + Run again, reduced-motion shows finish clocks; at 10 GbE the verdict admits cold-at-line-rate would outrun the measured cache figure — honest physics. Rest of page lifted to home-v3 chrome: statline hero (5 frozen stats), measured 01–03 charts in plate frames w/ legend chips, table on ink band, methodology/no-claim kept, closing ink band; strict ink/light zone rhythm. Gates: parser battery PASS both files, refs resolve from both roots, zero external resource loads, node --check PASS, voice sweep PASS, #1B102A = 0, 12 README claims byte-matched, live browser (localhost:8091) light+dark+1280+360 incl. drag/keyboard/mode-flip/link-sync/race-finish, console clean, home-v2/v3 + index/compare/calculator untouched. Value gate vs old canonical: five-second test WIN (claim + drivable proof above the fold), 2 teaching interactives vs 0 WIN, designed-not-styled WIN, honesty PASS=, tech PASS=. Verdict: PROMOTE — copied over site/performance.html (1710 ln) with path rewrites, canonical/og:url restored, og:description + og:image:alt moved to the improved story (cinema-dark wording), aria-current restored, badge dropped. scrub.js deleted + dead .scrub-* block removed from site.css (675→496 ln; grep-verified zero references). Files left uncommitted for orchestrator review.
- [2026-06-11 04:10] R3 — performance-v2 (1721 ln) + PROMOTED (3 wins/2 ties): playhead rebuilt as LOCAL/SERVER split with per-block dashed Stream-Blue transfer lines + persistent SSD cache row (founder mechanic verbatim), video-asset-ready markup (data-frame cells + commented video/canvas slot + pv:frac event), warm-cache race lanes (cold/warm/sync, honest 10GbE physics note); scrub.js deleted (0 refs), dead .scrub-* css trimmed; orchestrator verified both files + comment-only video refs + home iterations untouched. Verdict: PROMOTE.
- [2026-06-11 (R4)] R4 — site/iterations/compare-v2.html: the compare page rebuilt pricing-first. Hero tightened to the identity claim ("Same workflow. Different bill.") with the what-it's-not line woven once as the qualifier. DEMO 01 (#meter): the year-one cost meter — seats stepper 1–10 + 5/10/25 TB presets; per-vendor bars grow/settle with mono counters (rAF, ease-out, 90 ms stagger, autoplay-once on scroll, replumbs on every control change); Suite managed/BYO, LucidLink (with past-"best up to 10 TB" note when tb>10), Shade (honest "Enterprise quote" hatched state when the library passes Growth's 500 GB-active/seat cap — which is most states; it only prices at 10 seats × 5 TB), and JuiceMount as a split bar: solid green one-time capex + hatched green year-one power+mirror, with the capex-then-flatline framing ("after year one it runs on at $73.14/mo"). EVERY figure derived live by assets/calculator.js via a new window.JMCalc export of the existing pure functions (initDom gains an if(!form)return guard; git-diff proof: libraryAt/saasRate/selfRate/capexOf/simulate + FALLBACK_PRICING/DEFAULTS byte-identical to HEAD). Markup ships the default-state end figures for no-JS, byte-checked against simulate() ($9,000/$5,520/$9,300/Enterprise-quote/$2,703 = $1,825 + $878). Deep links per row + CTA carry vs/s/tb/g=0 through the calculator's own URL params (verified live: calculator reproduces month-3 payback at g=0). DEMO 02 (#exit): the leave-whenever toggle — JuiceMount scene: 16 chunk copies fill "your next setup" while the bucket grid never changes, badges "the bucket stays under your control" + stock-juicefs-client truth, README FAQ quoted verbatim; SaaS scene: a one-queue export gate with "export required — before you cancel" amber chip, 10-of-10 illustrative counter, category-level hedge (no invented egress numbers). Both ship end states (no-JS/reduced-motion safe), replay buttons, IO autoplay-once. Lanes carried VERBATIM (tables, pricing dl, both concessions, pick-list) and lifted: plate frames with sr-only captions, lane kickers, three-way-trade pillar cards, strict ink/light rhythm (hero ink → meter light → exit ink → map+lanes light → closing ink). Gates: parser battery PASS, 27/27 refs resolve from both roots, 0 external resource loads, node --check PASS (page + calculator.js), voice PASS, #1B102A = 0, claims/prices byte-match README + pricing.json (incl. the exit-story quotes), static meter figures = simulate() output, 360px zero overflow, live browser light+dark+1280+360 (stepper→10 seats/5 TB recompute $5,100/$3,360/$4,200/$3,570/$2,193 exact; both exit scenes; console clean). Value gate vs old canonical: five-second test WIN, 2 teaching interactives vs 0 WIN, designed-not-styled WIN, honesty PASS=, tech PASS=. Verdict: PROMOTE — copied over site/compare.html with path rewrites (../assets→assets, deep links incl. JS strings), canonical/og:url restored, og:image:alt moved to cinema-dark wording, meta/og:description moved to the pricing-first story, aria-current restored, badge dropped (+ dead badge CSS removed from canonical). index/performance/calculator + home/performance iterations untouched. Files left uncommitted for review.
- [2026-06-11 (R5)] R5 — site/iterations/calculator-v2.html: the calculator restyled around the founder's two notes, MATH UNTOUCHED. RECEIPTS: printed-register look — calculator.js renders the identical two <table><caption><tbody> receipts (renderReceipt unchanged); the v2 CSS makes each table a paper card that stays paper-WHITE in both themes (a receipt is paper; ink is literal #1B102A in its BRAND.md text-on-light role), JetBrains Mono throughout, CSS-only perforated tear teeth top+bottom (::before/::after dash strips, no images), register header printed by caption::before ("JUICEMOUNT — RENT VS OWN") and caption::after ("prices checked June 2026 · fetched 2026-06-10"), dashed item rules, right-aligned amounts, 3px-double rules over totals (36-month framing prominent, h2 retitled "The receipts — 36-month totals"), the literal "$0 JuiceMount software / this row is the point" line untouched, faint barcode rule block painted by a bottom background gradient (decorative-honest). CHART: draw-in once on scroll-into-view — page script measures each polyline with getTotalLength (pathLength on polylines is flaky cross-engine) and animates stroke-dashoffset len→0 over 700 ms, SaaS area fades up at 500 ms, payback dot pops at 800 ms, label/guide fade last; re-renders after the first draw paint instantly; mid-draw re-render settles to final; reduced-motion and no-IO browsers never arm. CLARITY: calculator.js update() gains a jm:sim CustomEvent rendering hook (no math out there); the page listens and keeps a one-line explainer between chart and receipts — "Receipts show 36-month totals. Payback is a different number: the month the green self-host line crosses below the blue SaaS line — month N for this setup", with honest variants for payback_beyond / saas_cheaper ("the lines don't cross: SaaS stays cheaper") / Shade's tier-cap states; the crossing dot gains a dashed drop-guide to the month axis (only when it sits clear of it) and a hover <title> ("cumulative self-host drops below cumulative SaaS"). NO formula/default change: git-diff proof libraryAt/saasRate/selfRate/capexOf/simulate + FALLBACK_PRICING/DEFAULTS byte-identical to HEAD; form + noscript blocks byte-identical to v1 through promotion. Gates: parser battery PASS both files, 19/19 refs from both roots, 0 external resource loads, node --check PASS (page inline + calculator.js), voice PASS, #1B102A only as receipt ink (text-on-light role), pricing strings byte-match pricing.json + receipt date line, 360px zero overflow, live browser light+dark+1280+360 (explainer live at month 3/8, all four status variants exercised, guide on late paybacks, paper stays white on dark, console clean), file://-safe by construction (parse-time inline script, fetch fallback). Value gate vs old canonical: five-second TIE, teaching motion WIN (draw-in + explicit crossing vs static chart), designed-not-styled WIN (printed receipts vs plain tables), honesty PASS= (dates printed on the receipts), tech PASS=. Verdict: PROMOTE — copied over site/calculator.html with path rewrites, canonical/og:url restored, stale "Pulp Ink card" og:image:alt moved to the cinema-dark wording (now consistent on all four pages), aria-current restored, badge + dead badge CSS dropped. index/performance + all prior iterations untouched. Files left uncommitted for review.
- [2026-06-11 05:20] R4+R5 — compare-v2 (1320 ln) + calculator-v2 (584 ln), BOTH PROMOTED: year-one cost meter (seats stepper + TB presets, capex-vs-recurring split bar, honest Shade-quote/LucidLink-cap states, deep-links carry state), exit-story toggle (bucket-never-changes vs export-gate, README-verbatim quotes); printed receipts (paper-white in dark, perforations, register header, barcode rule), chart draw-in + labeled payback crossing + explainer line w/ jm:sim event. calculator.js: 3 hooks only — formula freeze verified by git diff (orchestrator note: two of MY freeze checks false-alarmed — hash() randomization, then a regex that swallowed the file tail; diff is ground truth). All gates green; prior pages/iterations untouched.
- [2026-06-11 05:40] R6 — cohesion battery: 275 refs/anchors 0 broken across 4 canonical + 5 iterations + README; og titles/alt identity-aligned on all pages; voice clean; ink only in the 2 sanctioned receipt-text literals; deploy guide carries no stale refs. PASS.
