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
- [ ] R3. performance-v2: scrub widget transplanted + reorganized with
      local/server split + dashed paging lines + video-asset-ready
      markup; second visual; promote if better.
- [ ] R4. compare-v2: explicit-cost animated visual(s) (annual-cost
      meter at team-size slider), pricing more deliberate, what-it's-
      not woven in; promote if better.
- [ ] R5. calculator-v2: printed-receipt restyle + chart draw-in +
      clarity pass on the payback explanation; promote if better.
- [ ] R6. Cross-page cohesion pass: shared motion language, nav, OG
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
