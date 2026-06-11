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
- [x] R7+. Further iteration rounds as quality demands (vN+1 per page,
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
- [2026-06-11 07:25] R7 — fresh-eyes design crit + fix round, all four pages re-promoted.
  A no-context reviewer agent rendered all four canonical pages (served, 1280+360,
  light+dark emulated, every widget driven, consoles clean) against the value gate
  and the founder directives. Scorecard: zero WEAK cells anywhere; index strong
  across G1–G4, the others strong with adequate cells traced to one P1 and layout
  nits. Punch list: 12 items. Dispositions:
  P1 fonts-404 (tokens.css url()'d woff2s that don't exist; Inter never rendered,
    3 requests/page) — FIXED: @font-face now local()-only with commented url()
    siblings + exact drop-in paths; site/README.md documents the self-host upgrade
    (OFL sources). Verified: zero font requests, zero failed resources.
  P2 CTA cold-jump — FIXED: both index CTAs relabeled "Install from GitHub" + a
    what-you'll-need line (macOS 14+, any Docker box, one docker compose up — all
    README-sourced, no invented time claim) ahead of the closing CTA.
  P2 "/yr 1" unit glitch — FIXED: unit spans removed (header already carries
    ANNUAL COST · YEAR ONE).
  P2 mid-tween wrong numbers — FIXED: paint() takes textVals; printed figures snap
    to settled values, only bars animate. Browser-verified: $9,600 shown while bar
    still mid-flight.
  P2 dead right gutter (3 measured plates) — FIXED: at ≥980px each figure becomes
    a 2-col grid, figcaption fills the former gutter as a bordered side note
    (40rem chart cap kept for SVG type). Verified cap col x=773 w=374; collapses
    to block <980.
  P3 LucidLink bar drops as seats rise — FIXED: lucidYr1(s) vs s−1 comparison
    appends "an added seat lowers the bill… their math, not ours" to the basis
    line. Node-checked against real pricing: the drop is real at EVERY seat step
    2–10 for 5/10/25 TB (seat $324/yr < $384/yr overage relief), so the note
    shows whenever seats ≥ 2 — correct, since the bar move reads as a bug every
    time.
  P3 "10 GbE speed" hero overclaim — FIXED: "LAN speed" in hero + both meta
    descriptions (measured 7 Gbit/s stands in the stat strip).
  P3 table scroll affordance at 360 — FIXED: mono micro-hint (≤760px) + both
    .table-wrap now tabindex=0 role=region (keyboard-scrollable). Hint verified
    display:none at desktop.
  P3 "magic" in competitor lane copy — FIXED (cut).
  P3 replay beats too quick — FIXED where real: the read-back leg (the punchline)
    was ~1.5s; now ~2.2s with three separated arrivals (sampled live: chips in
    flight at 1.3s, done ~2.4s). Mount full-play was already ~4s of staged beats
    (crit impression didn't match the timer math); only its fast toggle path was
    eased 0.9→1.9s.
  P3 aria-valuetext missing on scrub slider — FALSE POSITIVE, no change: markup
    ships it at init and renderPlayhead writes it on every update (verified live:
    "0 GB into the clip — 0 of 80 blocks paged…"). Noted for honesty.
  P3 drives receipt note wrong at g=0 — FIXED in shared calculator.js (display
    copy only; git diff confirms formulas untouched beyond the 3 sanctioned
    hooks + this string). Deep-link ?g=0 verified showing "today's X TB × 1.25
    headroom, growth set to 0".
  Iterations home-v4 / performance-v3 / compare-v3 / calculator-v3 created per
  protocol (never overwrote v2/v3), all promoted to canonical after: node --check
  on every inline script, JMCalc node simulation, live browser verification of
  the riskiest changes (one stale-cache false alarm on the canonical re-check —
  cache-busted, grid confirmed), and the full battery re-run: 388 refs across 13
  files, 0 broken; voice clean (Blackmagic RAW is a file-kind label, not a voice
  hit); ink literals only the calculator's 2 sanctioned paper-text uses.

## CLOSING REPORT (design-10x sprint — backlog complete)
Every page now leads with identity (the open-source alternative to Suite, Shade,
and LucidLink, for indie filmmakers and small post teams), carries ≥2 teaching
interactives on cinema-dark surfaces, and survived a fresh-eyes crit with zero
weak gate cells. Iteration ledger: home v2→v3→v4 (3 reviewed), performance
v2→v3, compare v2→v3, calculator v2→v3 (2 each) — the ≥2-reviewed-iterations
aim is met on every page, all nine iteration files preserved in
site/iterations/ for founder review. Shared-asset deltas this sprint: tokens.css
(cinema-dark + fonts fix), site.css (band-ink retoken, dead scrub CSS removed),
calculator.js (3 sanctioned hooks + 1 display-copy string; formulas byte-frozen),
og-card re-rendered, state-mark SVGs vendored into site/assets. FOR THE FOUNDER:
(1) the morning review — iterations are numbered if you want to compare rounds;
(2) optional: drop the four woff2s into site/assets/fonts/ per site/README.md to
self-host Inter/JetBrains Mono; (3) the standing pre-publish list (screenshots
via docs/screenshots/CAPTURE.md, real video assets for the performance scrub,
public repo + deploy per site/README.md) is unchanged. Loop closed — no further
wakes scheduled.

# ROUND 2 — founder review of 2026-06-11 (morning)

Standing direction: "checking for quality along the way… feel free to have
a little creativity and a little wow factor." Everything below is from the
founder's review. NEW MANDATE: every section on every page carries a
visual/animated element ("that's gonna be mandatory"). Leave placeholders
for rich media (founder will supply a video file that syncs with the scrub
playhead; later, real screenshots). Terminology: prefer NAS over "server"
everywhere it fits ("a ubiquitous term creative people understand…
a server is a black box of magic to them").

## Backlog — round 2
- [x] S0. BUGS (canonical hotfix, committed d414cda): calculator chart
      invisible after restored-scroll loads (IO initial report beat the
      async pricing render; visibleNow() check added); race lanes
      finished inside one frame at uniform ≈114× (now per-lane scale,
      cold ≈1.5 s sweep / real-time at 1 GbE, two-scale note).
- [ ] S1. Research pack (WebSearch agent): Strada (S-T-R-A-D-A, Michael
      Cioni) value prop + pricing + lane fit; Suite Studios current
      status ("isn't that the same product as Iconik?" — verify
      relationship/acquisitions; founder calls Suite "sort of
      irrelevant" now); Iconik Storage Gateway / Suite on-prem cache
      add-ons (founder: both sell a local cache you install on your own
      hardware — confirm + price); Shade minimum seats (founder: "I
      should be able to get shade with a one-seat quota and five
      terabytes" — verify their storage model beyond active-per-seat +
      offline pinning docs); Dropbox / Google Drive / Nextcloud pricing
      + streaming semantics (for table + future calculator tiers); S3
      egress costs for the exit story (AWS, B2, R2, Wasabi; 20 TB
      worked example) + any documented SaaS export/egress fees; design
      reference notes from shade.inc / Suite / Iconik / LucidLink /
      Strada sites (hero patterns, laptop mockups, NLE logo usage).
      Output: dated facts pack → docs/web/RESEARCH_ROUND2.md; pricing
      deltas → site/assets/pricing.json (formulas stay frozen until
      founder sees the diff).
- [x] S2. home-v5: hero gets a macOS-style network-drive icon popping in
      ("like what would show up… with the name JuiceMount"); lead
      de-buried ("it's a real mount — your NAS shows up in Finder");
      consolidate the second line; instant-search demo (real search
      figures: ~29 ms across ~131 K names, vs SMB search pain) with
      example results; "menu bar tells the truth" reframed as the START
      of how-it-works (menu bar app is where you create the mount);
      block-level vs byte-level primer BEFORE the chunk demo (the
      upload-once message isn't landing without it); SMB contrast
      explainer (no real cache → remote = slow, local = round-trip
      inefficient; JuiceMount fixes both); scrub/playhead widget
      promoted from performance in compact form + data-video
      placeholder hooks (founder supplies the clip later; sync video to
      playhead); "without picking two" → any commercial NAS with
      Docker: QNAP, Synology, TrueNAS, Unraid…; "How it works" →
      "The technology stack" (NLE → normal Finder volume via NFS;
      SQLite metadata cache, SSD block cache, write spool; JuiceFS
      chunks to S3 objects on your NAS — less wordy, keep GitHub link);
      what-it's-not gains roadmap teaser (Finder-adjacent full-size
      app w/ richer metadata views); your-bytes closing goes deeper;
      NLE names as text (Resolve, Premiere, FCP — "to your Mac it's
      just a drive"); a laptop/desktop mockup frame (our own drawing,
      no third-party logo art); NAS wording sweep.
- [x] S3. performance-v4: tagline pivots from "7 Gbit/s to hardware you
      own" to latency-first — "storage that scales but feels local"
      (echo on homepage); perf numbers into a smaller table (less
      billboard); race widget re-ideated: not warm-vs-cold, but
      JuiceMount cache vs typical SMB round-tripping (every open/seek
      pays the wire; cache keeps your working set local and current);
      throughput reframed: cloud SaaS is WAN-bottlenecked — on-prem =
      local speed on site + cached speed off site, best of both
      (plainer words); add the SaaS on-prem-cache-add-on comparison
      (Suite/Iconik paid local cache — from S1 research); disk-platter
      logo animation (platter spins, read head seeks — the mark IS a
      read head over a citrus platter) as a section visual; "mount it
      wherever you want" titling idea; NAS wording sweep.
- [x] S4. compare-v4: "a three-way trade / why two lanes exist" REDESIGNED
      as a visual (pay-a-lot SaaS vs struggle-with-SMB, us in the
      middle — barely any words); table: ADD Dropbox, Google Drive,
      Nextcloud (familiar anchors), Strada (pending S1), DROP Aspect
      (unknown) and Suite (founder: irrelevant; pending S1 verdict),
      KEEP LucidLink, Shade (their offline pinning is documented —
      fix cell), Iconik; Open-source row: explicit "No" not a dash;
      "leave with your bytes" → cost-to-migrate framing (time + fees;
      S3 egress worked example, 20 TB); drop the "pricing in detail
      checked June 2026" cruft under the table; "what they do better" →
      "compromises you'll make with JuiceMount" (no managed support,
      you administer your own NAS — we make it easy, no
      collaboration/review layer yet — roadmap), consolidated with
      pick-the-other-thing-if; self-hosted sync table: simplify the
      ~1 Gbit point (we ride your connection up to 10 GbE; they won't
      move hundreds of GB in an editor's afternoon), DROP the wordy
      offline-semantics row; "leave whenever" panel: call out the
      built-in migration tool (JuiceMount Manager), "cost to enter and
      cost to exit is free", show generic S3 ingest/egress economics
      instead of the SaaS toggle; NAS wording sweep.
- [x] S5. calculator-v4: seat minimums audit (Shade 1-seat × 5 TB must
      price if their public tiers allow it — model their storage
      add-on instead of not_comparable, pending S1); jargon →
      footnotes (founder: "get a little less jargon out of there or
      minimize into much smaller footnotes"); keep/extend the
      vendor-color denotations (Suite blue / JuiceMount green —
      founder likes); groundwork for Dropbox/Google Drive tiers
      (pending S1 pricing); exit-cost panel: migration tool callout +
      S3 egress example. Formula changes go to the founder as a diff
      before canonical promotion.
- [x] S6. docs page v1 (site/docs.html or site/docs/): line-by-line
      get-started: confirm your NAS can run it (Docker check, vendor
      notes: TrueNAS SCALE, Synology DSM 7+, QNAP, Unraid, plain
      Linux); install the stack (compose YAML walkthrough); verify
      it's reachable on your network; install + open the JuiceMount
      client; macOS permissions; first mount; pin/offline basics;
      troubleshooting. References across hardware vendors. Source of
      truth: README + repo docs only — no invented behavior.
- [x] S7. Cohesion: NAS-term sweep on all canonical pages, motion
      language, value-gate re-run, OG copy, link battery; promote
      winners; journal verdicts.

## Journal — round 2
- [2026-06-11 ~09:00] S0 — both founder-reported bugs reproduced, fixed,
  verified live (chart draws when rendered in-view; race actually races,
  warm beats cold visibly, sync crawls at its own labeled scale) — d414cda.
- [2026-06-11 ~11:30] S2+S5+S6 — home-v5, calculator-v4, docs-v1 built,
  verified, promoted. home-v5 (my build): hero desk vignette (drive pops
  onto a Mac-desktop corner, menu-bar mark tints healthy, ping ring),
  lead de-buried per founder ("It's a real mount… Storage that scales,
  but feels local"), NEW instant-search demo (type-along ⌘⇧F window,
  3 query chips, ~29 ms/131K badge — README-sourced; SMB walk-the-tree
  contrast card), states section reframed "It starts in the menu bar",
  block primer (one-object-crawls vs three-blocks-hop, CSS choreography)
  ahead of the retitled chunk demo, SMB round-trip card added to a now
  3-way compare row (traffic-pattern glyphs: ping-pong dot / slab /
  chips), compact scrub demo with data-video rich-media hook (founder's
  clip will scrub via the playhead; live timecode until then), NAS
  vendor line + docs link, "The technology stack" retitle, what's-coming
  roadmap card (ROADMAP-sourced; Finder-adjacent app labeled
  drawing-board), deeper your-bytes close (migration tool + exit link).
  Fixed in verification: setPointerCapture ordering on the scrub strip
  (synthetic/stale pointers killed the drag before fromEvent ran).
  calculator-v4 (my build): "The exit, priced" panel (20 TB out: AWS
  ≈$1,741 vs B2/R2/Wasabi $0, bars fill on scroll; migration-tool
  callout), display-only #notices (Suite BYO 20 TB floor + CSP-fees,
  LucidLink $27-is-promo, Shade unpublished overage) fed by jm:sim —
  formulas untouched, NAS terms, jargon hints rewritten, preload links
  self-referencing. docs-v1 (agent build, gates all green incl. its own
  browser pass): 7-step guide + uninstall, sticky progress rail, vendor
  cards (TrueNAS production-tested badge vs vendor-generic), verbatim
  compose YAML diff-verified, copy buttons w/ clipboard fallback —
  promoted to site/docs.html (nav self-ref fixed at promotion; srcset
  rewrite gap in my promotion script caught by the battery and fixed).
  Full battery after promotion: 17 files, 510 refs, 0 broken. S1
  research committed earlier (8163dfa) — URGENT corrections applied to
  pricing.json + FALLBACK mirror. compare-v4 agent died on a 529 at
  startup — relaunched. performance-v4 agent still running.
- [2026-06-11 ~13:00] S3+S4 — performance-v4 + compare-v4 (agent builds),
  browser-verified by orchestrator, both promoted. performance-v4: hero
  is the founder's tagline verbatim ("Storage that scales, but feels
  local."), latency-first statline w/ 7 Gbit/s demoted-but-kept, platter
  homage ornament (spin + head-seek, reduced-motion static, mark files
  untouched), race re-ideated as "One minute of editing, two ways" —
  SMB lane 1,390 wire trips / 38 s vs JuiceMount 20 trips / 7.3 s at
  the VPN default (illustrative beats labeled; totals node-verified;
  live-verified to settle exactly), LAN verdict concedes SMB is fine on
  a quiet LAN; #tp chart → WAN-vs-LAN pipe diagram in plain words;
  charts condensed (cache-vs-wire plate stays, #table = canonical
  numbers home); NEW "Their answer to latency is your hardware"
  (research §3, all four vendors, dated); #scrub diff-verified sacred;
  #race/#scrub anchors stable. compare-v4: three-way trade redesigned
  as the rent-meter / ping-pong / quiet-middle visual (1 sentence per
  pole; 360px+dark verified); Aspect dropped, Suite kept (research:
  alive, funded, ≠ Iconik — founder flag raised); iconik corrected to
  $9/$65/$120 + credits/egress; Shade annual/monthly labels + pinning
  ✅ documented; Dropbox/Workspace familiar-anchors compact table
  ("automatically download" receipts); Strada other-approaches card;
  open-source row explicit "No"; leave-with-bytes → cost-to-migrate;
  pricing cruft → one details element; concessions consolidated into
  "Compromises you'll make with JuiceMount" (zero honesty deleted);
  exit section leads with the migration tool + the 20 TB worked
  example (AWS ≈$1,741 vs $0×3). S7 progress: nav aligned across all
  six canonical pages (docs.html order fixed at promotion), full
  battery 19 files / 559 refs / 0 broken, voice clean, ink literals
  all sanctioned (receipt paper ×2, platter outline ×5), docs
  exclamations all inside pre/code. README: stale Shade-pinning cell
  corrected (research receipt), Aspect column dropped, price labels
  annotated — the README/site compare tables agree again. Adversarial
  verification workflow (3 lenses + refuters) running; its confirmed
  findings land as a follow-up fix commit.
- [2026-06-11 ~14:30] S7 + adversarial audit round — CLOSED. The
  ultracode verification workflow (3 lenses x 4 pages, every finding
  attacked by a refuter; 29 agents) confirmed 25 of 26 raw findings
  (6 P2, 19 P3). ALL 25 fixed and live-verified:
  - index: #states moved to demo 01 (menu bar genuinely starts the
    how-it-works course now: menu bar -> mount -> search -> blocks ->
    scrub, renumbered), nav "How it works" retargeted to #states on
    all pages, 8 leftover "server" uses -> NAS (the one survivor is
    the README-verbatim "server stack"), visuals mandate closed on the
    bottom third (pillar traffic/pin/meter glyphs — the economics
    meter counts to the documented $750/mo and parks at "forever";
    what's-coming roadmap rail lights 4 dots in order; closing band
    exit-echo chips), search demo timings collapsed to the one frozen
    ~29 ms figure, replay buttons now work under reduced motion (snap
    to end state), .is-veiled gains visibility:hidden (no tabbing into
    invisible Finder rows), svh-strip stray aria-hidden=false removed,
    mb panes tabindex=0.
  - calculator (agent): receipt register date 2026-06-11 + sync
    comment, Shade option/receipt labeled annual ($35 monthly from
    pricing data), aria-valuetext on both range sliders, aria-live on
    #warnings/#notices, full ARIA table roles restored on the receipts
    (display:block had stripped semantics), reduceQ change listener.
    Formula freeze re-proven: the five math functions byte-identical
    to HEAD, default-scenario output identical.
  - performance (agent): ISG claim re-hedged to "appears included";
    #6FB1EB and #f2f2f4 literals correctly KEPT with documented token
    verdicts (the .pv panel is forced-dark on both themes; tokens
    would shift computed colors); #method pipeline glyph, #noclaim
    measured-vs-modeled legend, closing platter echo (byte-identical
    SVG reuse); dead race-link selector left per the in-file
    byte-identity note.
  - docs (agent): uninstall pin-list overclaim corrected against
    scripts/uninstall.sh, success-log quotes made true for BOTH
    composes (they print different precheck-5 strings — receipts in
    the agent report), aria-current="step" on the rail (markup, JS,
    and the 3 CSS selectors), step-4 preflight mock now animates its
    checks with a replay (the page's second interactive).
  - og-card.svg/png re-rendered with the identity one-liner ("the
    open-source alternative... storage that scales, but feels local")
    replacing the pre-identity 10GbE line; both README banners updated
    the same way; og:image:alt on all five pages + README banner alt
    match; README "why this exists" drops Aspect + "magic".
  - NEW BUG found during verification, fixed in calculator.js:
    pricing.json was fetched with default cache policy, so a browser
    that cached the old JSON rendered "$undefined monthly" after the
    data update added seat_monthly — fetch now uses cache:"no-cache"
    (revalidation; 304s keep it cheap) and the label guards against
    missing keys. This is a deploy-relevant fix, not just a dev nit.
  - One preview-tool incident for the record: the shared preview
    renderer wedged (blank composites, dead IntersectionObservers)
    and mimicked a page bug; a server restart cleared it. The page
    was never broken — diagnosis receipts in the session log.
  Final gates: 19 files / 560 refs / 0 broken; every inline script
  parses; formula freeze byte-identical; consoles clean on all six
  canonical pages.

## ROUND 2 CLOSING (2026-06-11 afternoon)
S0–S7 complete across seven commits (d414cda, 97e0a21, 8163dfa,
c46d5bb, 0f5f8b4, 20b4d6b + the wave-1 journal commit). Six canonical
pages at the round-2 standard; iteration trail preserved (home-v5,
performance-v4, compare-v4, calculator-v4, docs-v1). Founder decisions
parked: (1) Suite stays in the table pending his call — research says
independent + funded, not Iconik; (2) the scrub video file slots in via
data-video on #svh-stage when he supplies it; (3) Dropbox/Drive
calculator tiers are data-ready (pricing.json) but not built into the
simulate() formulas — formula changes remain founder-gated.

# ROUND 3 — founder review of 2026-06-11 (afternoon)

## Backlog — round 3
- [x] T0. GLOBAL: the demo volume is named JuiceMount, not zpool —
      "/Volumes/JuiceMount" everywhere on the site ("much cooler").
- [x] T1. Comprehension gut-check (founder-mandated): a no-context
      sub-agent with a creative-not-technical persona reads the home
      page cold and reports, section by section, what they think the
      product is/does. Findings inform the T2 flow rework.
- [x] T2. home-v6: hero mock dies — no window chrome, no menu-bar strip,
      no caption: just a macOS-style external-drive icon popping on
      with a green glow, labeled JuiceMount, ONE-TIME (no replay
      button). Kill the statline (7 Gbit/100 GB/$0 callouts). H1 stays
      but record the A/B plan (simpler variant: "Self-hosted block
      storage for media" class). Menu-bar section demoted far down
      (it's "how you interact with it", not the opener — the app is
      nothing without the NAS-side stack). Section order per founder:
      real mount → instant search → blocks ("Your files become blocks.
      That's the magic." — founder's phrase) with the scrub timeline
      copied in as its second segment + the read-back story FIXED
      (reading back your own upload makes no sense — it's a teammate's
      Mac / any other Mac that reads 3 seconds) + the SMB
      back-and-forth duel visual ported from performance ("I quite
      like that one") → three-ways cards cut to graphical (current
      copy "insane"ly wordy) → pillars reworked: speed "tested up to
      10 GbE", resilience is offline/pinning (NOT milliseconds —
      that's latency), economics in plain words (no "where the bucket
      lives decides the bill") → menu-bar/how-you-drive-it → tech
      stack → what's-not → closing.
- [ ] T3. performance-v5: platter animation must use the REAL mark —
      dissect logos/color.svg, separate the founder-designed read head
      and platter groups, animate those (not an homage drawing). WAN
      graphic redesigned: the cloud path must visibly choke through
      the ISP (current layout puts NAS and cloud on the same side —
      "doesn't work"). DROP "Mount it wherever you want" (conveys
      rename-ability, not the intended hardware-agnosticism). "Their
      answer to latency is your hardware" visual reworked by a
      sub-agent for moderately-technical creatives (pain points: long
      waits for massive files; paying cloud rent while owning a NAS).
- [x] T4. docs-v2 + remote access: a "Working remotely" section —
      JuiceMount does storage, Tailscale does routing (README-sourced:
      "the author runs Tailscale; any VPN that gives the Mac a route
      to the Redis + MinIO ports works"). Simple, line-by-line. Plus a
      pointer from performance/home where remote work comes up.
- [ ] T5. Verify (gates + fresh browser pass), promote, cohesion,
      journal, closing report.

## Journal — round 3
- [2026-06-11 ~16:30] T0+T1+T2+T4 — zpool is dead (long live
  /Volumes/JuiceMount): full sweep on the marketing demos; docs keeps
  the shipped YAML's literal zpool (it's verbatim-verified) and gains a
  "want Finder to say something nicer? set JM_VOL_NAME before first
  format" pointer — changing the SHIPPED default is flagged as a
  founder/server-side decision. T1 comprehension gut-check (no-context
  freelance-editor persona): comprehension B-, the demos teach better
  than the copy; "demo 02 + the scrub do the heavy lifting";
  persuasion peaks at $0-vs-$750/mo; CONVERSION DIES at
  docker/YAML/GitHub CTAs; "will it eat my footage" raised by the
  blocks lecture and never answered; zpool flagged cold (validating
  T0). Full report drove T2. T2 home-v6 built+promoted: hero mock
  replaced by the founder's ask — bare macOS-style drive icon, green
  glow pop, one-time, no replay, no window chrome; statline killed;
  H1 A/B note recorded (simpler "self-hosted block storage" variant
  to test); section order mount→search→blocks(+scrub as "the magic,
  part two")→the ported SMB round-trip duel (founder: "copy that
  visual over — I quite like that one")→three-ways cards cut to
  one-liners→pillars reworked (10 GbE tested-up-to; resilience as
  offline/pinning not milliseconds; economics in plain $0 words)→
  menu bar demoted to "the cockpit · how you drive it" with a
  jargon-translating caption (Redis/MinIO/FUSE/NFS = the four links
  in the chain)→stack→not-yet→closing (persona rewrite: "LucidLink
  speed, on the NAS you already own — free, forever"). Conversion
  fix: primary CTAs now "Set it up on your NAS — the guide" → docs;
  GitHub demoted to secondary. Chunk story reframed (a TEAMMATE
  scrubs three seconds — reading back your own upload made no sense,
  founder right); safety answer in persona words ("if JuiceMount
  vanished tomorrow, a free standard tool still opens everything").
  Port bugs found+fixed in verification: my substring extraction
  grabbed a NESTED wireLocRadios as standalone (closure refs →
  silent load-kill) and missed CACHED_MBPS_LO/HI; both repaired,
  whole-script no-throw proven by in-page execution probe. T4
  docs "Working remotely" (agent): step 7 of 8, Tailscale 3-step
  (README-sourced, third-party-labeled), tunnel visual, rail wired;
  home duel links to docs#remote. TOOL NOTE for the record: the
  preview tab reports visibilityState=hidden when the Launch panel
  isn't showing it — Chrome throttles rAF to ZERO there; this
  masqueraded as broken widgets twice today. rAF-driven motion is
  verified by code-equivalence to the visible-tab runs on
  performance; final visual QA wants a visible tab.
