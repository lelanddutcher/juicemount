# juicemount.com — the static site

This directory is the whole site: plain HTML, CSS, and JS with no build
step. No bundler, no framework, no external CDNs — type uses locally
installed Inter/JetBrains Mono when present and otherwise the system
stack. Any static host can serve it as-is.

To self-host the brand fonts (optional, ~80 KB total): download the
woff2s from the official releases (Inter: github.com/rsms/inter,
JetBrains Mono: github.com/JetBrains/JetBrainsMono — both SIL OFL), drop
them into `assets/fonts/` under the four exact names listed at the top
of `assets/tokens.css`, and swap each `src:` line in that file for its
commented sibling. Until the files exist the `url()` sources stay
commented — a `src` that 404s fires a network request for every visitor
without the font installed.

The pages: `index.html` (hero + how it works + scrub explainer),
`calculator.html` (rent vs. own payback), `compare.html` (the honest
comparison), `performance.html` (author-measured numbers). Brand tokens
live in `assets/tokens.css`; the contract is `docs/web/BRAND.md`.

## Local preview

```sh
# from the repo root
python3 -m http.server -d site 8080
# then open http://localhost:8080
```

Opening `site/index.html` straight from Finder (`file://`) also works:
scripts are plain deferred scripts, and the calculator falls back to its
baked-in pricing table when `fetch()` is unavailable over `file://`
(`assets/calculator.js` keeps that fallback in sync with
`assets/pricing.json`).

## Deploy A — GitHub Pages

The caveat first: Pages' "deploy from a branch" mode only serves the repo
root or `/docs` — it cannot point at `site/`. Don't restructure the repo
around that; deploy with the Actions workflow instead, which uploads any
directory as the Pages artifact. (The older alternative — a `gh-pages`
branch you copy `site/` onto — works but adds a branch to babysit; the
workflow is one file and deploys on push.)

The repo isn't public yet, so the workflow file is intentionally not
committed (Pages on a private repo needs a paid plan, and the GitHub links
on the site 404 until the repo flips public anyway). When ready, enable
**Settings → Pages → Source: GitHub Actions** and add
`.github/workflows/pages.yml`:

```yaml
name: Deploy site to GitHub Pages
on:
  push:
    branches: [main]
    paths: ["site/**"]
  workflow_dispatch:
permissions:
  contents: read
  pages: write
  id-token: write
concurrency:
  group: pages
  cancel-in-progress: true
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/configure-pages@v5
      - uses: actions/upload-pages-artifact@v3
        with:
          path: site
      - id: deployment
        uses: actions/deploy-pages@v4
```

Custom domains work on GitHub Pages too (a `CNAME` file plus DNS), but the
Cloudflare route below handles the apex domain, redirects, and headers with
less ceremony — use Pages as the fallback, not the primary.

## Deploy B — Cloudflare Pages (recommended for juicemount.com)

Recommended because the three things this site needs at launch — an apex
custom domain, a www redirect, and cache headers — are first-class there,
and the `_headers` file in this directory only does anything on Cloudflare.

1. **Connect the repo.** Cloudflare dashboard → Workers & Pages → Create →
   Pages → connect the GitHub repo.
2. **Build settings.** Framework preset: none. Build command: leave empty.
   Build output directory: `site`. Every push to the production branch
   deploys; other branches get preview URLs.
3. **Custom domain.** Project → Custom domains → add `juicemount.com`, then
   add `www.juicemount.com`. With the zone on the same Cloudflare account,
   the DNS records are created for you; otherwise set:

   | Type  | Name | Target                  | Proxy   |
   |-------|------|-------------------------|---------|
   | CNAME | `@`  | `<project>.pages.dev`   | proxied |
   | CNAME | `www`| `<project>.pages.dev`   | proxied |

   A CNAME at the apex is normally illegal DNS; Cloudflare flattens it to
   A/AAAA answers automatically, which is why this works.
4. **www → apex redirect.** Rules → Redirect rules → single redirect:
   wildcard pattern `https://www.juicemount.com/*` →
   `https://juicemount.com/${1}`, 301, preserve query string. (A
   `_redirects` file with `https://www.juicemount.com/* https://juicemount.com/:splat 301`
   is the in-repo alternative; the dashboard rule keeps the redirect even if
   the project moves.)
5. **SSL.** Universal SSL issues automatically once the custom domain
   activates — usually minutes, occasionally up to an hour; the domain
   serves from `*.pages.dev` with valid TLS in the meantime. Set SSL/TLS
   mode to Full (strict); the Pages origin is Cloudflare-managed, so strict
   costs nothing. The `<project>.pages.dev` URL stays reachable after
   launch; every page's canonical tag already points at `juicemount.com`,
   which keeps search engines on the domain.

Whichever route deploys, the artifact should exclude `site/iterations/`
and `.DS_Store` litter. The iterations directory is design drafts, not
pages; the `/iterations/*` noindex rule in `_headers` is the backstop if a
copy ships anyway, but the clean move is to not upload drafts at all. Both
routes run from the repo root, so the same commands work in the Cloudflare
build command field or as an Actions step before `upload-pages-artifact`:
`rm -rf site/iterations && find site -name .DS_Store -delete`.

## Cache headers

`site/_headers` ships the rules (Cloudflare Pages format; inert on any
other host):

- **HTML: `no-cache`** — browsers revalidate before reuse, so a deploy is
  visible on the next request. Each page is listed twice (`/compare` and
  `/compare.html`) because Pages serves clean URLs alongside the `.html`
  paths.
- **`/assets/*`: `max-age=86400` + 7-day `stale-while-revalidate`** —
  nothing is content-hashed, so "immutable" would pin stale CSS/JS after a
  deploy; a one-day browser cache is the honest middle. Worst case after a
  deploy: a returning visitor gets day-old styling once while the cache
  revalidates.
- **Root icons: `max-age=604800`** — favicons and the touch icon change
  about never and browsers hammer them.
- Cloudflare purges its edge cache on every Pages deploy; these headers
  only govern browsers.

## The og:image URL, and the pre-launch checklist

Every page's `og:image` is the absolute URL
`https://juicemount.com/assets/og-card.png` — link scrapers resolve images
against nothing, so a relative path would break card previews. Consequence:
previews shared from `*.pages.dev` or from a preview deploy still point at
the production URL, and cards only render once juicemount.com DNS is live.
That is the intended trade.

Regenerating the social/icon assets (sources in `assets/`, pipeline is
`scripts/svg2png.swift`):

```sh
swift scripts/svg2png.swift site/assets/og-card.svg site/assets/og-card.png 1200 630
swift scripts/svg2png.swift logos/black.svg site/favicon-32.png 32
swift scripts/svg2png.swift logos/black.svg site/favicon-16.png 16
swift scripts/svg2png.swift site/assets/apple-touch-icon.svg site/apple-touch-icon.png 180
```

(`favicon.svg` is `logos/color.svg` verbatim — modern browsers take the
SVG; the 32 px mono PNG is the legacy fallback, per the BRAND.md rule that
the color mark mushes below ~32 px. `favicon-16.png` ships for anything
that wants an exact 16 — an `.ico` bundle later, for instance — and is
deliberately not linked in the heads.)

Before announcing the site:

- [ ] Flip the GitHub repo public (or update the
      `github.com/lelanddutcher/juicemount` links if the slug changes) —
      every "GitHub ↗" link 404s until then.
- [ ] Run the screenshot capture (`docs/screenshots/CAPTURE.md`) and wire
      the real PNGs into the site and README.
- [ ] Check the favicon in light and dark tab strips — the SVG color mark
      carries both (its ink outline does the work); the mono PNG fallback
      only matters in legacy browsers.
- [ ] `curl -I https://juicemount.com/assets/og-card.png` returns 200, then
      spot-check a link preview with the validator of whichever network you
      post to first.
- [ ] Re-check the dated pricing cells on `compare.html` and
      `assets/pricing.json` — every price says "checked June 2026" and
      SITE_PLAN.md calls for a re-verify at launch.
