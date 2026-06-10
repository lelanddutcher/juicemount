# Screenshot capture guide (founder-run)

Five PNGs land in this folder with the exact filenames below. The README
has a commented-out "A look at it" section (search `SCREENSHOTS:` in
README.md) wired to these names — uncomment it once the files exist, and
the site can reuse the same files later.

## Ground rules

- **Light mode** for every shot: System Settings → Appearance → Light
  (the README is light-first; dark variants can come later).
- **Volume name:** the dev volume name (`zpool` / `zpool-dev`) is fine to
  show — it's not personal data. Do avoid frames where client/project
  file names you wouldn't publish are front and center; an SFX or LUTs
  folder makes a good safe subject.
- **Retina is fine.** Capture at native resolution; don't downscale.
- Two capture methods, used below:
  - **Window capture (with shadow):** `screencapture -i -W <out.png>`
    in Terminal, then click the target window. Equivalent: ⌘⇧4, press
    Space, click the window (saves to Desktop; move + rename it).
  - **Region capture:** ⌘⇧4 and drag. Needed for the menu bar and the
    popover (popovers don't reliably hit-test as windows).

## Shot list

### 1. `menubar-states.png` — the state-tinted menu-bar icon

The icon IS the health signal (green healthy / amber degraded / blue
offline-files / red fault), so show the states, not just one icon.

1. Start JuiceMount, wait for healthy (green mark).
2. ⌘⇧4 region-capture a short strip of the right side of the menu bar —
   the JuiceMount icon plus 2–3 neighboring icons for context. Keep the
   clock out of frame if you don't want the date visible.
3. Repeat the same strip for the other states you can trigger on demand:
   - **Blue (offline-files):** popover → Offline mode toggle on.
   - **Idle (green at 50%):** popover → Stop.
   - **Amber (degraded):** stop Redis on the server for a minute
     (`docker compose stop redis`), or pull the Mac's network. Optional —
     skip if you don't want to poke the live box.
4. Tile the strips into one image: open the first in Preview, then for
   each additional strip Tools → Adjust Size to confirm equal widths,
   and paste them stacked (Edit → Insert from clipboard), or just lay
   the PNGs side by side in any editor. Export as `menubar-states.png`.
5. Minimum viable: ship the healthy-state strip alone under this
   filename and improve later.

### 2. `popover-glance.png` — the popover, healthy, with cache detail

1. Click the menu-bar icon to open the popover while the server is
   healthy and a pinned folder exists (pin a small SFX folder first so
   the per-root list and Ready counts are populated).
2. Nice-to-have: a drain in progress (copy a few hundred MB onto the
   volume with the spool enabled) so the *Pending uploads* row is
   visible.
3. ⌘⇧4 region-capture the popover with ~10 px of margin around it.

### 3. `onboarding.png` — Setup Assistant preflight

1. Menu-bar icon → **Setup Assistant…** (it reopens any time; no need
   for a fresh install).
2. Show the preflight-check step with all three checks passing:
   `juicefs`, macFUSE, backend reachability.
3. Window capture: `screencapture -i -W docs/screenshots/onboarding.png`
   and click the Setup Assistant window. Default window size as-is.

### 4. `preferences-connection.png` — Preferences → Connection

1. ⌘, → **Connection** tab. The window is a fixed 600 pt width — leave
   it as-is.
2. The Redis URL with a private LAN IP (`192.168.x.x`) is fine to show;
   if yours displays a Tailscale hostname or anything public, retype a
   LAN-style placeholder first.
3. Window capture:
   `screencapture -i -W docs/screenshots/preferences-connection.png`,
   click the Preferences window.

### 5. `calculator-web.png` — the rent-vs-own calculator

1. Open `site/calculator.html` in Safari or Chrome (`file://` works —
   the site has no build step).
2. Resize the browser window to roughly 1280 × 900, light mode, leave
   the default inputs (they're the worked example) so the payback
   readout is populated.
3. Window capture:
   `screencapture -i -W docs/screenshots/calculator-web.png`, click the
   browser window.

## Afterwards

```sh
ls docs/screenshots/*.png        # all five names present
```

Then delete the `<!-- SCREENSHOTS:` and `-->` lines around the README's
"A look at it" section (keep everything between them) and re-run a link
check. The sprint loop (item L in docs/web/BRAND_SPRINT.md) wires the
same files into the site.
