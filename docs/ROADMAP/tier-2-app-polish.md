# Tier 2 — macOS app polish ("iconic")

Goal: editor opens the app for the first time, gets to a working mount
in under 2 minutes, never feels like they're using a dev tool. Iconic
UX is the difference between "interesting OSS project" and "thing my
team actually uses."

## Acceptance tests

| # | Test | Pass criterion |
|---|---|---|
| 2.1 | First-launch onboarding | New user from `open JuiceMount.app` → mounted and editing in <2 min, zero docs consulted |
| 2.2 | Error self-service | Every user-visible error includes copy-pasteable remediation command or clear next-step. Generic "something went wrong" = bug. |
| 2.3 | Status legibility | Menu bar icon distinguishes idle / starting / running / syncing / offline / degraded / error with a glance, no tooltip required |
| 2.4 | Diagnostics export | One click produces a zip a contributor can ingest, including logs, mount table, JuiceFS state, MinIO healthcheck output |
| 2.5 | Auto-update with channels | Sparkle integration, two channels (stable/beta), user-controlled cadence, never forced |
| 2.6 | Dark/light + accessibility | Pass macOS HIG accessibility audit; VoiceOver labels on every interactive element; works in dark mode and high-contrast mode |

## Feature backlog (ordered by impact)

### 2.A — First-launch wizard

Reference: Tailscale's first-launch is the cleanest cross-platform
example. Suite Studio's onboarding is closest in our domain.

Flow:

1. Welcome screen — one paragraph of what JuiceMount is.
2. Server URL field — accepts `redis://host:port` or a paste-able
   share string. Validate live (TCP reachability + version handshake).
3. Mount point picker — defaults to `/Volumes/<volume-name-from-server>`.
4. Optional: cache-size slider with disk-free hint.
5. Pin a starter folder (optional).
6. Done. Mount opens, popover shows briefly with status.

No accounts, no IAM, no email verification. The shared key IS the
credential.

### 2.B — Popover redesign

Today's popover lists status + actions in a single column. The
target layout:

```
┌──────────────────────────────────────────┐
│  ● Connected · 287 GB cached / 2 TB pool │
│  ─────────────────────────────────────── │
│  📁 Active Projects                      │
│    ▸ Scott Pride Cake     8/8 ●          │
│    ▸ Brand Spot Vol 3     12/47 ⋯        │
│    ▸ Frame Test Drive     2/2 ●          │
│  ─────────────────────────────────────── │
│  🔍 Search Files…                  ⌘⇧F  │
│  🔄 Sync Now                             │
│  ⚙ Preferences…                    ⌘,   │
│  📤 Export Diagnostics…                  │
│  🚀 Force Eject Mount                    │
│  ⏻ Quit JuiceMount                  ⌘Q  │
└──────────────────────────────────────────┘
```

Per-project tree with completion indicators. Reference: LucidLink's
filespace view, but project-organized (Suite's contribution).

### 2.C — Self-explaining errors

Every code path that user-faces an error must produce:

- A one-line plain-English description (no error codes alone).
- A "What to do next" line.
- A `Copy diagnostic` button that includes the exact command or
  config snippet to fix or report.

Reference: nobody does this well. Differentiator.

Examples:

| Today | After |
|---|---|
| `Operation timed out` | `Couldn't reach server at 192.168.0.210. Check your Wi-Fi or VPN. (Last successful: 2 min ago)` |
| `redis: client is closed` | `Server connection dropped mid-operation. JuiceMount will retry in 60 s. [Retry now]` |
| `Stale NFS file handle` | `That file moved or was rebuilt while open. Close and reopen the file. (Affects: testing/)` |

### 2.D — Self-test dashboard

A real status page accessible from the popover, not just a menu icon
dot. Inspired by Tailscale's `Status` view.

Surfaces:

- Read throughput (latest self-test + historical sparkline)
- Write probe (now/historical)
- Connection state (online/offline/degraded with reasons)
- Cache fill (per-project breakdown)
- RPC latency distribution
- Recent errors (last 10, with copy-diagnostic per row)

### 2.E — Notifications (opt-in)

`UNUserNotificationCenter` for events worth attention:

- Pin completed: "Brand Spot Vol 3 fully cached (12 GB)"
- Sync completed: "Sync done — 234 files updated"
- Offline auto-engaged: "Network lost — operating offline (14 pinned available)"
- Recovery: "Reconnected to server"
- Error: "Cache disk nearly full — reclaim space?"

Each user-configurable in Preferences.

### 2.F — Sparkle auto-update

- Two channels: `stable` (signed + notarized releases) and `beta`
  (signed only, opt-in via Preferences).
- Manifest hosted on the project's GitHub releases page or a
  self-host-friendly mirror.
- Never auto-install — always prompt with changelog.
- Per the non-negotiables in `VISION.md`: never force-push updates.

### 2.G — Dark/light + accessibility audit

- Test every view in dark mode (some today don't render right).
- VoiceOver labels for the icon + every popover row.
- High-contrast mode support (the popover's `.ultraThinMaterial`
  background washes out badly in high-contrast).
- Keyboard navigation for the search window (already exists; polish).
- Reduce-motion respect for icon animations.

## Anti-patterns

- **No vocabulary that requires onboarding.** Don't introduce
  "filespace," "snapshot identifier," "block manifest." The user
  knows "shared folder," "project," "pinned."
- **No dozens-of-toggles preferences pane.** Sane defaults; advanced
  settings hidden behind a disclosure triangle.
- **No success-toast spam.** Notifications for failures and
  user-triggered completions only.
- **No telemetry without opt-in.** Period.

## Dependencies

- Tier 1's offline-state work (tier-4 doc, items 1.7-1.10) provides
  the data the UI surfaces. Tier 2 builds the surface.
- Tier 5 (Finder UX) needs an installer flow for FinderSync
  registration — overlaps 2.A's wizard step.
