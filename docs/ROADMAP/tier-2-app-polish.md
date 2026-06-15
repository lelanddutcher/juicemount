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
| `Operation timed out` | `Couldn't reach server at 127.0.0.1. Check your Wi-Fi or VPN. (Last successful: 2 min ago)` |
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

## Iteration plan

Each item is one commit + code-reviewer pass (Swift-UI changes don't
always need review, but onboarding-wizard and Sparkle do touch
lifecycle).

| # | Slice | Hours | Files |
|---|---|---|---|
| 2.A.1 | Welcome screen + server URL field (paste → validate via TCP dial) | 4 | `app/JuiceMount/Sources/JuiceMount/UI/OnboardingWindow.swift` (new) |
| 2.A.2 | Mount-point picker + cache-size slider + "starter pin" optional | 3 | same |
| 2.A.3 | First-launch detection (skip wizard if `Preferences.hasCompletedOnboarding`) | 1 | `Preferences.swift` + App.swift |
| 2.B.1 | Popover redesign: per-project tree replacing flat actions list | 4 | `MenuPopoverView.swift` |
| 2.B.2 | Active-Projects list driven by `.juiceproject` files (depends on tier 6 schema — can stub with manual pin roots) | 3 | same |
| 2.C.1 | Error catalog: map every user-facing `syscall.*`/NFSStatus to (description, next-step, copy-diagnostic) | 5 | new `ErrorCatalog.swift` |
| 2.C.2 | "Copy diagnostic" button in error sheets, generates remediation snippet | 2 | `ErrorCatalog.swift` + popover row |
| 2.D.1 | Self-test dashboard window (read throughput + write probe + RPC latency + recent errors) | 4 | new `DashboardWindow.swift` |
| 2.D.2 | Sparkline rendering for historical self-test results | 3 | Charts framework |
| 2.E.1 | `UNUserNotificationCenter` events: pin-complete, sync-complete, offline-transition, error | 3 | extend `ServerController.swift` (offline-notify pattern already exists) |
| 2.E.2 | Per-event opt-in toggles in Preferences | 2 | `Preferences.swift` + new pane |
| 2.F.1 | Sparkle dependency added to SPM, plist config for stable channel | 2 | `Package.swift`, `Info.plist` |
| 2.F.2 | Beta-channel toggle in Preferences + appcast feed split | 2 | same |
| 2.G.1 | Dark-mode audit — render every view in light + dark, fix contrast | 3 | all UI files |
| 2.G.2 | VoiceOver labels on every interactive element, .accessibilityLabel everywhere | 3 | all UI files |
| 2.G.3 | High-contrast mode pass (replace `.ultraThinMaterial` washes) | 2 | popover background |

Total: ~46 hours = ~6 working days of focused tier-2 work.

## Signals to watch

| Item | Signal proving it works |
|---|---|
| 2.A | New user (defaults wiped) reaches mounted state via wizard, no docs consulted, <2min stopwatch |
| 2.B | Popover renders pinned-project tree at p99 <50ms (no cgo blocking on MainActor) |
| 2.C | Every error in the user-facing surface includes a "Copy diagnostic" button; clicked outputs a non-generic remediation |
| 2.D | Dashboard sparkline updates within 5s of each self-test cycle |
| 2.E | Toggling preferences off silences notifications immediately (next event); on grants permission and emits |
| 2.F | `appcast.xml` reachable; "Check for Updates" surfaces release notes |
| 2.G | macOS Accessibility Inspector reports 0 issues; high-contrast mode is legible |

---

## 2.H — "Rebuilding index" progress indicator (added 2026-06-15)

**Problem (observed on the Wi-Fi/cellular WAN test):** after a restart / app
update / juicefs remount, the full-tree reconcile repopulates the local metadata
cache (~261k entries: ~2 min on Wi-Fi, several minutes on a WAN link). The menu
bar already shows "Connected", but during this settling window navigation is slow
and freshly-created paths can transiently fail (post-remount stale handles) — and
the user has **no signal that anything is happening in the background**, so it
looks broken.

**Build:** surface reconcile/rebuild progress.
- Go core: expose an "indexing" state + progress — syncMetadata already tracks
  `lastSyncEntries` / `lastSyncDuration`; add an in-progress flag and entries-
  synced count (or %) on `/metrics` (or `/spool`/a status endpoint) and a "first
  full reconcile complete" milestone.
- Swift menu bar: show a **"Syncing… rebuilding index (N%)"** state (spinner +
  determinate progress where possible) instead of a bare "Connected" until the
  first post-remount reconcile completes. Especially important on first launch and
  on slow links. (task #37)

**Acceptance:** after a cold launch / remount, the menu bar shows a moving
progress affordance until the index is rebuilt, then transitions to "Connected".
