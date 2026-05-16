# Competitive analysis: Suite Studio

## What it is

Suite Studio (<https://suite.studio>) is a SaaS storage-and-collaboration
product for video teams. It mounts as a single network drive on Mac and
adds a team-first layer: per-project workspaces, activity feeds, comments
on files inside Finder, real-time presence indicators, and an integrated
review tool.

Architecturally adjacent to LucidLink (cloud-streamed mount), but the
product axis is *team UX* rather than *streaming engine*.

## What to learn from them (tier-2 and beyond)

Suite is not in our streaming-engine competition lane. They're in the
"how does this *feel* to use as a team" lane. We don't want to be Suite,
but their UX patterns are the right reference for tier-2 polish.

### Onboarding flow (tier-2 reference)

Suite's first-launch is the cleanest of the three. The user enters an
invite code → the app authenticates, lists available projects, lets the
user pick one or more to mount, and they're done. No filespace
configuration, no bandwidth toggles, no IAM.

**Our adaptation:** the onboarding wizard in tier-2 should aim for this
feel. Shared key → list of projects (from MinIO bucket prefixes) → pick
which to mount → done.

### Project-as-first-class-concept

Suite organizes the mount around projects, not folders. Each project has
its own pin state, members, settings. Switching projects switches what's
mounted/visible.

**Our adaptation:** the `.juiceproject` YAML file (tier-6) maps to this
concept. A `.juiceproject` declares: which paths to pin, what bandwidth
budget to use, what NLE config presets to load. Commit it to git
alongside the project; new editors `git clone` it and double-click to
configure their JuiceMount.

### Per-project status visibility

Suite shows per-project sync status — bytes downloaded, who's currently
working in it, last activity timestamp.

**Our adaptation:** tier-2 popover layout should organize by project root,
with per-project progress bars and pin counts. Roll up to a global
status at the top.

## What we explicitly don't take from Suite

### 1. Web platform / review tools

Suite has been investing in a browser-based review interface — comments,
approvals, version comparison. This is Frame.io's competitive lane.

**Our position:** the user already has Frame.io if they want this. We're
the storage; the review layer is someone else's product.

### 2. Real-time presence / activity feeds

Suite shows "Jane is editing this project" in real-time, with an avatar
overlay. Useful for some teams; complexity-expensive for us; out of scope
unless tier-7 (collaboration) becomes a thing.

### 3. SaaS-only deployment

Suite hosts everything. There's no self-host option, no on-prem story.
That's their business; not ours.

## Specific UX details to study

When we hit tier-2:

- Their first-launch wizard
- Their project-list view in the menu bar
- Pin / unpin interaction (they have a particularly clean version)
- How they show "this file is offline" vs "this file is in the cloud"
  in Finder
- How they handle the offline mode UX

Screencap into `docs/COMPETITIVE/screenshots/suite/` if a trial account
is available.
