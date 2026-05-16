# Tier 5 — Finder-adjacent UX (FinderSync — NEVER FileProvider)

Goal: the right-click-pin / status-badge / Spotlight-results
experience that competitors deliver through their kernel-extension
or FileProvider implementations — without the ghost-domain failure
class that bit us once already.

## The hard rule

**No FileProviderExtension, ever.** See `docs/no-fileprovider.md` for
the postmortem. The registration persists in `fileproviderd`'s
database for the life of the Mac, can pin `filecoordinationd` at
100% CPU, and the recovery requires Finder's privileged XPC
`removeDomain`. Never again.

What we ship instead:

- **`NSServices`** for right-click menu items. Works in any app, not
  just Finder. No daemon to register, no persistent state.
- **Finder Sync Extension (FinderSync)** for badge overlays only —
  it's the lightweight cousin of FileProvider. Apple positions it
  for Dropbox-style badge-and-context-menu, doesn't have the ghost
  failure mode.
- **QuickLook Extension** for previews.
- **mdimport plugin** for Spotlight metadata.

## Acceptance tests

| # | Test | Pass criterion |
|---|---|---|
| 5.1 | Right-click "Pin for offline" works in any app | Available in Finder, in NLE file pickers, in Quick Look. Triggers pin via NSServices. |
| 5.2 | Badge overlays show pin state | Pinned-ready files show a green dot; pinning-in-progress shows a spinner; failed shows red. Refreshes on state change. |
| 5.3 | Quick Look proxy preview | For codecs where JuiceMount has a pre-generated proxy (future tier 6), Quick Look uses the proxy not the full file. |
| 5.4 | Spotlight searches our metadata | Cmd-Space → type filename → JuiceMount results appear with file path, size, type. |
| 5.5 | No FileProvider registration | `pluginkit -m -p com.apple.fileprovider-extension` does not list JuiceMount on a clean install. Ever. |

## Feature backlog

### 5.A — Services menu items

`Info.plist` registers `NSServices` entries:

- "Pin for Offline" — accepts file paths. Triggers `POST /pin` on
  the admin API.
- "Unpin" — accepts file paths. `POST /unpin`.
- "Copy JuiceMount share link" — emits a `juicemount://...` URL
  that another team member can paste into their wizard step 2.
- "Show in JuiceMount" — opens the popover focused on this file's
  pin status / project.

These appear in the right-click menu of any app that supports
NSServices. No installation, no permission grant.

### 5.B — Finder Sync Extension for badges

A small extension target inside the app bundle. Registers on app
launch. Subscribes to a path prefix (e.g., `/Volumes/zpool-dev`).

Badge logic:

- Pinned + cached → green check
- Pinned, fetch in flight → spinner
- Pinned, fetch failed → red X
- Not pinned, fully cached → blue dot (subtle)
- Not pinned, partially cached → half-blue
- Online not cached → no badge
- Offline + cached → green
- Offline + not cached → red triangle

State source: WebSocket to the admin API. The extension subscribes
to pin-state-change events, updates badges as they arrive.

Per the postmortem in `docs/no-fileprovider.md`: FinderSync
extensions are SEPARATE from FileProviderExtensions. The Info.plist
declares `NSExtensionPointIdentifier = com.apple.FinderSync`, NOT
`com.apple.fileprovider-nonui`. The build-time guard in
`scripts/build-app.sh` rejects any `Contents/PlugIns/` that registers
the FP extension point — FinderSync passes.

### 5.C — QuickLook Preview Extension

`QLPreviewExtension` for video codecs. Initial scope: video files
get a thumbnail and a small video preview (1080p downscale of the
first 5 s).

Source: JuiceMount serves a proxy if it exists (tier 6 generates
proxies). Otherwise the extension reads from FUSE directly with a
hard 1 s budget — past that, falls back to system QuickLook (which
will hit the NFS path and may be slow).

### 5.D — Spotlight `mdimport` plugin

The metadata store already has every file's path, size, mtime,
inode. Expose them to Spotlight via a `mdimporter` bundle.

This means Spotlight (Cmd-Space) finds files on the JuiceMount
volume even when Finder hasn't walked them. Adds attributes:

- `kMDItemDisplayName` (filename)
- `kMDItemPhysicalSize` (size on backend)
- `kMDItemContentModificationDate` (mtime)
- `kMDItemKeywords` (tier 6 content tags, when available)

The Cmd-Shift-F search window we already have stays — it's faster
and JuiceMount-scoped. The mdimport plugin is for users who default
to Spotlight.

### 5.E — Drag-and-drop pinning

`NSDraggingDestination` on the popover root. User drags a folder
from Finder into the popover → it becomes a pin root.

## Anti-patterns

- **No FileProviderExtension. Ever.** Repeating because it's that
  important.
- **No deep Finder integration via private APIs.** The Services /
  FinderSync / QuickLook / mdimport surfaces are all public,
  documented, and stable.
- **No badge overlay that blocks Finder.** The extension must NOT
  do network I/O on the Finder main thread. State subscription is
  on a background queue; badges update from a published state.
- **No "JuiceMount" branding in path names visible to NLEs.** The
  mount is `/Volumes/<user-named>`, never `/Volumes/JuiceMount-Inc-Stuff`.

## Dependencies

- Tier 4's offline state powers the badge color logic (online/offline
  distinction visible at the file level).
- Tier 6's `.juiceproject` schema is consumed by 5.A's "Copy JuiceMount
  share link" — share links should reference the project.
- Tier 2's installer flow needs to handle FinderSync registration on
  first launch (system prompt for enable permission).
