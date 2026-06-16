# Usability issues

UX-level observations (not crashes/hangs) that hurt the experience, esp. on WAN.

- **Offline/disconnected UI lag (#30):** after an outage→reconnect, the menu-bar app
  still shows "offline/disconnected" though the Go core is back (last sync ~1 s, spool
  draining). Long-standing UI-only reporting bug; data path fine.
- **No "rebuilding index" progress (#37):** post-remount settling looks broken.
- **"Disk is full" wording (graceful-stall era):** offline buffer full reads as the whole
  Mac disk being full. Now exposed as `offline_buffer_full` on `/spool` for the app to
  surface "Offline buffer full — reconnect to drain" (#24, shipped core side).
- **Transient wrong file size (#38):** mid/post-slow-drain, `stat` briefly reports a
  partial size (saw 7 MB for a 100 MB file) before settling; data is complete. Could
  confuse Finder/tools that gate on size.
- **Drain durability invisibility:** a file "copied" locally may still be uploading for
  hours over a slow link. Need a clear "N files / N GB still uploading" surface so users
  don't yank the drive / sleep before it's durable on the backend.
- **Remote read = pin-first:** un-pinned cold reads over WAN are slow + fragile. The
  product story must steer users to pin/prefetch a project before working remote.

## Open
- Per-link UX mode? (e.g., detect cellular → show a "metered link" banner, throttle
  background drain, suggest pinning.)
