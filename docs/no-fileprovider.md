# Do not add a FileProvider extension to JuiceMount

**Status:** load-bearing. The build script (`scripts/build-app.sh`) enforces this
with a guard that fails the build if `JuiceMount.app/Contents/PlugIns/` exists.

## TL;DR

JuiceMount serves files via NFS (kernel mount at `/Volumes/zpool`) backed by
JuiceFS over FUSE. It does not need a `FileProviderExtension`. Adding one is a
load-bearing mistake — the registration persists in macOS's `fileproviderd`
database **forever**, even after the source project is deleted, and there is no
supported terminal command to remove it. The only known recovery is the
Finder-AppleScript-`delete` trick described at the bottom of this document.

## Postmortem: 2026-05-12

### What happened

Around 11 PM, JuiceMount's NFS mount slowed to "frames take tens of seconds to
load" speeds. Diagnostic timeline:

| Layer | Throughput |
|---|---|
| Raw cache chunk file on disk | 760 GB/s |
| FUSE-direct (`~/.juicemount/fuse-internal/<file>`) | 919–2426 MB/s |
| NFS path (`/Volumes/zpool/<same file>`) | **13 MB/s** |

JuiceMount itself, the JuiceFS cache, and the local SSD were all healthy. The
slowdown was entirely in the kernel-NFS-to-FileProvider coordination layer.
`filecoordinationd` was at 146.8% CPU, `fileproviderd` at 132.5% CPU. Each NFS
RPC was waiting on file-coordination locks that the two daemons held while
trying to consult a dead FileProvider extension.

### Root cause

Approximately a month earlier (Apr 16), the user had opened an older
`JuiceMount` Xcode project and hit Run. That Debug build included a
`FileProviderExtension` target signed with personal-team credentials and
bundle ID `com.lelanddutcher.JuiceMount`. On launch, the extension called
`NSFileProviderManager.add(domain:)` to register a domain named
`JuiceMount-zpool`.

The source project was later deleted. The Xcode DerivedData was deleted.
**The registration persisted.** `fileproviderd` keeps domain registrations
across reboots, daemon restarts, even system updates — the design assumes
cloud-storage data shouldn't disappear when an extension crashes.

For a month the orphan was dormant. Then macOS kicked off a periodic
CloudStorage reconciliation pass, found the orphan, tried to reach the
dead extension, started spinning, and pinned two daemons at 100%+ CPU.

### Why standard recovery paths failed

| Attempt | Outcome |
|---|---|
| Delete the extension binary | Extension already gone — orphan was the registration, not the binary |
| `pluginkit -e ignore com.lelanddutcher.JuiceMount.FileProviderExtension` | Marked disabled but the **domain** was not the plugin — registrations survive `pluginkit ignore` |
| `kill -9 filecoordinationd` / `fileproviderd` | Both respawn under launchd with the orphan still in the DB |
| `launchctl bootout system/com.apple.FileProvider.plist; rm -rf ~/Library/Application Support/FileProvider; bootstrap` | Daemon respawned. Pending operation queue restored from xattrs on `~/Library/CloudStorage/JuiceMount-zpool/` |
| System Settings → Login Items & Extensions → File Provider Extensions | JuiceMount not shown — macOS can't render a UI row for a plugin whose binary is missing |
| `fileproviderctl` (any subcommand) | No `remove-domain` subcommand exists |
| Ad-hoc-signed Swift app with bundle ID `com.lelanddutcher.JuiceMount` calling `NSFileProviderManager.remove()` | `NSFileProviderErrorDomain -2014` — framework verifies team identifier, not bundle ID |
| `rm -rf ~/Library/CloudStorage/JuiceMount-zpool` | "Permission denied" — `dr-x` perms enforced by FileProvider ACLs |
| `xattr -d com.apple.file-provider-domain-id ~/Library/CloudStorage/JuiceMount-zpool` | "Permission denied" — xattr protected too |
| Reboot | Cleared the active retry queue but did not GC the registration |

### What worked

```applescript
tell application "Finder"
    delete POSIX file "/Users/USER/Library/CloudStorage/JuiceMount-zpool"
end tell
```

**Finder has a privileged XPC channel to `fileproviderd`** for exactly this
case — it can issue `removeDomain` on behalf of any user via that channel,
even when the owning extension is unreachable. Manual drag-to-trash from
`~/Library/CloudStorage` in Finder is the user-facing equivalent.

After Finder issued the removal, the trashed copy retained the FP xattr and
`uchg` BSD flag. Cleanup required:

```bash
chflags -R nouchg ~/.Trash/JuiceMount-zpool
chmod -R u+rwx ~/.Trash/JuiceMount-zpool
sudo rm -rf ~/.Trash/JuiceMount-zpool
```

After all of that, `fileproviderctl dump | grep -c "JuiceMount-zpool"` finally
returned 0.

## Prevention

1. **Don't add `FileProviderExtension` to JuiceMount6.** The mount works as
   plain NFS + macFUSE. There is no UX benefit FP would provide that an NFS
   mount doesn't already cover for Resolve/Premiere/Finder.

2. **The build-script guard** in `scripts/build-app.sh` fails the build if
   `JuiceMount.app/Contents/PlugIns/` exists. This catches accidental
   introduction at build time, before any registration can happen.

3. **Don't open old JuiceMount projects in Xcode.** Older project iterations
   (JuiceMount through JuiceMount5) may have had FP targets. If you ever
   resurrect one, **strip the FP extension target before hitting Run**.

4. **If you genuinely need an extension someday:** call
   `NSFileProviderManager.remove(_:completionHandler:)` in the app's
   `applicationWillTerminate` AND in a CLI command available to support, so
   uninstall is graceful. Document the team identifier you sign with so
   support can rebuild a tool with matching credentials to issue cleanup
   calls.

## Recovery playbook (if it ever happens again)

```bash
# 1. confirm the ghost
fileproviderctl dump 2>&1 | grep -B1 -A2 "JuiceMount"

# 2. ask Finder to evict via the privileged XPC channel
osascript -e 'tell application "Finder" to delete POSIX file "/Users/<USER>/Library/CloudStorage/JuiceMount-zpool"'

# 3. clear the trashed copy's protections + remove
chflags -R nouchg ~/.Trash/JuiceMount-zpool 2>/dev/null
chmod  -R u+rwx  ~/.Trash/JuiceMount-zpool 2>/dev/null
sudo rm -rf ~/.Trash/JuiceMount-zpool

# 4. verify
fileproviderctl dump 2>&1 | grep -c "JuiceMount-zpool"    # must be 0
ps -p $(pgrep -x fileproviderd) -o %cpu                   # must settle to 0
ps -p $(pgrep -x filecoordinationd) -o %cpu               # must settle to 0
```
