# Overnight stability audit — 2026-05-13

User left an instruction at ~02:30 to run an autonomous loop overnight
auditing JuiceMount for hang/crash/perf vulnerabilities. They need the
mount working tomorrow morning for files only accessible via JuiceMount.

This doc is the running journal. Each loop iteration appends a section
with: what was investigated, what was found, what was fixed (or
documented as "user must verify"). The user will read this first thing
in the morning.

## Morning recovery checklist (READ THIS FIRST)

The mount is currently WEDGED in the kernel. To launch fresh:

```bash
# 1. From a fresh Terminal (not one of last night's hung shells):
sudo umount -f -t nfs /Volumes/zpool
# If that hangs, just reboot — fastest path.

# 2. Confirm clean:
mount | grep -iE "juicefs|zpool"   # should be empty
pgrep -lf "juicefs|JuiceMount"     # should be empty

# 3. Launch the new build:
open /Users/LelandDutcher/Developer/JuiceMount6/build/JuiceMount.app
```

Last good commit before overnight loop: `1121bae` —
"fix(stability): tighter NFS timeouts + Force Eject + ordered unmount".
That commit's NFS mount opts (`timeo=10,retrans=2`) mean a wedged mount
takes 3 seconds to fail per stat, not 150 seconds. So even if something
goes sideways tomorrow, Finder will be annoyed for 3 s, not catatonic.

## Working hypotheses for tonight's audit

1. **Auto-self-test on startup deadlocks** when our Go process reads
   from `/Volumes/zpool` (localhost NFS) — the read goes through macOS
   NFS client → back to our NFS server → our handler tries to look up
   the same path in the metadata store that `pickSelfTestTarget` was
   walking. Recursive lock or lock contention is likely.

2. **`.juicemount-selftest.tmp` from a crashed prior run** poisons the
   next run — its blocks may be in Redis (metadata) but not in MinIO
   (data). Reading triggers JuiceFS to retry MinIO forever.

3. **NFS handler may hold locks on the metadata store** while serving
   reads, preventing internal Go code (prefetcher, self-test) from
   making progress.

4. **fdpool** may have stale entries from prior runs leading to EIO on
   reuse paths.

5. **Pre-mount conflict probe (Phase A1)** may not catch the case where
   the previous JuiceMount left a `127.0.0.1`-source mount table entry
   with no server behind it. We treat `127.0.0.1` as "ours, reuse" but
   reuse may inherit a wedged state.

## Loop iteration log
