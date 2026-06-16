# Environmental constraints

Host/OS/infra factors that shape behavior independent of raw bandwidth.

## Backend availability events

- **Redis restart mid-session** (observed 17:51): `connection refused` + `LOADING Redis
  is loading the dataset`. juicefs lost its metadata connection → wedged → app-side
  remount → freshly-created paths got orphaned NFS handles (ENOENT/ESTALE) until the
  reconcile settled. Recovered, no data loss. (#36)
- **Sustained backend-unreachable window** (cellular, 19:30): auto-offline engaged
  correctly; but it killed an in-flight cold read → "connection interrupted" (#39).

## macOS specifics

- **macFUSE is a kernel extension** (io.macfuse 5.1.3) — needs System-Settings approval +
  reboot; tightening kext policy on Apple Silicon is a launch/long-term risk. Consider
  FUSE-T (NFS-based, no kext) for the internal mount.
- **Single TCP connection per NFS mount** (macOS has no `nconnect`). One serve goroutine
  handles ALL RPCs — head-of-line blocking is a real risk (drove the write-admission
  isolation fix). All metadata + data multiplex over one socket → one slow op can stall
  others until the per-RPC subread deadline (2 s) yields.
- **APFS purgeable space / Time Machine local snapshots** distort "free disk," which the
  spool's auto-size + 20 GB floor must account for (saw a 50 GB request clamp to ~36 GB).
- **FDA (Full Disk Access)** must persist — solved via Developer-ID signing (stable
  team-ID requirement) so the grant survives rebuilds.
- **Power assertion** required during transfer or a clamshelled Mac idle-sleeps mid-copy
  ("device disappeared"). Fixed via keep-awake keyed on byte movement.

## Watchdog interaction

- The health watchdog SIGKILLs+remounts juicefs when the process tree is gone or a
  sustained wedge + backend reachable. On a flaky WAN this fires more (juicefs dies on a
  Redis blip). Each remount reassigns inodes → orphans NFS handles (#12). The watchdog
  itself behaves correctly (defers while juicefs is alive — the #13 fix).

## Open questions

- juicefs cache dir location + size on the laptop; eviction behavior under WAN cold reads.
- Should the watchdog be more patient (longer probe) on a measured-slow link? (#16)
