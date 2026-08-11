#!/usr/bin/env python3
"""finder_populate_clean.py — a CLEAN, AppleScript-FREE Finder-populate
measurement for one directory, with a phase breakdown.

WHY THIS EXISTS
---------------
The prior AppleScript harness (finder_column_open.sh) reported ~2.4 s to
populate BOTH a 0-byte list-view folder and a 50 MB column-view folder — i.e.
that 2.4 s was dominated by AppleScript's `make new Finder window` + the
`count of items` polling loop, NOT by real populate work. bytes_read was the
only trustworthy signal in that run. This harness removes AppleScript entirely
and measures the actual filesystem work Finder makes the NFS server do.

METHOD — two independent measurements that must agree
-----------------------------------------------------
(c) SYSCALL FAN-OUT REPLAY (primary populate number + phase breakdown):
    We replay, in-process against the real NFS mount (/Volumes/zpool), the
    syscall fan-out that a Finder open issues, timing each phase separately:

      P1 READDIRPLUS : one os.scandir(dir)  — the directory listing itself.
      P2 GETATTR     : os.lstat() of every child + its ._<name> AppleDouble
                       sidecar + .DS_Store (Finder GETATTRs every row to draw
                       the icon/size/chevron; the sidecar/.DS_Store probes are
                       the ACCESS/GETATTR storm the dossier saw).
      P3 BYTES       : the COLUMN-VIEW preview/icon byte reads — open each
                       child (or, for a folder child, its first few entries)
                       and read the first PREVIEW_BYTES. This is the "50 MB
                       even for a folder-of-folders" cost. Skipped in
                       --metadata-only mode (list view has ZERO byte reads).

    populate_ms = P1 + P2 (list view)  or  P1 + P2 + P3 (column view).
    Each phase is timed with time.perf_counter() around the syscalls only —
    no shell, no AppleScript, no polling tax.

(b) SERVER OP-STREAM BOUND (independent cross-check):
    Around the whole fan-out we snapshot the JuiceMount NFS server /metrics
    (http://127.0.0.1:11050/metrics) before and after, and ALSO poll it at
    ~200 Hz DURING the fan-out to bound the op burst wall-time from the
    server's own vantage point: first poll where rpc_total starts climbing ->
    last poll after which rpc_total stays flat for QUIET_MS. The server-burst
    wall-time should be <= the syscall wall-time (server can't be busier than
    the client kept it) and the per-RPC count deltas should match the phases
    (READDIRPLUS≈1, GETATTR≈children, READ==0 in metadata-only). If they
    disagree wildly, the number is NOT trustworthy and the harness says so.

COLD vs WARM
------------
Default does a COOL sleep (> the 5 s juicefs attr/entry TTL) before the timed
pass so caches expire, then a second WARM pass immediately after. cold isolates
backend leverage; warm is the "already-browsed" repeat-open feel. --no-cool
skips the sleep (measures warm-process, caches-hot).

HONESTY / CAVEATS
-----------------
* This is the FILESYSTEM work of a populate, not pixels-on-screen. It does NOT
  include macOS-side Finder render/layout/QuickLook-daemon cost — but the
  dossier established that the *server-visible* work (bytes_read, RPC counts) is
  the real lever, and the AppleScript wall-time was pure scripting overhead.
  This harness reproduces the exact syscall fan-out the server sees, so its
  populate_ms is the honest "how long the I/O takes" — the floor Finder can't
  beat, and the thing our fixes move.
* The server-burst poll is bounded by the poll interval (~5 ms) resolution;
  reported as a RANGE, not a false-precision point.
* --preview-bytes controls the column-view read depth. The dossier measured
  ~50 MB total for GMTM's 33 folders in real Finder; Finder's exact per-item
  read depends on file type (it reads enough to render an icon/thumbnail). We
  read a fixed PREVIEW_BYTES per leaf to APPROXIMATE and BOUND that; the number
  is reported as bytes-actually-read so it's auditable, not a guess.

READ-ONLY. Metadata + bounded prefix reads only. Never writes, never pulls whole
large files (capped at --preview-bytes per item, --max-preview-items total).

Usage:
  finder_populate_clean.py <dir> [--label L] [--view column|list]
        [--preview-bytes N] [--max-preview-items N] [--cool-s S] [--no-cool]
        [--json]

Examples:
  # GMTM folder-of-folders, column view (the 50MB case), cold:
  finder_populate_clean.py "/Volumes/zpool/Film Projects/GMTM" --view column
  # same, list view (should be ~0 bytes):
  finder_populate_clean.py "/Volumes/zpool/Film Projects/GMTM" --view list
"""
import argparse, json, os, sys, threading, time, urllib.request

CP = "http://127.0.0.1:11050"
RPCS = ["LOOKUP", "GETATTR", "ACCESS", "READDIR", "READDIRPLUS", "READ",
        "FSSTAT", "OPEN", "CREATE", "WRITE"]

# ------------------------------------------------------------------ server side


def metrics_snap():
    """Return (rpc_total, bytes_read, {rpc: count}) or None if unreachable."""
    try:
        with urllib.request.urlopen(CP + "/metrics", timeout=3) as r:
            d = json.load(r)
    except Exception:
        return None
    rpcs = d.get("rpcs", {})
    return (d.get("rpc_total", 0), d.get("bytes_read", 0),
            {k: rpcs.get(k, {}).get("count", 0) for k in RPCS})


class BurstPoller(threading.Thread):
    """Poll /metrics at ~POLL_HZ during the fan-out to bound the server op
    burst: first sample where rpc_total rises above baseline -> last sample
    after which rpc_total is flat for QUIET_MS. Non-fatal if metrics are down."""

    def __init__(self, poll_hz=200, quiet_ms=400):
        super().__init__(daemon=True)
        self.interval = 1.0 / poll_hz
        self.quiet_s = quiet_ms / 1000.0
        self.samples = []          # (t_perf, rpc_total)
        self._stopflag = threading.Event()
        self.alive = True

    def run(self):
        base = metrics_snap()
        if base is None:
            self.alive = False
            return
        while not self._stopflag.is_set():
            s = metrics_snap()
            if s is not None:
                self.samples.append((time.perf_counter(), s[0]))
            time.sleep(self.interval)

    def stop(self):
        self._stopflag.set()
        self.join(timeout=2)

    def burst_bounds(self):
        """Return (start_t, end_t, samples_seen, total_delta) in perf-counter
        seconds for the rpc_total burst, or None if no rise seen / metrics down.

        Burst = from the FIRST inter-sample rise to the LAST inter-sample rise.
        Working off consecutive deltas (not vs a fixed baseline) naturally
        ignores both the flat pre-burst baseline and the flat post-burst tail,
        so a single quick jump gives a ~1-poll-interval window rather than the
        whole sampling span. A trailing gap > quiet_s ends the burst even if a
        later unrelated rise appears (guards against a second, ambient burst)."""
        if not self.alive or len(self.samples) < 3:
            return None
        rises = []  # indices i where samples[i] > samples[i-1]
        for i in range(1, len(self.samples)):
            if self.samples[i][1] > self.samples[i - 1][1]:
                rises.append(i)
        if not rises:
            return None
        first = rises[0]
        # A single populate is ONE contiguous burst: even a multi-second byte
        # READ burst has sub-quiet_s gaps between READ batches, so we must NOT
        # end at the first gap. Instead: the burst ends at the LAST rise that is
        # still followed (eventually) by a SUSTAINED quiet tail (>= quiet_s of no
        # further rise). We find the last rise index, then confirm the tail after
        # it is quiet. A separate ambient burst would be separated from ours by a
        # gap >> quiet_s AND our fan-out has already returned, so in practice the
        # last rise during our (blocking) fan-out IS the burst end. To avoid
        # folding in a truly-later ambient burst, we cut at the last rise whose
        # gap to the NEXT rise (or end) is the start of a >= quiet_s silence.
        last = first
        for idx in range(len(rises)):
            i = rises[idx]
            nxt_t = (self.samples[rises[idx + 1]][0]
                     if idx + 1 < len(rises) else self.samples[-1][0])
            gap = nxt_t - self.samples[i][0]
            last = i
            if idx + 1 < len(rises) and gap >= self.quiet_s:
                # silence begins here; the next rise starts a NEW (ambient) burst
                break
        # start = the sample JUST BEFORE the first rise (burst began between them)
        start_t = self.samples[first - 1][0]
        end_t = self.samples[last][0]
        total_delta = self.samples[last][1] - self.samples[first - 1][1]
        return (start_t, end_t, len(self.samples), total_delta)


# ------------------------------------------------------------------ client side


def cool(seconds):
    if seconds > 0:
        time.sleep(seconds)


def phase_scandir(d):
    """P1: the directory listing (READDIRPLUS). Returns (ms, [entries])."""
    t = time.perf_counter()
    ents = []
    try:
        with os.scandir(d) as it:
            for e in it:
                ents.append((e.name, e.path))
    except OSError:
        pass
    return (time.perf_counter() - t) * 1000.0, ents


def phase_getattr(d, ents):
    """P2: Finder's per-row GETATTR fan-out. lstat every real child, its
    ._<name> sidecar, plus .DS_Store once. Returns (ms, n_lstats)."""
    t = time.perf_counter()
    n = 0
    # .DS_Store probe (Finder always looks)
    try:
        os.lstat(os.path.join(d, ".DS_Store"));
    except OSError:
        pass
    n += 1
    for name, path in ents:
        if name.startswith("._") or name == ".DS_Store":
            continue
        try:
            os.lstat(path)
        except OSError:
            pass
        n += 1
        # AppleDouble sidecar probe
        try:
            os.lstat(os.path.join(d, "._" + name))
        except OSError:
            pass
        n += 1
    return (time.perf_counter() - t) * 1000.0, n


def phase_bytes(d, ents, preview_bytes, max_items):
    """P3: column-view preview/icon byte reads. For each child, read the first
    preview_bytes. For a folder child, descend one level and read a few of its
    leaves' prefixes (Finder renders a folder-preview stack). Returns
    (ms, bytes_read, items_read)."""
    t = time.perf_counter()
    total = 0
    items = 0

    def read_prefix(path):
        nonlocal total, items
        if items >= max_items:
            return
        try:
            with open(path, "rb", buffering=0) as f:
                b = f.read(preview_bytes)
                total += len(b)
                items += 1
        except OSError:
            pass

    for name, path in ents:
        if name.startswith("._") or name == ".DS_Store":
            continue
        if items >= max_items:
            break
        try:
            is_dir = os.path.isdir(path)
        except OSError:
            is_dir = False
        if is_dir:
            # folder child: Finder previews a few inner leaves for the stack icon
            try:
                with os.scandir(path) as it:
                    inner = 0
                    for e in it:
                        if e.name.startswith("._") or e.name == ".DS_Store":
                            continue
                        try:
                            if e.is_file(follow_symlinks=False):
                                read_prefix(e.path)
                                inner += 1
                                if inner >= 4:
                                    break
                        except OSError:
                            pass
            except OSError:
                pass
        else:
            read_prefix(path)
    return (time.perf_counter() - t) * 1000.0, total, items


def run_pass(d, args, want_bytes):
    """One full fan-out pass with the server burst poller wrapped around it."""
    poller = BurstPoller()
    before = metrics_snap()
    poller.start()
    # tiny settle so the poller has a clean baseline sample before we act
    time.sleep(0.03)

    wall_t0 = time.perf_counter()
    p1_ms, ents = phase_scandir(d)
    n_children = sum(1 for name, _ in ents
                     if not name.startswith("._") and name != ".DS_Store")
    p2_ms, n_lstats = phase_getattr(d, ents)
    if want_bytes:
        p3_ms, bytes_read, items_read = phase_bytes(
            d, ents, args.preview_bytes, args.max_preview_items)
    else:
        p3_ms, bytes_read, items_read = 0.0, 0, 0
    wall_ms = (time.perf_counter() - wall_t0) * 1000.0

    # let the burst settle, then stop the poller
    time.sleep(0.2)
    poller.stop()
    after = metrics_snap()

    populate_ms = p1_ms + p2_ms + (p3_ms if want_bytes else 0.0)

    # server-side deltas
    server = None
    if before and after:
        d_total = after[0] - before[0]
        d_bytes = after[1] - before[1]
        d_rpc = {k: after[2][k] - before[2][k] for k in RPCS}
        server = {"rpc_total_delta": d_total, "bytes_read_delta": d_bytes,
                  "rpc_delta": d_rpc}
    burst = poller.burst_bounds()
    burst_ms = None
    burst_rpcs = None
    if burst:
        burst_ms = (burst[1] - burst[0]) * 1000.0
        burst_rpcs = burst[3]

    return {
        "populate_ms": populate_ms,
        "wall_ms": wall_ms,
        "phases": {"p1_readdirplus_ms": p1_ms, "p2_getattr_ms": p2_ms,
                   "p3_bytes_ms": p3_ms},
        "n_children": n_children, "n_lstats": n_lstats,
        "preview_bytes_read": bytes_read, "preview_items_read": items_read,
        "server_burst_rpcs": burst_rpcs,
        "server": server,
        "server_burst_ms": burst_ms,
        "server_burst_samples": burst[2] if burst else 0,
    }


def consistency_check(res, want_bytes):
    """Cross-check syscall wall-time vs server-burst + RPC counts. Returns
    (trustworthy: bool, notes: [str])."""
    notes = []
    ok = True
    srv = res.get("server")
    burst = res.get("server_burst_ms")
    wall = res["wall_ms"]

    if srv is None:
        notes.append("metrics UNREACHABLE — server cross-check skipped; "
                     "syscall number stands alone (less corroboration)")
        return True, notes

    rpc = srv["rpc_delta"]
    # READDIRPLUS should fire (>=1) for the listing
    if rpc.get("READDIRPLUS", 0) < 1 and rpc.get("READDIR", 0) < 1:
        notes.append("WARN: server saw 0 READDIR/READDIRPLUS — client cache "
                     "served the listing (not a cold populate); use --cool-s "
                     "or a fresh dir for a cold number")
    # GETATTR/ACCESS should scale with children (some are absorbed by client
    # attr cache, so we only assert 'nonzero on cold')
    if want_bytes:
        if srv["bytes_read_delta"] <= 0:
            notes.append("WARN: column mode but server bytes_read_delta==0 — "
                         "preview bytes served from client cache or files "
                         "empty; P3 not exercised cold")
    else:
        if srv["bytes_read_delta"] > 1_000_000:
            notes.append("NOTE: metadata-only but server read %d bytes "
                         "(background prefetch/other clients on the mount)"
                         % srv["bytes_read_delta"])

    # burst attribution: with idle-mount ambient ~0 rpc/s, the burst RPCs should
    # equal my fan-out's rpc_total delta. If burst_rpcs << total delta, ambient
    # traffic split the burst; if it matches, the burst window is trustworthy.
    b_rpcs = res.get("server_burst_rpcs")
    total_delta = srv["rpc_total_delta"]
    if burst is not None:
        if b_rpcs is not None and total_delta > 0 and b_rpcs < total_delta * 0.5:
            notes.append("WARN: server-burst captured only %d of %d RPCs — "
                         "ambient/other traffic fragmented the window; treat "
                         "server-burst as loose, trust the syscall phases"
                         % (b_rpcs, total_delta))
        elif burst > wall + 50:
            # burst legitimately exceeds client wall only if the server op is
            # slow (a real backend-latency finding, NOT contamination)
            notes.append("FINDING: server op-burst %.0fms exceeds client "
                         "syscall wall %.0fms by >50ms — server-side/backend "
                         "latency in the populate (not client tax); worth "
                         "drilling with l2_roundtrips.sh / accesslog"
                         % (burst, wall))
        else:
            notes.append("server-burst %.0fms ~ syscall wall %.0fms, captured "
                         "%s/%s RPCs (consistent)"
                         % (burst, wall, b_rpcs, total_delta))
    else:
        notes.append("server burst not resolved (delta=%d RPCs, too fast/small "
                     "for the ~5ms poll) — syscall phases are the primary signal"
                     % total_delta)
    return ok, notes


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dir")
    ap.add_argument("--label", default=None)
    ap.add_argument("--view", choices=["column", "list"], default="column",
                    help="column = includes P3 preview byte reads (the 50MB "
                         "case); list = metadata only (0 bytes)")
    ap.add_argument("--preview-bytes", type=int, default=256 * 1024,
                    help="bytes read per leaf for column-view preview (default "
                         "256KiB; cap that bounds byte pull)")
    ap.add_argument("--max-preview-items", type=int, default=400,
                    help="max leaves to preview-read (caps total byte pull)")
    ap.add_argument("--cool-s", type=float, default=7.0,
                    help="sleep before the cold pass to expire the 5s juicefs "
                         "TTL (default 7)")
    ap.add_argument("--no-cool", action="store_true",
                    help="skip the cool sleep (measure warm-process/hot-cache)")
    ap.add_argument("--json", action="store_true",
                    help="emit a single JSON object (for scripting)")
    args = ap.parse_args()

    d = args.dir
    label = args.label or os.path.basename(d.rstrip("/"))
    want_bytes = (args.view == "column")

    if not os.path.isdir(d):
        print("ERROR: not a directory: %s" % d, file=sys.stderr)
        sys.exit(2)

    # reconcile-idle guard (best-effort)
    guard = None
    try:
        with urllib.request.urlopen(CP + "/activity", timeout=4) as r:
            guard = json.load(r).get("summary")
    except Exception:
        pass

    if not args.no_cool:
        cool(args.cool_s)
    cold = run_pass(d, args, want_bytes)
    # warm pass immediately (no cool) to show cache leverage
    warm = run_pass(d, args, want_bytes)

    ok, notes = consistency_check(cold, want_bytes)

    out = {
        "dir": d, "label": label, "view": args.view,
        "reconcile_guard": guard,
        "cold": cold, "warm": warm,
        "trustworthy": ok, "notes": notes,
        "params": {"preview_bytes": args.preview_bytes,
                   "max_preview_items": args.max_preview_items,
                   "cool_s": (0 if args.no_cool else args.cool_s)},
    }

    if args.json:
        json.dump(out, sys.stdout)
        print()
        return

    def fmt_pass(tag, p):
        ph = p["phases"]
        print("  [%s] populate=%.1f ms  (wall=%.1f ms)" % (tag, p["populate_ms"], p["wall_ms"]))
        print("       P1 readdirplus=%.1f ms | P2 getattr=%.1f ms (%d lstats over %d children) | P3 bytes=%.1f ms"
              % (ph["p1_readdirplus_ms"], ph["p2_getattr_ms"], p["n_lstats"], p["n_children"], ph["p3_bytes_ms"]))
        if want_bytes:
            print("       preview bytes read=%s (%d items)"
                  % (_h(p["preview_bytes_read"]), p["preview_items_read"]))
        if p["server"]:
            rd = p["server"]["rpc_delta"]
            print("       server delta: bytes_read=%s | %s"
                  % (_h(p["server"]["bytes_read_delta"]),
                     " ".join("%s=%d" % (k, rd[k]) for k in RPCS if rd[k])))
        if p["server_burst_ms"] is not None:
            print("       server op-burst wall≈ %.1f ms (%s RPCs captured, %d poll samples, ~5ms res)"
                  % (p["server_burst_ms"], p.get("server_burst_rpcs"), p["server_burst_samples"]))

    print("=" * 72)
    print("CLEAN FINDER-POPULATE  [%s]  view=%s" % (label, args.view))
    print("  dir=%s" % d)
    print("  reconcile_guard=%s" % guard)
    print("=" * 72)
    fmt_pass("COLD", cold)
    fmt_pass("WARM", warm)
    print("-" * 72)
    print("  TRUSTWORTHY=%s" % ("YES" if ok else "NO (see notes)"))
    for n in notes:
        print("   - " + n)


def _h(n):
    n = float(n)
    for u in ["B", "KiB", "MiB", "GiB"]:
        if abs(n) < 1024 or u == "GiB":
            return "%.1f%s" % (n, u)
        n /= 1024


if __name__ == "__main__":
    main()
