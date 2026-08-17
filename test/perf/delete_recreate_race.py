#!/usr/bin/env python3
"""delete_recreate_race.py — does a directory vanish after delete-then-recreate?

THE BUG. Copies into a freshly recreated directory intermittently fail with
ENOENT on the DESTINATION, on a directory that makedirs() just created
successfully. Reproduced 2026-08-17 on two independent harnesses:
  smallfile_write_bench  80 and 79 files lost in separate runs
  mixed_drain_bench      9, 145 and 56 files lost, in 3 of 6 runs
The failing names include files directly in the destination root
(media001.bin, small0106.bin), so it is the destination DIRECTORY that is
disappearing, not individual files.

This is silent data loss in Finder terms: a copy drops files without an obvious
error. It is also the shape of the documented workaround for the Finder
collision stall — delete the destination, recreate it, copy in — so users hit
this deliberately.

WHAT THIS ISOLATES. The full benches confound the race with 1500-file copies.
Here each trial is exactly: populate N files, rmtree, makedirs, immediately
create one probe file, verify it exists. N is the knob, because rmtree duration
(how many deletes are still settling when the recreate lands) is the suspected
driver — the earlier 300-file attempt did NOT reproduce, the 1540-file runs did.

NOT a server-side reconcile prune: the metadata sync during a failing window
reported pruned:0, and MkdirAll marks new dirs LocalOnly specifically to spare
them from the prune.
"""

import argparse
import json
import os
import shutil
import sys
import time
import urllib.request

CONTROL = os.environ.get("JM_CONTROL", "http://127.0.0.1:11050")


def rpc_counts():
    try:
        with urllib.request.urlopen(CONTROL + "/metrics", timeout=5) as r:
            q = json.load(r).get("rpcs") or {}
        return {k: (q.get(k) or {}).get("count", 0)
                for k in ("REMOVE", "RMDIR", "MKDIR", "CREATE", "LOOKUP")}
    except Exception:
        return {}


def populate(d, n, size_kb):
    """Returns (written, first_error). The directory can vanish MID-populate —
    observed at file 615 of 800, seconds and hundreds of files after the
    rmtree/makedirs that preceded it — so this must report rather than raise."""
    os.makedirs(d, exist_ok=True)
    blob = b"z" * (size_kb * 1024)
    for i in range(n):
        try:
            with open(os.path.join(d, "f%05d.bin" % i), "wb") as f:
                f.write(blob)
        except OSError as e:
            return i, "%s (at file %d of %d)" % (e, i, n)
    return n, None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mount", default="/Volumes/zpool")
    ap.add_argument("--files", type=int, default=800,
                    help="files deleted per trial; the suspected driver")
    ap.add_argument("--size-kb", type=int, default=4)
    ap.add_argument("--trials", type=int, default=12)
    ap.add_argument("--settle", type=float, default=0.0,
                    help="seconds to wait between rmtree and makedirs; the "
                         "candidate mitigation — if a delay fixes it, the race "
                         "is a deferred delete landing on the new directory")
    a = ap.parse_args()

    root = os.path.join(a.mount, ".jm-recreate-probe")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    d = os.path.join(root, "target")

    failures, details = 0, []
    for t in range(a.trials):
        wrote, perr = populate(d, a.files, a.size_kb)
        before = rpc_counts()
        if perr:
            failures += 1
            details.append({"trial": t + 1, "phase": "populate", "error": perr})
            print("  trial %2d/%d  FAIL during POPULATE after %d files: %s"
                  % (t + 1, a.trials, wrote, perr[:90]))
            continue
        shutil.rmtree(d, ignore_errors=True)
        if a.settle:
            time.sleep(a.settle)
        os.makedirs(d, exist_ok=True)

        # The probe: create immediately, then verify from a fresh listing.
        err = None
        try:
            with open(os.path.join(d, "probe.bin"), "wb") as f:
                f.write(b"p" * 4096)
        except OSError as e:
            err = str(e)
        present = os.path.isdir(d)
        listed = "probe.bin" in (os.listdir(d) if present else [])
        after = rpc_counts()

        ok = err is None and present and listed
        if not ok:
            failures += 1
            details.append({"trial": t + 1, "error": err, "dir_exists": present,
                            "probe_listed": listed,
                            "rpc_delta": {k: after.get(k, 0) - before.get(k, 0)
                                          for k in before}})
        print("  trial %2d/%d  %s  dir_exists=%-5s probe_listed=%-5s%s"
              % (t + 1, a.trials, "FAIL" if not ok else "ok ",
                 present, listed, ("   " + err[:70]) if err else ""))

    shutil.rmtree(root, ignore_errors=True)
    print("\n  %d/%d trials lost the directory (files=%d, settle=%.1fs)"
          % (failures, a.trials, a.files, a.settle))
    for x in details[:4]:
        if "rpc_delta" in x:
            print("    trial %d: %s" % (x["trial"], json.dumps(x["rpc_delta"])))
        else:
            print("    trial %d [%s]: %s" % (x["trial"], x.get("phase", "?"),
                                             (x.get("error") or "")[:110]))
    if failures == 0:
        print("  NOT a clearance — this shape/scale did not trigger it. The full "
              "benches hit it at 1540 files with a concurrent copy in flight.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
