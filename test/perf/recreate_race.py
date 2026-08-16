#!/usr/bin/env python3
"""Does deleting a directory and immediately recreating it lose files?

WHY THIS EXISTS. Across ~25 small-file bench runs, several lost files to ENOENT
on directories the harness had ALREADY created — 80 files in one run, 79 in
another, single files in others. copyfile got ENOENT on the destination, which
means the parent directory was gone at open() time despite makedirs() having
succeeded moments earlier.

THE HYPOTHESIS: the bench deletes the previous run's tree and immediately
recreates the same paths. If a deferred delete of the OLD directory lands after
the NEW one is created, it removes the new one and its in-flight children.

WHY IT MATTERS BEYOND THE HARNESS: "delete the destination folder, recreate it,
copy into it" is a real user workflow — it is in fact the documented workaround
for the Finder collision stall, so the founder does exactly this.

CONTROLLED: arm A reuses ONE path, deleting and recreating it every iteration.
Arm B uses a FRESH path every iteration and never deletes. Same file count, same
concurrency, same mount. If A fails and B does not, delete/recreate is the cause
and this is not a generic write-path flake.
"""
import os, shutil, sys, threading, time

MOUNT = "/Volumes/zpool"
ROOT = os.path.join(MOUNT, ".jm-recreate-race")
N = 300
THREADS = 8
ITERS = 8


def write_tree(dest):
    """Create dest with N files across 10 subdirs, THREADS-way. Returns errors."""
    for i in range(10):
        os.makedirs(os.path.join(dest, "d%d" % i), exist_ok=True)
    blob = b"x" * (32 * 1024)
    errs, lock, idx = [], threading.Lock(), [0]

    def worker():
        while True:
            with lock:
                i = idx[0]
                idx[0] += 1
            if i >= N:
                return
            p = os.path.join(dest, "d%d" % (i % 10), "f%04d.bin" % i)
            try:
                with open(p, "wb") as f:
                    f.write(blob)
            except OSError as e:
                with lock:
                    errs.append("%s: %s" % (p, e))

    ts = [threading.Thread(target=worker, daemon=True) for _ in range(THREADS)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return errs


def arm(name, reuse_path):
    total_err, iters_failed = 0, 0
    for it in range(ITERS):
        dest = (os.path.join(ROOT, "reused") if reuse_path
                else os.path.join(ROOT, "fresh%02d" % it))
        if reuse_path:
            shutil.rmtree(dest, ignore_errors=True)
        os.makedirs(dest, exist_ok=True)
        errs = write_tree(dest)
        # Also check what actually survived on disk.
        # Count REAL files only. The first version of this counted every
        # entry and reported "missing=-310" on a clean run, because macOS had
        # written a ._ AppleDouble sidecar beside all 300 files AND beside each
        # of the 10 directories. That is not data loss; it is the sidecar
        # doubling, and counting it as loss produced a confident false verdict.
        landed = sum(1 for _, _, fs in os.walk(dest) for f in fs
                     if not f.startswith("._"))
        sidecars = sum(1 for _, _, fs in os.walk(dest) for f in fs
                       if f.startswith("._"))
        missing = N - landed
        if errs or missing:
            iters_failed += 1
        total_err += len(errs)
        print("    iter %d/%d  write-errors=%-4d real-files=%-4d sidecars=%-4d missing=%-4d%s"
              % (it + 1, ITERS, len(errs), landed, sidecars, missing,
                 ("   first: " + errs[0][-90:]) if errs else ""))
    print("  %s: %d/%d iterations lost data, %d write errors total"
          % (name, iters_failed, ITERS, total_err))
    return iters_failed, total_err


if not os.path.ismount(MOUNT) and not os.path.isdir(MOUNT):
    print("mount absent")
    sys.exit(3)
shutil.rmtree(ROOT, ignore_errors=True)
os.makedirs(ROOT, exist_ok=True)

print("  ARM A — delete and recreate the SAME path each iteration")
a_fail, a_err = arm("arm A (delete+recreate)", True)
time.sleep(5)
print("\n  ARM B — control: a FRESH path each iteration, never deleted")
b_fail, b_err = arm("arm B (fresh path)", False)

print("\n  VERDICT")
if a_fail and not b_fail:
    print("    delete+recreate loses data; fresh paths do not. The recreate race "
          "is REAL and is not a generic write-path flake.")
elif a_fail and b_fail:
    print("    BOTH arms lose data — this is a general write-path defect, not "
          "specific to delete/recreate.")
elif not a_fail and not b_fail:
    print("    Neither arm reproduced it at this scale. NOT a clearance: the bench "
          "hit it on ~4 of 25 runs, so absence here means this shape/scale did "
          "not trigger it.")
else:
    print("    Only the control failed — the hypothesis is wrong; investigate B.")
shutil.rmtree(ROOT, ignore_errors=True)
