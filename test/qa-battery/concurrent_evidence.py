#!/usr/bin/env python3
"""Bounded /spool sampling and offline category05 interleaved-progress proof.

Capture runs only while the category's storm flag exists. Verify never makes
network requests; it requires two exact destination families growing between
the same consecutive samples while their original AppleEvents are outstanding
and read/listing observations fall inside that interval. This does not measure
simultaneous CPU execution or independent backend persistence.
"""
import json
from pathlib import Path
import sys
import time
from urllib.request import urlopen


MAX_SAMPLES = 3000


def families(root):
    result = {}
    seen = set()
    mount = (root / "mount.path").read_text().strip().rstrip("/")
    count = int((root / "parallel.count").read_text().strip())
    if not mount.startswith("/") or mount == "/":
        raise ValueError("invalid mount binding")
    if not 3 <= count <= 4:
        raise ValueError("copy count outside 3–4")
    for i in range(1, count + 1):
        dest = (root / f"dest{i}.path").read_text().strip()
        if not dest.startswith(mount + "/") or dest in seen:
            raise ValueError("invalid or duplicate destination")
        seen.add(dest)
        # /spool exposes r.NFSPath (mount-relative, no leading slash).
        result[i] = dest[len(mount):].strip("/") + f"/tree{i}/"
    return result


def capture(url, root, flag):
    prefixes = families(root)
    output = root / "spool.samples"
    for _ in range(MAX_SAMPLES):
        if not flag.exists():
            return 0
        started = time.time()
        rows = {}
        try:
            with urlopen(url, timeout=2) as response:
                body = json.load(response)
            if body.get("enabled") is not True or body.get("error"):
                raise ValueError("spool unavailable")
            for entry in body.get("entries", []):
                path, size = entry.get("path"), entry.get("size")
                if (isinstance(path, str) and type(size) is int and size >= 0
                        and entry.get("drain_state") == "writing"
                        and any(path.startswith(p) for p in prefixes.values())):
                    rows[path] = size
            record = {"from": started, "to": time.time(), "rows": rows}
        except (OSError, ValueError, TypeError, KeyError) as exc:
            record = {"from": started, "to": time.time(), "error": type(exc).__name__}
        with output.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, separators=(",", ":")) + "\n")
        time.sleep(0.25)
    # A bounded observer limit is not a positive result. The original category
    # will join this child, then fail its overlap gate if proof was not captured.
    return 0


def event_times(root, i):
    pid, started = (root / f"copy{i}.worker").read_text().split()
    if not pid.isdigit():
        raise ValueError("invalid worker identity")
    replied = float((root / f"copy{i}.reply_at").read_text().strip())
    start = float(started)
    if replied <= start:
        raise ValueError("invalid worker interval")
    return start, replied


def sample_times(path):
    result = []
    for line in path.read_text().splitlines():
        parts = line.split(" ", 2)
        if len(parts) != 3 or not parts[2].strip():
            raise ValueError("malformed observer sample")
        start, end = float(parts[0]), float(parts[1])
        if not start <= end:
            raise ValueError("invalid observer interval")
        result.append((start, end))
    return result


def verify(root):
    prefixes = families(root)
    spans = {i: event_times(root, i) for i in prefixes}
    read = sample_times(root / "readback.samples")
    nav = sample_times(root / "nav.samples")
    if not read or not nav:
        raise ValueError("missing read or listing coverage")
    snapshots = [json.loads(line) for line in (root / "spool.samples").read_text().splitlines()]
    for before, after in zip(snapshots, snapshots[1:]):
        if "error" in before or "error" in after:
            continue
        t0, t1 = before["from"], after["to"]
        if not (type(t0) in (int, float) and type(t1) in (int, float) and t0 < t1):
            continue
        if before["to"] > after["from"]:
            continue
        if not any(start <= t1 and end >= t0 for start, end in read) or not any(
                start <= t1 and end >= t0 for start, end in nav):
            continue
        grew = []
        for i, prefix in prefixes.items():
            start, replied = spans[i]
            if not (start <= t0 < t1 < replied):
                continue
            if any(path.startswith(prefix) and type(size) is int and
                   type(after["rows"].get(path)) is int and after["rows"][path] > size
                   for path, size in before["rows"].items()):
                grew.append(i)
        if len(grew) >= 2:
            return {"families": grew, "from": t0, "to": t1,
                    "claim": "interleaved destination progress, not CPU simultaneity"}
    raise ValueError("no two-family progress with overlapping original requests and read/list samples")


def main(argv):
    if len(argv) == 4 and argv[1] == "capture":
        return capture(argv[2], Path(argv[3]), Path(argv[3]) / "storm.flag")
    if len(argv) == 3 and argv[1] == "verify":
        try:
            print(json.dumps(verify(Path(argv[2])), sort_keys=True))
            return 0
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            print(f"INCONCLUSIVE category05 overlap: {exc}", file=sys.stderr)
            return 1
    print("usage: concurrent_evidence.py capture URL CC_TMP | verify CC_TMP", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
