#!/usr/bin/env python3
"""pick_targets.py — enumerate + classify directories under the JuiceMount mount
to pick latency-ladder targets. Labels each dir folder-of-FOLDERS (FF) vs
folder-of-MEDIA (MED) and reports child dir/file/media counts and depth.

Metadata-only, mirror-served walk (os.scandir, never opens bytes). READ-ONLY.

Usage: pick_targets.py [ROOT] [MAX_DEPTH] [MIN_CHILDREN]
Prints a table plus JSON candidate lists on the last lines (parsable).
"""
import os, sys, json

ROOT = sys.argv[1] if len(sys.argv) > 1 else "/Volumes/zpool"
MAX_DEPTH = int(sys.argv[2]) if len(sys.argv) > 2 else 4
MIN_CHILDREN = int(sys.argv[3]) if len(sys.argv) > 3 else 8

MEDIA_EXT = {"mov","mp4","mxf","mkv","wav","aif","aiff","jpg","jpeg","png","tif",
             "tiff","cr2","cr3","arw","dng","r3d","braw","psd","mp3","m4a","gif","heic"}

def classify(d):
    dirs = files = media = 0
    try:
        with os.scandir(d) as it:
            for e in it:
                n = e.name
                if n.startswith("._") or n == ".DS_Store":
                    continue
                try:
                    if e.is_dir(follow_symlinks=False):
                        dirs += 1
                    else:
                        files += 1
                        ext = n.rsplit(".", 1)[-1].lower() if "." in n else ""
                        if ext in MEDIA_EXT:
                            media += 1
                except OSError:
                    pass
    except OSError:
        return None
    return dirs, files, media

rows = []  # (depth, path, dirs, files, media)
def walk(d, depth):
    if depth > MAX_DEPTH:
        return
    c = classify(d)
    if c is None:
        return
    dirs, files, media = c
    total = dirs + files
    if total >= MIN_CHILDREN:
        rows.append((depth, d, dirs, files, media))
    # recurse into subdirs
    try:
        with os.scandir(d) as it:
            subs = [e.path for e in it
                    if e.is_dir(follow_symlinks=False) and not e.name.startswith(".")]
    except OSError:
        subs = []
    for s in subs:
        walk(s, depth + 1)

walk(ROOT, 0)

# FF = folder of folders: dirs dominate, ~no media files
ff = [r for r in rows if r[2] >= 8 and r[4] == 0 and r[3] <= max(2, r[2] // 5)]
# MED = folder of media: media files dominate
med = [r for r in rows if r[4] >= 10 and r[4] >= r[2]]

ff.sort(key=lambda r: (-r[2], -r[0]))   # most subfolders, deepest first
med.sort(key=lambda r: (-r[4], -r[0]))

print(f"# scanned root={ROOT} max_depth={MAX_DEPTH} min_children={MIN_CHILDREN}")
print(f"# {len(rows)} dirs with >= {MIN_CHILDREN} children")
print()
print("=== FOLDER-OF-FOLDERS candidates (FF: dirs, ~no media) ===")
print(f"  {'depth':5s} {'dirs':5s} {'files':6s} {'media':6s}  path")
for depth, p, dirs, files, media in ff[:15]:
    print(f"  {depth:5d} {dirs:5d} {files:6d} {media:6d}  {p}")
print()
print("=== FOLDER-OF-MEDIA candidates (MED: many media files) ===")
print(f"  {'depth':5s} {'dirs':5s} {'files':6s} {'media':6s}  path")
for depth, p, dirs, files, media in med[:15]:
    print(f"  {depth:5d} {dirs:5d} {files:6d} {media:6d}  {p}")

# Size-scaling curve: FF dirs bucketed by child count near 10/50/200/1000
def nearest(target):
    best = None
    for depth, p, dirs, files, media in rows:
        n = dirs + files
        if best is None or abs(n - target) < abs(best[0] - target):
            best = (n, p, dirs, files, media)
    return best

print()
print("=== SIZE-SCALING targets (nearest to 10/50/200/1000 children) ===")
scaling = []
for t in (10, 50, 200, 1000):
    b = nearest(t)
    if b:
        n, p, dirs, files, media = b
        print(f"  target≈{t:4d}  actual={n:5d}  dirs={dirs} files={files}  {p}")
        scaling.append({"target": t, "n": n, "path": p, "dirs": dirs, "files": files})

# Machine-parsable tail
out = {
    "ff": [{"path": p, "depth": d, "dirs": dr, "files": f, "media": m} for d, p, dr, f, m in ff[:8]],
    "med": [{"path": p, "depth": d, "dirs": dr, "files": f, "media": m} for d, p, dr, f, m in med[:8]],
    "scaling": scaling,
}
print()
print("###JSON###")
print(json.dumps(out))
