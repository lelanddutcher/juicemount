#!/usr/bin/env python3
"""Verify the repository-owned SDK source and its narrow upstream patch."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import zipfile


PATCH_FILES = {
    "patches/0004-preflight-link-counters.patch": ["pkg/meta/redis.go"],
    "patches/0001-synchronize-upload-failure.patch": ["pkg/chunk/cached_store.go"],
    "patches/0002-create-only-object-writes.patch": [
        "pkg/object/file.go", "pkg/object/interface.go",
        "pkg/object/s3.go", "pkg/object/sharding.go"],
    "patches/0003-inode-bound-slice-allocation.patch": [
        "pkg/meta/interface.go", "pkg/vfs/helpers_test.go", "pkg/vfs/writer.go"],
    "patches/0005-owner-keeps-kernel-mount.patch": [
        "cmd/mount_unix.go", "cmd/umount.go", "pkg/fuse/fuse.go"],
    "patches/0006-redact-redis-url-errors-default-ipv6-port.patch": [
        "pkg/meta/redis.go", "pkg/object/redis.go"],
    "patches/0007-jmdeny-erofs.patch": ["pkg/meta/base.go", "pkg/meta/utils.go", "pkg/vfs/writer.go"],
    "patches/0008-stop-signal-never-remounts.patch": ["cmd/mount_unix.go"],
    "patches/0009-darwin-exclusive-rename.patch": ["pkg/fuse/fuse.go"],
    "patches/0010-replacement-guards-and-cache-ownership.patch": [
        "pkg/chunk/cached_store.go", "pkg/chunk/disk_cache.go", "pkg/meta/redis.go"],
    "patches/0011-stage-birth-create-watch.patch": ["pkg/meta/redis.go"],
    "patches/0012-born-stage-upload-watch.patch": ["pkg/meta/redis.go"],
}
VERSION_PACKAGE = "github.com/juicedata/juicefs/pkg/version"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def apply_patch(text, patch):
    """Apply difflib unified diffs (per file) to {name: text}; return new map."""
    out = dict(text)
    lines = patch.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        if not lines[i].startswith("--- a/"):
            raise ValueError("SDK patch is not a plain unified diff")
        name = lines[i][len("--- a/"):].rstrip("\n")
        if lines[i + 1].rstrip("\n") != "+++ b/" + name:
            raise ValueError("SDK patch header mismatch: " + name)
        i += 2
        old = out[name].splitlines(keepends=True)
        new, cursor = [], 0
        while i < len(lines) and lines[i].startswith("@@"):
            def span(field):
                start, _, length = field[1:].partition(",")
                return int(start), int(length) if length else 1
            (start, old_len), (_, new_len) = (span(f) for f in lines[i].split()[1:3])
            if old_len == 0:
                start += 1
            i += 1
            new.extend(old[cursor:start - 1])
            cursor = start - 1
            while old_len or new_len:
                kind, body = lines[i][0], lines[i][1:]
                if kind in " -":
                    if cursor >= len(old) or old[cursor] != body:
                        raise ValueError("SDK patch does not apply: " + name)
                    cursor += 1
                    old_len -= 1
                if kind in " +":
                    new.append(body)
                    new_len -= 1
                if kind not in " -+" or old_len < 0 or new_len < 0:
                    raise ValueError("SDK patch hunk is malformed: " + name)
                i += 1
        new.extend(old[cursor:])
        out[name] = "".join(new)
    return out


def verify(archive=None):
    root = Path(__file__).resolve().parents[1]
    source = root / "third_party/juicefs"
    manifest = json.loads((source / "SOURCE.json").read_text())
    files = manifest["files"]
    patches = manifest["patches"]
    local = manifest["local_files"]
    expected = set(files) | set(patches) | set(local) | {"SOURCE.json", "README.juicemount.md"}
    actual = {str(p.relative_to(source)) for p in source.rglob("*") if p.is_file()}
    if actual != expected:
        raise ValueError("SDK snapshot has missing or unrecorded files")
    for name, entry in files.items():
        path = source / name
        if path.is_symlink() or not path.resolve().is_relative_to(source.resolve()):
            raise ValueError("SDK snapshot contains a path alias")
        if sha(path.read_bytes()) != entry["sha256"]:
            raise ValueError("SDK source hash mismatch: " + name)
    for name, digest in patches.items():
        if sha((source / name).read_bytes()) != digest:
            raise ValueError("SDK patch hash mismatch")
    for name, digest in local.items():
        path = source / name
        if name in files or path.is_symlink() or sha(path.read_bytes()) != digest:
            raise ValueError("SDK local source hash mismatch: " + name)
    binary = manifest["binary"]
    flags = dict(item.split("=", 1) for item in binary["ldflags"].split() if "=" in item)
    revision = flags.get(VERSION_PACKAGE + ".revision", "")
    date = flags.get(VERSION_PACKAGE + ".revisionDate", "")
    if (not revision.startswith(binary["upstream_commit"][:12] + "-jm")
            or binary["version_output"] != "juicefs version 1.3.1+" + date + "." + revision):
        raise ValueError("bundled JuiceFS version pin is inconsistent")
    changed = [n for n, v in files.items() if v["sha256"] != v["upstream_sha256"]]
    if changed != sorted({n for group in PATCH_FILES.values() for n in group}) or set(patches) != set(PATCH_FILES):
        raise ValueError("SDK snapshot changed outside the reviewed source files")
    replacement = "replace github.com/juicedata/juicefs v1.3.1 => ./third_party/juicefs"
    if (root / "go.mod").read_text().splitlines().count(replacement) != 1:
        raise ValueError("SDK replacement is missing or ambiguous")
    module_sum = "github.com/juicedata/juicefs v1.3.1 " + manifest["module_sum"]
    if module_sum not in (root / "go.sum").read_text().splitlines():
        raise ValueError("SDK archive checksum disagrees with root go.sum")
    if archive:
        prefix = manifest["module"] + "@" + manifest["version"] + "/"
        with zipfile.ZipFile(archive) as zipped:
            # Go's dirhash.Hash1: sorted names, each prefixed by its content hash.
            h = hashlib.sha256()
            for name in sorted(zipped.namelist()):
                h.update((sha(zipped.read(name)) + "  " + name + "\n").encode())
            if "h1:" + base64.b64encode(h.digest()).decode() != manifest["module_sum"]:
                raise ValueError("SDK upstream archive checksum mismatch")
            selected = {n[len(prefix):] for n in zipped.namelist()
                        if n.startswith(prefix + "pkg/") or n.startswith(prefix + "cmd/") or n in
                        {prefix + "LICENSE", prefix + "go.mod", prefix + "go.sum", prefix + "main.go"}}
            if selected != set(files):
                raise ValueError("SDK snapshot is not the complete declared library scope")
            for name, entry in files.items():
                if sha(zipped.read(prefix + name)) != entry["upstream_sha256"]:
                    raise ValueError("SDK upstream source hash mismatch: " + name)
            # Patches stack in name order; a file may be changed by several.
            # Applying them to upstream must reproduce the tree exactly, and
            # each patch must touch exactly its declared files.
            state = {n: zipped.read(prefix + n).decode() for n in changed}
            for patch in sorted(PATCH_FILES):
                text = (source / patch).read_text()
                touched = [l[len("--- a/"):].rstrip("\n") for l in text.splitlines(keepends=True)
                           if l.startswith("--- a/")]
                if touched != PATCH_FILES[patch]:
                    raise ValueError("SDK patch touches undeclared files: " + patch)
                state = apply_patch(state, text)
            for name in changed:
                if state[name] != (source / name).read_text():
                    raise ValueError("SDK source delta differs from the recorded patches: " + name)
    return {"module": manifest["module"], "version": manifest["version"],
            "source_files": len(files), "patched_files": changed, "local_files": sorted(local),
            "binary_version": binary["version_output"],
            "upstream_archive_verified": bool(archive), "result": "PASS"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, help="Optional upstream v1.3.1 module ZIP")
    args = parser.parse_args()
    print(json.dumps(verify(args.archive), sort_keys=True))
