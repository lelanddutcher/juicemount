#!/usr/bin/env bash
# write-integrity.sh — byte-integrity test for the user-visible NFS mount.
#
# This harness exists because the JuiceMount-A/B QA loops shipped a
# write-corruption bug (QA-14) that the existing self-test could not
# detect. The existing self-test wrote a 4 KiB single-RPC payload and
# reported `write_ok: true` while real cp/cat/Finder copies were
# silently scrambling bytes through the NFS handler. Single-RPC
# probes physically cannot trigger the multi-RPC-concurrent-write
# race that produced the corruption.
#
# Per docs/QA-procedure.md Rule 1, this is the PRIMARY correctness
# gate for any change to nfs/, internal/nfs/, bridge/, cache/, or
# metadata/. Run before commit; fail loud on any md5 mismatch.
#
# Tests (all on /Volumes/<MOUNT_BASENAME>, the user-visible NFS path,
# NEVER the FUSE-internal path — FUSE-direct bypasses the handler):
#
#   1. Small single-RPC write (512 KiB) — fits in one WRITE RPC
#   2. Medium multi-RPC write (10 MiB) — multiple sequential RPCs
#   3. Large multi-RPC write (200 MiB) — many sequential RPCs
#   4. Concurrent parallel writes (4 × 10 MiB to different paths,
#      simultaneous) — exercises the concurrent-dispatch race
#   5. cp with xattrs (the Finder-equivalent path that triggered
#      QA-13 and QA-14 in the first place)
#
# Exit codes:
#   0  all tests passed
#   1  one or more byte-integrity tests failed
#   2  precondition error (no mount, no sudo, etc)

set -euo pipefail

MOUNT="${MOUNT:-/Volumes/zpool}"
TMPDIR_LOCAL="${TMPDIR_LOCAL:-/tmp}"

# --- helpers ---
ts_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
pass() { printf '\033[32m[PASS]\033[0m %s\n' "$*"; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { printf '\033[31m[FAIL]\033[0m %s\n' "$*"; FAIL_COUNT=$((FAIL_COUNT+1)); }
warn() { printf '\033[33m[WARN]\033[0m %s\n' "$*"; }

PASS_COUNT=0
FAIL_COUNT=0
TMPSRC=""
DESTS=()

cleanup() {
    [[ -n "$TMPSRC" ]] && rm -f "$TMPSRC"
    for d in "${DESTS[@]+"${DESTS[@]}"}"; do
        rm -f "$d" 2>/dev/null
        # Also clean up any AppleDouble sidecar that landed
        local dir base
        dir=$(dirname "$d")
        base=$(basename "$d")
        rm -f "$dir/._$base" 2>/dev/null
    done
}
trap cleanup EXIT INT TERM

# --- preconditions ---
log "== preconditions =="
log "  mount: $MOUNT"
log ""

if ! mount | grep -q " $MOUNT "; then
    fail "mount $MOUNT is not active"
    echo "[FAIL] write-integrity: precondition (mount inactive)"
    exit 2
fi
mount_kind=$(mount | grep " $MOUNT " | head -1 | awk -F'[()]' '{print $2}' | awk '{print $1}')
if [[ "$mount_kind" != "nfs" ]]; then
    fail "mount $MOUNT is $mount_kind, expected nfs — wrong filesystem at this path"
    echo "[FAIL] write-integrity: precondition (mount is $mount_kind, not nfs)"
    exit 2
fi
log "  ✓ mount is active as nfs"

# Quick writability probe — fail fast if the mount rejects creates entirely.
probe="$MOUNT/.write-integrity-probe-$$"
if ! : >"$probe" 2>/dev/null; then
    fail "cannot create files on $MOUNT"
    echo "[FAIL] write-integrity: precondition (cannot touch $probe)"
    exit 2
fi
rm -f "$probe"
log "  ✓ mount is writable (file creation works)"
log ""

# write_and_verify <label> <size_in_mib> <dest_path>
#
# Generates a random source file of the given size, copies it to the
# destination via `cat` (forces a real read+write loop, not a
# clonefile shortcut), then md5-verifies. Returns 0 on match, 1 on
# mismatch, 2 on missing dest.
write_and_verify() {
    local label="$1"
    local size_mib="$2"
    local dst="$3"
    local src="$TMPDIR_LOCAL/wi-src-$$-$RANDOM"

    dd if=/dev/urandom of="$src" bs=1M count="$size_mib" 2>/dev/null
    local src_md5
    src_md5=$(md5 -q "$src")
    local src_size
    src_size=$(stat -f%z "$src")

    # `cat` forces a simple read+write loop — no clonefile, no
    # copyfile(3), no xattr dance. This is the cleanest path to
    # exercise the NFS WRITE pipeline.
    cat "$src" > "$dst" 2>/dev/null
    local cat_exit=$?

    rm -f "$src"

    if [[ $cat_exit -ne 0 ]]; then
        fail "$label: cat returned exit $cat_exit"
        return 1
    fi

    if [[ ! -f "$dst" ]]; then
        fail "$label: destination did not appear"
        return 2
    fi

    local dst_size
    dst_size=$(stat -f%z "$dst")
    local dst_md5
    dst_md5=$(md5 -q "$dst")

    if [[ "$src_size" != "$dst_size" ]]; then
        fail "$label: size mismatch (src $src_size, dst $dst_size)"
        return 1
    fi
    if [[ "$src_md5" != "$dst_md5" ]]; then
        fail "$label: md5 mismatch (src $src_md5, dst $dst_md5) — SIZE MATCHES, CONTENT CORRUPTED"
        return 1
    fi

    pass "$label: $size_mib MiB md5=$src_md5"
    return 0
}

# --- test 1: small single-RPC ---
log "== test 1: small single-RPC write (512 KiB) =="
T1_DST="$MOUNT/.wi-t1-$$"
DESTS+=("$T1_DST")
# 512 KiB fits in a single 1 MiB rsize/wsize RPC.
src="$TMPDIR_LOCAL/wi-src-$$-t1"
dd if=/dev/urandom of="$src" bs=1024 count=512 2>/dev/null
src_md5=$(md5 -q "$src")
cat "$src" > "$T1_DST" 2>/dev/null
rm -f "$src"
if [[ "$(md5 -q "$T1_DST")" == "$src_md5" ]]; then
    pass "test 1 (512 KiB single-RPC): md5=$src_md5"
else
    fail "test 1: md5 mismatch ($src_md5 vs $(md5 -q "$T1_DST"))"
fi
log ""

# --- test 2: medium multi-RPC ---
log "== test 2: medium multi-RPC write (10 MiB) =="
T2_DST="$MOUNT/.wi-t2-$$"
DESTS+=("$T2_DST")
write_and_verify "test 2 (10 MiB multi-RPC)" 10 "$T2_DST"
log ""

# --- test 3: large multi-RPC ---
log "== test 3: large multi-RPC write (200 MiB) =="
T3_DST="$MOUNT/.wi-t3-$$"
DESTS+=("$T3_DST")
write_and_verify "test 3 (200 MiB multi-RPC)" 200 "$T3_DST"
log ""

# --- test 4: concurrent parallel writes ---
# Exercises the race window — 4 cp processes writing to 4 different
# paths simultaneously. If the handler's writeFile.Write shares fd
# offsets across paths, this is where the corruption shows up.
log "== test 4: concurrent parallel writes (4 × 10 MiB) =="
declare -a T4_SRCS T4_DSTS T4_MD5S
for i in 1 2 3 4; do
    T4_SRCS+=("$TMPDIR_LOCAL/wi-src-$$-t4-$i")
    T4_DSTS+=("$MOUNT/.wi-t4-$$-$i")
    DESTS+=("$MOUNT/.wi-t4-$$-$i")
    dd if=/dev/urandom of="${T4_SRCS[$((i-1))]}" bs=1M count=10 2>/dev/null
    T4_MD5S+=("$(md5 -q "${T4_SRCS[$((i-1))]}")")
done
# Kick off all 4 cats in parallel.
for i in 0 1 2 3; do
    cat "${T4_SRCS[$i]}" > "${T4_DSTS[$i]}" 2>/dev/null &
done
wait
for i in 0 1 2 3; do
    rm -f "${T4_SRCS[$i]}"
    actual=$(md5 -q "${T4_DSTS[$i]}" 2>/dev/null)
    if [[ "$actual" == "${T4_MD5S[$i]}" ]]; then
        pass "test 4.$((i+1)): md5 match (${T4_MD5S[$i]})"
    else
        fail "test 4.$((i+1)): md5 mismatch (expected ${T4_MD5S[$i]}, got $actual)"
    fi
done
log ""

# --- test 5: cp with xattrs (Finder-equivalent) ---
log "== test 5: cp -p with xattrs (Finder-equivalent path) =="
T5_SRC="$TMPDIR_LOCAL/wi-src-$$-t5"
T5_DST="$MOUNT/.wi-t5-$$"
DESTS+=("$T5_DST")
dd if=/dev/urandom of="$T5_SRC" bs=1M count=5 2>/dev/null
# Set a fake Finder xattr on the source so cp -p has something to copy.
# 32-byte hex string (FinderInfo is 32 bytes); some macOS xattr versions
# reject shorter strings with "Result too large" — that's harmless for
# this test (we just want SOMETHING for cp -p to try to copy).
xattr -w com.apple.FinderInfo \
    "00000000000000000000000000000000000000000000000000000000000000000000000000000000" \
    "$T5_SRC" 2>/dev/null || true
src_md5=$(md5 -q "$T5_SRC")
# cp -p returns non-zero when it warns about xattr issues even if the
# byte stream copied cleanly. We only care about md5 round-trip and
# sidecar presence — both are observable after the cp completes.
# Disable `set -e` for this one call so the harness keeps going.
set +e
cp -p "$T5_SRC" "$T5_DST" 2>&1 | head -3
set -e
rm -f "$T5_SRC"
dst_md5=$(md5 -q "$T5_DST" 2>/dev/null)
dst_size=$(stat -f%z "$T5_DST" 2>/dev/null)
if [[ "$src_md5" == "$dst_md5" ]]; then
    pass "test 5 (cp -p with xattrs): md5 match"
else
    fail "test 5 (cp -p with xattrs): md5 mismatch (src $src_md5, dst $dst_md5, size $dst_size)"
fi
# Check whether the AppleDouble sidecar landed (informational)
sidecar="$MOUNT/._.wi-t5-$$"
DESTS+=("$sidecar")
if [[ -f "$sidecar" ]]; then
    log "  (informational) ._sidecar created at $sidecar, $(stat -f%z "$sidecar") bytes"
fi
log ""

# --- summary ---
log "== summary =="
log "  passed: $PASS_COUNT"
log "  failed: $FAIL_COUNT"
if [[ $FAIL_COUNT -gt 0 ]]; then
    echo "[FAIL] write-integrity: $FAIL_COUNT/$((PASS_COUNT+FAIL_COUNT)) failed"
    exit 1
fi
echo "[PASS] write-integrity: all $PASS_COUNT tests passed"
exit 0
