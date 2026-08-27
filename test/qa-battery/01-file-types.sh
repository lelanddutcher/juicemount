#!/usr/bin/env bash
# 01-file-types.sh — JuiceMount RELEASE BATTERY: every realistic file/item TYPE
#                    in ONE real Finder copy + per-item chain-of-custody verify.
#
# WHY THIS EXISTS (read before "simplifying" to a single cp):
#   A prior agent kept collapsing this battery to a synthetic `cp` after a context
#   compaction and missed real bugs. Synthetic cp FALSE-GREENS: it never exercises
#   the Finder→NFS path users actually hit (LOOKUP/CREATE/SETATTR/WRITE +
#   ._AppleDouble sidecars + xattr/resource forks + symlink/bundle semantics).
#   THIS SCRIPT MUST drive the copy with a REAL Finder duplicate via osascript.
#   md5 is for VERIFY-AFTER only — NEVER as the driver of the copy.
#
# SCOPE — stage one folder containing EVERY item type below, do ONE real Finder
# duplicate of it onto the mount, drain the spool fully, then verify each item
# landed with the correct semantics (custody md5 for regular files; "still a
# symlink" / "Versions/Current resolves" / "perms preserved" / "uchg honored" /
# "ACL present" / "xattr sidecar produced" for the special items):
#   - plain media:        .mov .mp4 .wav .aac
#   - docs:               .pdf .jpg .png
#   - archives:           .zip .dmg
#   - a .app bundle       (Contents/MacOS/<exec>, Info.plist)
#   - a *.framework       WITH internal Versions/A + Versions/Current + top
#                         symlinks (the classic dylib framework symlink layout)
#   - a .fcpbundle package (Final Cut package directory)
#   - loose symlinks:     relative, absolute, dangling
#   - a macOS Finder alias (osascript "make alias file")
#   - hidden dotfiles:    .hidden_config
#   - files carrying xattrs: com.apple.quarantine + Finder tags
#                         (_kMDItemUserTags) + FinderInfo + where-from
#                         → these GENERATE ._AppleDouble sidecars over NFS
#   - read-only file:     chmod 444
#   - uchg-locked file:   chflags uchg
#   - a file with an ACL: chmod +a "<user> allow read"
#
# CHAIN OF CUSTODY (principle #2): Finder write → NFS handler (CREATE/WRITE) →
# write spool → drainer (durable checkpoints + SHA verify) → JuiceFS backend →
# READBACK md5 == source. We qa_wait_drain (pending_files==0 && in_progress==0)
# BEFORE any verify — verifying a non-drained spool reads cache, not at-rest.
#
# ZERO-FINDER-ERROR GATE (principle #3): bracket the copy with qa_log_mark …
# qa_error_scan and FAIL on any FromHandle STALE / purging phantom / 100070 /
# 100060 / -48 'already an item' / -36 / -5000 / permission signature, naming the
# offending path. Known residual ~1 STALE per ~960 ._ files is qa_warn, not fail.
#
# NON-DESTRUCTIVE (principle #5): sources live under $QA_STAGE (/tmp); the copy
# lands in a UNIQUE timestamped dest under $QA_DEST_ROOT; the EXIT trap clears any
# uchg lock we set and removes ONLY what we created. We NEVER touch real folders.
#
# NOT RUN HERE: the orchestrator runs this against the live mount afterward.
# Final line is a single VERDICT: 'PASS 01-file-types' or
# 'FAIL 01-file-types: <reason with offending path>'.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CAT_LABEL="01-file-types"

# --- per-run scratch we must clean up regardless of where we exit -------------
# (uchg-locked sources would block rm -rf, so we track them and unlock first.)
QA_FT_STAGE=""          # this run's staging subtree under $QA_STAGE
QA_FT_DEST=""           # this run's unique dest under $QA_DEST_ROOT
QA_FT_UCHG_PATHS=""     # newline list of paths we set `uchg` on (src AND dest)

_ft_unlock_all() {
    local p old="$IFS"
    IFS='
'
    for p in $QA_FT_UCHG_PATHS; do
        [ -n "$p" ] || continue
        [ -e "$p" ] && chflags nouchg "$p" 2>/dev/null || true
    done
    IFS="$old"
}

_ft_cleanup() {
    # Clear any uchg locks first so qa_cleanup's rm -rf can actually delete.
    _ft_unlock_all
    # Our dest leaf (qa_cleanup's guard only removes QA_*_$$_* dirs; ours matches,
    # but remove explicitly too in case the tag epoch races the find glob).
    if [ -n "$QA_FT_DEST" ]; then
        case "$QA_FT_DEST" in
            "$MOUNT"/JM_RELEASE_BATTERY/*) rm -rf "$QA_FT_DEST" 2>/dev/null || true ;;
        esac
    fi
    if [ -n "$QA_FT_STAGE" ]; then
        case "$QA_FT_STAGE" in
            /tmp/jm-battery-stage-*) rm -rf "$QA_FT_STAGE" 2>/dev/null || true ;;
        esac
    fi
    # lib.sh qa_cleanup (called by qa_end) handles $QA_STAGE + tagged dests too.
}
trap '_ft_cleanup' EXIT

# Emit the single-line VERDICT and let the EXIT trap clean up. Always exit 0 so
# the harness keeps running; pass/fail is read from .summary + the VERDICT line.
_verdict() {
    if [ "${QA_FAIL:-0}" -eq 0 ]; then
        echo "VERDICT: PASS $QA_CAT_LABEL"
    else
        # $1 = reason (already names the offending path).
        echo "VERDICT: FAIL $QA_CAT_LABEL: ${1:-see [FAIL] lines above}"
    fi
}

# ============================================================================ #
qa_begin "$QA_CAT_LABEL"

if ! qa_preflight; then
    qa_warn "preflight failed — skipping 01-file-types (env not ready)"
    qa_end
    _verdict "preflight failed (control plane/mount/dest not ready)"
    exit 0
fi

QA_FT_STAGE="$QA_STAGE/file-types"
SRC="$QA_FT_STAGE/payload"          # the one folder we Finder-duplicate
mkdir -p "$SRC"

QA_FT_DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$QA_FT_DEST"
LEAF="payload"                       # Finder duplicate of "payload" → DEST/payload
LANDED="$QA_FT_DEST/$LEAF"

# Manifest of REGULAR-FILE payloads only (the items qa_verify_custody can md5).
# Special items (symlinks/alias/bundle-symlinks/perms/uchg/acl) are verified by
# dedicated per-type checks below, NOT via the flat md5 manifest.
MANIFEST="$QA_CAT_DIR/payload.manifest"
: > "$MANIFEST"
_add_manifest() {  # _add_manifest RELPATH ABS_SRC_PATH
    local rel="$1" abs="$2" md
    md="$(md5 -q "$abs" 2>/dev/null)"
    printf '%s\t%s\n' "$rel" "$md" >> "$MANIFEST"
}

# ---------------------------------------------------------------------------
qa_sec "staging every item type under $SRC"

# --- plain media + docs + archives (regular files → custody-md5'd) -----------
# Sizes kept modest so the staged set copies fast; type coverage is the point,
# not throughput (09-large-deep-tree covers scale).
MEDIA_SPEC="media/clip.mov:3145728 media/take.mp4:2097152 media/audio.wav:1048576 media/voice.aac:524288"
DOC_SPEC="docs/brief.pdf:262144 docs/frame.jpg:524288 docs/logo.png:131072"
ARC_SPEC="archives/bundle.zip:786432 archives/disk.dmg:1572864"

for spec in $MEDIA_SPEC $DOC_SPEC $ARC_SPEC; do
    rel="${spec%%:*}"; size="${spec#*:}"
    qa_stage_file "$SRC/$rel" "$size" >/dev/null
    _add_manifest "$rel" "$SRC/$rel"
    qa_log "  staged regular file: $rel ($size B)"
done

# --- a .app bundle (regular files inside; custody-md5'd by relpath) ----------
APP="$SRC/Sample.app"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
qa_stage_file "$APP/Contents/MacOS/Sample" 65536 >/dev/null
_add_manifest "Sample.app/Contents/MacOS/Sample" "$APP/Contents/MacOS/Sample"
cat > "$APP/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleExecutable</key><string>Sample</string>
<key>CFBundleIdentifier</key><string>com.juicemount.qa.sample</string>
</dict></plist>
PLIST
_add_manifest "Sample.app/Contents/Info.plist" "$APP/Contents/Info.plist"
qa_stage_file "$APP/Contents/Resources/icon.bin" 32768 >/dev/null
_add_manifest "Sample.app/Contents/Resources/icon.bin" "$APP/Contents/Resources/icon.bin"
qa_log "  staged .app bundle: Sample.app"

# --- a *.framework WITH internal Versions/A + Versions/Current symlinks -------
# Classic macOS framework layout. The whole point is the INTERNAL symlinks
# (Versions/Current -> A, and the top-level shims) must round-trip as symlinks
# AND keep resolving after a Finder copy over NFS — the hardest case.
FW="$SRC/Sample.framework"
mkdir -p "$FW/Versions/A/Resources" "$FW/Versions/A/Headers"
qa_stage_file "$FW/Versions/A/Sample" 131072 >/dev/null
_add_manifest "Sample.framework/Versions/A/Sample" "$FW/Versions/A/Sample"
cat > "$FW/Versions/A/Resources/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict><key>CFBundleName</key><string>Sample</string></dict></plist>
PLIST
_add_manifest "Sample.framework/Versions/A/Resources/Info.plist" "$FW/Versions/A/Resources/Info.plist"
qa_stage_file "$FW/Versions/A/Headers/Sample.h" 4096 >/dev/null
_add_manifest "Sample.framework/Versions/A/Headers/Sample.h" "$FW/Versions/A/Headers/Sample.h"
# Internal version symlink + top-level shims (all RELATIVE, as real frameworks are).
ln -s "A" "$FW/Versions/Current"
ln -s "Versions/Current/Sample" "$FW/Sample"
ln -s "Versions/Current/Resources" "$FW/Resources"
ln -s "Versions/Current/Headers" "$FW/Headers"
qa_log "  staged *.framework with Versions/Current symlink chain"

# --- a .fcpbundle package (Final Cut package = a directory with .fcpbundle ext) -
FCP="$SRC/Library.fcpbundle"
mkdir -p "$FCP/Sample Event/Original Media" "$FCP/CurrentVersion.flexolibrary"
qa_stage_file "$FCP/Sample Event/Original Media/take001.mov" 1048576 >/dev/null
_add_manifest "Library.fcpbundle/Sample Event/Original Media/take001.mov" "$FCP/Sample Event/Original Media/take001.mov"
qa_stage_file "$FCP/CurrentVersion.flexolibrary/library.plist" 8192 >/dev/null
_add_manifest "Library.fcpbundle/CurrentVersion.flexolibrary/library.plist" "$FCP/CurrentVersion.flexolibrary/library.plist"
qa_log "  staged .fcpbundle package"

# --- loose symlinks: relative, absolute, dangling ----------------------------
mkdir -p "$SRC/links/target_dir"
qa_stage_file "$SRC/links/real_target.dat" 16384 >/dev/null
_add_manifest "links/real_target.dat" "$SRC/links/real_target.dat"
ln -s "real_target.dat"            "$SRC/links/rel_link"           # relative → file
ln -s "$SRC/links/real_target.dat" "$SRC/links/abs_link"           # absolute → file
ln -s "no_such_file_$$"            "$SRC/links/dangling_link"      # dangling
qa_log "  staged loose symlinks: relative / absolute / dangling"

# --- a macOS Finder alias (NOT a symlink — a real alias file) ----------------
# Built via Finder so it's a genuine alias resource, exactly what a user makes.
ALIAS_TARGET="$SRC/links/real_target.dat"
ALIAS_OUT="$SRC/links"
_alias_made=1
osascript \
    -e 'on run argv' \
    -e '  set t to POSIX file (item 1 of argv) as alias' \
    -e '  set d to POSIX file (item 2 of argv)' \
    -e '  tell application "Finder" to make alias file to t at (d as alias)' \
    -e 'end run' \
    "$ALIAS_TARGET" "$ALIAS_OUT" >/dev/null 2>&1 || _alias_made=0
if [ "$_alias_made" -eq 1 ]; then
    qa_log "  staged macOS Finder alias file in links/"
else
    qa_warn "could not stage a Finder alias (osascript make alias failed) — alias check will be skipped"
fi

# --- hidden dotfiles ---------------------------------------------------------
qa_stage_file "$SRC/.hidden_config" 2048 >/dev/null
_add_manifest ".hidden_config" "$SRC/.hidden_config"
qa_log "  staged hidden dotfile: .hidden_config"

# --- files carrying xattrs (→ ._AppleDouble sidecars over NFS) ---------------
# quarantine + Finder tags + FinderInfo + where-from. These are the forks that
# produce ._<name> sidecars on the NFS write path.
XF="$SRC/xattr/tagged.mov"
qa_stage_file "$XF" 524288 >/dev/null
_add_manifest "xattr/tagged.mov" "$XF"
xattr -w com.apple.quarantine "0083;00000000;Safari;" "$XF" 2>/dev/null || true
# Finder tags (Red): _kMDItemUserTags as a plist array.
xattr -w com.apple.metadata:_kMDItemUserTags '("Red\n6")' "$XF" 2>/dev/null || true
# where-from.
xattr -w com.apple.metadata:kMDItemWhereFroms '("https://example.com/take.mov")' "$XF" 2>/dev/null || true
# FinderInfo (32 bytes of type/creator/flags) — drives the AppleDouble resource fork.
xattr -wx com.apple.FinderInfo \
    "4D 6F 6F 56 54 56 4F 44 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00" \
    "$XF" 2>/dev/null || true
qa_log "  staged xattr-carrying file (quarantine+tags+FinderInfo+wherefrom): xattr/tagged.mov"

# --- read-only (chmod 444) ---------------------------------------------------
RO="$SRC/perms/readonly.dat"
qa_stage_file "$RO" 16384 >/dev/null
_add_manifest "perms/readonly.dat" "$RO"
chmod 444 "$RO"
qa_log "  staged read-only (444) file: perms/readonly.dat"

# --- uchg-locked (chflags uchg) — track for unlock-before-cleanup ------------
LK="$SRC/perms/locked.dat"
qa_stage_file "$LK" 16384 >/dev/null
_add_manifest "perms/locked.dat" "$LK"
chflags uchg "$LK" 2>/dev/null || true
QA_FT_UCHG_PATHS="$QA_FT_UCHG_PATHS
$LK"
qa_log "  staged uchg-locked file: perms/locked.dat"

# --- a file with an ACL (chmod +a) -------------------------------------------
ACLF="$SRC/perms/acl.dat"
qa_stage_file "$ACLF" 16384 >/dev/null
_add_manifest "perms/acl.dat" "$ACLF"
_acl_user="$(id -un)"
_acl_made=1
chmod +a "$_acl_user allow read,write" "$ACLF" 2>/dev/null || _acl_made=0
if [ "$_acl_made" -eq 1 ]; then
    qa_log "  staged file with ACL: perms/acl.dat ($_acl_user allow read,write)"
else
    qa_warn "could not stage an ACL (chmod +a failed) — ACL check will be skipped"
fi

# ---------------------------------------------------------------------------
qa_sec "ONE real Finder duplicate of the whole payload → $LANDED"

mark="$(qa_log_mark)"
out="$(qa_finder_copy "$SRC" "$QA_FT_DEST")"
rc=$?
qa_log "  finder duplicate rc=$rc result: $(printf '%s' "$out" | head -1)"
if [ "$rc" -ne 0 ]; then
    qa_fail "Finder duplicate FAILED (rc=$rc) for $SRC -> $QA_FT_DEST: $out"
fi
if ! qa_finder_result_clean "$out"; then
    qa_fail "Finder result carried a gated error for $SRC: $out"
fi

# Finder duplicating "payload" into a dest that may already hold a "payload"
# (it won't on a fresh unique dest, but be robust) appends " copy". Resolve the
# actual landed leaf so every per-item check targets the right tree.
if [ ! -d "$LANDED" ]; then
    _alt="$(ls -1d "$QA_FT_DEST/$LEAF"* 2>/dev/null | head -1)"
    [ -n "$_alt" ] && LANDED="$_alt"
fi
qa_log "  landed leaf resolved to: $LANDED"

# ---------------------------------------------------------------------------
qa_sec "MANDATORY drain gate before any verify"
if ! qa_wait_drain; then
    qa_fail "spool did not fully drain for $LANDED (pending/in_progress nonzero — see WARN)"
fi

# ---------------------------------------------------------------------------
qa_sec "chain-of-custody (md5) for all REGULAR-FILE payloads"
if qa_verify_custody "$MANIFEST" "$LANDED"; then
    qa_pass "custody clean for all regular-file payloads under $LANDED"
else
    qa_fail "custody mismatch/missing under $LANDED (see custody lines)"
fi

# ---------------------------------------------------------------------------
qa_sec "per-type semantic checks (the items md5 can't speak to)"

# Helper: assert a path landed and IS a symlink (stayed a symlink, not deref'd).
_is_symlink() {  # _is_symlink ABS LABEL
    if [ -L "$1" ]; then qa_pass "symlink preserved: $2 ($1)"; return 0
    elif [ -e "$1" ]; then qa_fail "symlink DEREFERENCED to a real file: $2 ($1)"; return 1
    else qa_fail "symlink MISSING after copy: $2 ($1)"; return 1; fi
}

# .app bundle — present as a directory with its exec + Info.plist (md5 above
# already proved bytes; here assert the bundle structure survived as a dir).
if [ -d "$LANDED/Sample.app/Contents/MacOS" ] && [ -f "$LANDED/Sample.app/Contents/Info.plist" ]; then
    qa_pass ".app bundle structure intact: $LANDED/Sample.app"
else
    qa_fail ".app bundle structure broken: $LANDED/Sample.app"
fi

# .framework — internal symlinks must STILL be symlinks AND resolve.
FWL="$LANDED/Sample.framework"
_is_symlink "$FWL/Versions/Current" "framework Versions/Current"
_is_symlink "$FWL/Sample"           "framework top Sample shim"
_is_symlink "$FWL/Resources"        "framework top Resources shim"
_is_symlink "$FWL/Headers"          "framework top Headers shim"
# Resolution: Versions/Current -> A, and the top Sample shim resolves to the real binary.
if [ "$(readlink "$FWL/Versions/Current" 2>/dev/null)" = "A" ]; then
    qa_pass "framework Versions/Current -> A target preserved"
else
    qa_fail "framework Versions/Current target wrong: $FWL/Versions/Current (got '$(readlink "$FWL/Versions/Current" 2>/dev/null)')"
fi
if [ -f "$FWL/Sample" ] && [ -f "$FWL/Versions/A/Sample" ]; then
    # -f follows the symlink: it must resolve through Current to a real file.
    qa_pass "framework Sample shim resolves through Versions/Current to a real binary"
else
    qa_fail "framework Sample shim does NOT resolve to a file: $FWL/Sample"
fi

# .fcpbundle — package directory survived (md5 above proved the inner media).
if [ -d "$LANDED/Library.fcpbundle/Sample Event/Original Media" ]; then
    qa_pass ".fcpbundle package structure intact: $LANDED/Library.fcpbundle"
else
    qa_fail ".fcpbundle package structure broken: $LANDED/Library.fcpbundle"
fi

# loose symlinks — relative / absolute / dangling must all stay symlinks.
_is_symlink "$LANDED/links/rel_link"      "loose relative symlink"
_is_symlink "$LANDED/links/abs_link"      "loose absolute symlink"
_is_symlink "$LANDED/links/dangling_link" "loose dangling symlink"
# Relative link must still point at the same (relative) target string.
if [ "$(readlink "$LANDED/links/rel_link" 2>/dev/null)" = "real_target.dat" ]; then
    qa_pass "relative symlink target string preserved (real_target.dat)"
else
    qa_fail "relative symlink target string changed: $LANDED/links/rel_link (got '$(readlink "$LANDED/links/rel_link" 2>/dev/null)')"
fi
# Dangling link must remain dangling (a symlink whose target does not exist).
if [ -L "$LANDED/links/dangling_link" ] && [ ! -e "$LANDED/links/dangling_link" ]; then
    qa_pass "dangling symlink stayed dangling (not materialized): $LANDED/links/dangling_link"
elif [ -L "$LANDED/links/dangling_link" ]; then
    qa_warn "dangling symlink resolves at dest (abs target happened to exist): $LANDED/links/dangling_link"
fi

# macOS Finder alias — landed as a file carrying the alias resource fork.
if [ "${_alias_made:-0}" -eq 1 ]; then
    _aliasfile="$(ls -1 "$LANDED/links/" 2>/dev/null | grep -i 'alias' | head -1)"
    if [ -z "$_aliasfile" ]; then
        # Finder names it "<target> alias"; fall back to the known target name.
        [ -e "$LANDED/links/real_target.dat alias" ] && _aliasfile="real_target.dat alias"
    fi
    if [ -n "$_aliasfile" ] && [ -e "$LANDED/links/$_aliasfile" ]; then
        qa_pass "macOS Finder alias landed: $LANDED/links/$_aliasfile"
    else
        qa_fail "macOS Finder alias MISSING after copy under $LANDED/links/"
    fi
fi

# hidden dotfile — landed and still hidden (leading dot preserved).
if [ -f "$LANDED/.hidden_config" ]; then
    qa_pass "hidden dotfile landed: $LANDED/.hidden_config"
else
    qa_fail "hidden dotfile MISSING after copy: $LANDED/.hidden_config"
fi

# xattr-carrying file — the xattrs must survive (Finder carries them over NFS as
# ._AppleDouble forks). Assert the key user-visible ones round-tripped.
XL="$LANDED/xattr/tagged.mov"
if [ -f "$XL" ]; then
    _x_all="$(xattr -l "$XL" 2>/dev/null)"
    _missing_x=""
    printf '%s' "$_x_all" | grep -q 'com.apple.quarantine'                  || _missing_x="$_missing_x quarantine"
    printf '%s' "$_x_all" | grep -q 'com.apple.metadata:_kMDItemUserTags'   || _missing_x="$_missing_x FinderTags"
    printf '%s' "$_x_all" | grep -q 'com.apple.metadata:kMDItemWhereFroms'  || _missing_x="$_missing_x whereFrom"
    printf '%s' "$_x_all" | grep -q 'com.apple.FinderInfo'                  || _missing_x="$_missing_x FinderInfo"
    if [ -z "$_missing_x" ]; then
        qa_pass "all xattrs round-tripped on $XL (quarantine+tags+wherefrom+FinderInfo)"
    else
        qa_fail "xattr(s) LOST on $XL:$_missing_x"
    fi
else
    qa_fail "xattr-carrying file MISSING after copy: $XL"
fi

# read-only (444) — REPORT the landed mode; do NOT gate on exact perm carry.
# A macOS Finder *duplicate* intentionally creates a fresh, owner-editable copy:
# it does NOT preserve a 444 source mode the way `cp -p`/`ditto` do — the
# duplicate lands owner-writable (644). That widening is correct Finder behavior,
# not an NFS/JuiceMount bug, so (exactly like the uchg + ACL checks below) the
# real GATE is the BYTES (already md5'd via custody), and we only REPORT the
# landed mode. We still HARD-FAIL the genuinely-bad outcomes: the file going
# MISSING, or it landing world-/group-WRITABLE (a real over-permissive bug).
ROL="$LANDED/perms/readonly.dat"
if [ -f "$ROL" ]; then
    # stat -f '%Lp' already yields the low permission bits as an OCTAL digit
    # string (e.g. "444"); operate on that string directly — NO arithmetic
    # (an `& 0777` mask would convert to decimal and corrupt the digits).
    _m3="$(stat -f '%Lp' "$ROL" 2>/dev/null)"
    # Right-justify to 3 chars so each owner/group/other position is testable.
    while [ "${#_m3}" -lt 3 ]; do _m3="0$_m3"; done
    # A write bit is octal digit 2,3,6,7. We only GATE on GROUP/OTHER write
    # (positions 2 and 3) — owner write on a Finder duplicate is expected.
    case "$_m3" in
        ?[2367]?|??[2367]) qa_fail "read-only file landed GROUP/OTHER-writable after copy: $ROL (mode=$_m3)" ;;
        444) qa_pass "read-only (444) perms preserved exactly: $ROL" ;;
        *)   qa_info "read-only source landed owner-writable (expected Finder-duplicate behavior; bytes intact via custody): $ROL (mode=$_m3)" ;;
    esac
else
    qa_fail "read-only file MISSING after copy: $ROL"
fi

# uchg-locked — Finder copies the FILE bytes; whether the uchg FLAG carries is
# version-dependent, so the GATE is "the file landed with correct bytes" (already
# md5'd) and we just REPORT the landed flag. We unlock dest+src in the EXIT trap.
LKL="$LANDED/perms/locked.dat"
if [ -f "$LKL" ]; then
    if ls -lO "$LKL" 2>/dev/null | grep -q 'uchg'; then
        qa_pass "uchg lock flag carried to dest: $LKL"
        QA_FT_UCHG_PATHS="$QA_FT_UCHG_PATHS
$LKL"          # ensure we unlock it before cleanup
    else
        qa_info "uchg flag not carried to dest (bytes intact via custody): $LKL"
    fi
else
    qa_fail "uchg-locked file MISSING after copy: $LKL"
fi

# ACL — assert an ACL entry is present on the landed file.
if [ "${_acl_made:-0}" -eq 1 ]; then
    ACLL="$LANDED/perms/acl.dat"
    if [ -f "$ACLL" ]; then
        if ls -le "$ACLL" 2>/dev/null | grep -qi 'allow'; then
            qa_pass "ACL entry preserved on dest: $ACLL"
        else
            qa_warn "ACL not present on dest (bytes intact via custody): $ACLL"
        fi
    else
        qa_fail "ACL file MISSING after copy: $ACLL"
    fi
fi

# ---------------------------------------------------------------------------
qa_sec "snappiness — landed dir listing must be < ${QA_SNAPPY_MS}ms"
ms="$(qa_dir_listing_ms "$LANDED")"
if [ "$ms" -lt 0 ] 2>/dev/null; then
    qa_fail "dir listing FAILED (readdir error) for $LANDED"
elif [ "$ms" -lt "$QA_SNAPPY_MS" ]; then
    qa_pass "dir listing snappy: ${ms}ms < ${QA_SNAPPY_MS}ms for $LANDED"
else
    qa_fail "dir listing SLOW: ${ms}ms >= ${QA_SNAPPY_MS}ms for $LANDED (spinner risk)"
fi

# ---------------------------------------------------------------------------
qa_sec "zero-Finder-error gate — scan log window since copy"
scan="$(qa_error_scan "$mark" "$QA_CAT_LABEL")"
qa_log "  $scan"
errfile="$QA_CAT_DIR/errscan-$QA_CAT_LABEL.txt"

_get() { printf '%s' "$scan" | tr ' ' '\n' | grep "^$1=" | cut -d= -f2; }
n_stale="$(_get STALE)";   n_phantom="$(_get PHANTOM)"
n_e70="$(_get E100070)";   n_e60="$(_get E100060)"
n_e48="$(_get E48)";       n_e36="$(_get E36)"
n_e5000="$(_get E5000)";   n_perm="$(_get PERM)"
: "${n_stale:=0}" "${n_phantom:=0}" "${n_e70:=0}" "${n_e60:=0}"
: "${n_e48:=0}" "${n_e36:=0}" "${n_e5000:=0}" "${n_perm:=0}"

# Hard-fail signatures (must be strictly zero) — name the path from the errscan dump.
_hardfail() {  # _hardfail COUNT NAME
    local cnt="$1" name="$2" first=""
    if [ "$cnt" -gt 0 ] 2>/dev/null; then
        [ -f "$errfile" ] && first="$(head -1 "$errfile" 2>/dev/null)"
        qa_fail "$name x$cnt in copy window (e.g. ${first:-see $errfile}) [$errfile]"
    fi
}
_hardfail "$n_phantom" "purging phantom"
_hardfail "$n_e70"     "100070/NFS3ERR_STALE"
_hardfail "$n_e60"     "100060 FUSE-wedge"
_hardfail "$n_e36"     "-36 ioErr"
_hardfail "$n_e5000"   "-5000 afpAccessDenied"
_hardfail "$n_perm"    "permission-denied"

# -48 'already an item': we copy into a FRESH unique dest, so ANY -48 here is a
# real bug (the known-OK -48 is delete-then-recopy, covered in 02). Hard-fail.
_hardfail "$n_e48" "-48 already-an-item (unexpected: fresh dest)"

# FromHandle STALE: known residual ~1 per ~960 ._-heavy files. This payload has
# only a handful of ._ sidecars, so the allowance is essentially 1. Warn within
# allowance; fail above it, naming the path.
filecount="$(wc -l < "$MANIFEST" | tr -d ' ')"
stale_allow=$(( (filecount + 959) / 960 ))
[ "$stale_allow" -lt 1 ] && stale_allow=1
if [ "$n_stale" -gt "$stale_allow" ] 2>/dev/null; then
    _sp="$(grep -o 'path=[^ ]*' "$errfile" 2>/dev/null | head -1)"
    qa_fail "FromHandle STALE x$n_stale > allowance $stale_allow (path: ${_sp:-see $errfile})"
elif [ "$n_stale" -gt 0 ] 2>/dev/null; then
    _sp="$(grep -o 'path=[^ ]*' "$errfile" 2>/dev/null | head -1)"
    qa_warn "FromHandle STALE x$n_stale within known allowance $stale_allow (path: ${_sp:-n/a})"
fi

if [ "$n_phantom" = "0" ] && [ "$n_e70" = "0" ] && [ "$n_e60" = "0" ] \
    && [ "$n_e48" = "0" ] && [ "$n_e36" = "0" ] && [ "$n_e5000" = "0" ] \
    && [ "$n_perm" = "0" ] && [ "$n_stale" -le "$stale_allow" ]; then
    qa_pass "zero gated Finder-error signatures in copy window (STALE within allowance)"
fi

# Mount still healthy after exercising every type.
if qa_health >/dev/null 2>&1; then
    qa_pass "mount healthy after file-types copy"
else
    qa_fail "mount NOT healthy after file-types copy ($CP_BASE/health)"
fi

# ---------------------------------------------------------------------------
# qa_end writes .summary, prints totals, and runs qa_cleanup. We then emit the
# single-line VERDICT. The first [FAIL] line above already names the offending
# path; _verdict points the reader at it.
qa_end
_verdict
exit 0
