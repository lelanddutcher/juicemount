#!/usr/bin/env bash
# 04-unicode-names.sh — JuiceMount RELEASE TESTING BATTERY
# ===========================================================================
# CATEGORY: pathological / unicode / special filenames over the REAL Finder
#           copy path + full chain of custody + zero-Finder-error gate.
#
# WHAT THIS PROVES
#   A user can copy files whose NAMES are hostile — emoji, precomposed (NFC) AND
#   decomposed (NFD) accents, CJK, RTL/bidi marks, a 255-byte max-length leaf,
#   spaces/&/;/parentheses, a leading dot, a trailing space — into the mount via
#   a real Finder duplicate, and EVERY one lands, is findable, drains to the
#   JuiceFS backend, and reads back byte-identical (md5 == source). No
#   normalization-induced collision or loss; no -36/-43/-1407/STALE/phantom.
#
#   The macOS NFS path NFD-normalizes leaf names at rest (HFS/APFS legacy
#   behavior the mount inherits): a name a user typed/created as NFC `café`
#   (U+00E9) is STORED on the mount as NFD `cafe´` (U+0065 U+0301). This script
#   stages BOTH the NFC and the NFD spelling as DISTINCT source files, copies
#   both, and asserts BOTH round-trip. Because custody compares by the stored
#   name, the manifest relpaths are normalized to the NFD form the mount keeps.
#   The KNOWN-OPEN gap — that an explicit NFC-spelled lookup of an NFD-stored
#   leaf can miss unless the caller normalizes first — is recorded as its OWN
#   explicit assertion (qa_warn, not a silent pass), so we drive it to zero
#   over time instead of masking it.
#
# DRIVER: real Finder `duplicate` via osascript (qa_finder_*). Synthetic cp/dd
#         is used ONLY off-mount inside qa_stage_file to produce source bytes.
#
# CHAIN OF CUSTODY: Finder write -> NFS handler -> write spool -> drainer ->
#   JuiceFS backend -> readback md5. qa_wait_drain (pending_files==0 &&
#   in_progress==0) is MANDATORY before qa_verify_custody.
#
# NON-DESTRUCTIVE: copies ONLY into a unique $QA_DEST_ROOT/QA_<epoch>_<pid>_*
#   dest; stages ONLY under $QA_STAGE; EXIT trap + qa_cleanup tear down only
#   those. NEVER touches the user's real folders.
#
# bash 3.2-safe (macOS /bin/bash 3.2.57): no assoc arrays, no mapfile, no
#   ${v^^}, no GNU timeout (bounded via qa_timeout / perl alarm in lib.sh).
#
# This script is AUTHORED here and run LATER by the orchestrator against the
# live mount. It is NOT executed during authoring. It always `exit 0`; the
# verdict lives in the printed VERDICT line and the .summary fail count.
# ===========================================================================

set -uo pipefail

source "$(dirname "$0")/lib.sh"

QA_CATEGORY="04-unicode-names"

# ---------------------------------------------------------------------------
# EXIT trap: restore online state (in case anything toggled offline) and clean
# up ONLY our staged /tmp data + our unique mount dest. qa_cleanup has its own
# guard (refuses anything outside $MOUNT/JM_RELEASE_BATTERY + $QA_STAGE).
# ---------------------------------------------------------------------------
_cleanup_done=0
_on_exit() {
    [ "$_cleanup_done" = "1" ] && return
    _cleanup_done=1
    # Best-effort: ensure we never leave the mount stuck offline.
    qa_offline off >/dev/null 2>&1 || true
    # Remove only THIS run's unique dest leaf (qa_cleanup also sweeps QA_*_$$_*).
    if [ -n "${DEST:-}" ]; then
        case "$DEST" in
            "$MOUNT"/JM_RELEASE_BATTERY/QA_*) rm -rf "$DEST" 2>/dev/null || true ;;
        esac
    fi
    qa_cleanup 2>/dev/null || true
}
trap _on_exit EXIT INT TERM

# ---------------------------------------------------------------------------
# Helpers local to this category (NOT lib.sh helpers — pure shell, no mount).
# ---------------------------------------------------------------------------

# _nfc / _nfd STRING — echo the NFC- / NFD-normalized form of STRING.
# Uses perl Unicode::Normalize (ships with the macOS system perl). We need this
# to (a) write manifest relpaths in the NFD form the mount stores, and (b)
# explicitly test the NFC-lookup-vs-NFD-stored gap.
_nfc() { printf '%s' "$1" | perl -CSAD -MUnicode::Normalize -e 'print NFC(do{local $/;<STDIN>})' 2>/dev/null; }
_nfd() { printf '%s' "$1" | perl -CSAD -MUnicode::Normalize -e 'print NFD(do{local $/;<STDIN>})' 2>/dev/null; }

# _byte_len STRING — echo the UTF-8 byte length of STRING (for the 255-byte cap).
_byte_len() { printf '%s' "$1" | wc -c | tr -d ' '; }

# _hexname STRING — echo a hex rendering of the bytes (for naming the offending
# path in a FAIL message when the literal bytes won't render in a log).
_hexname() { printf '%s' "$1" | xxd -p 2>/dev/null | tr -d '\n'; }

# ===========================================================================
# BEGIN
# ===========================================================================
qa_begin "$QA_CATEGORY"

# Preflight gate: if the environment isn't safe, self-skip cleanly (the harness
# already aborted in run-all's precheck / qa_preflight if the mount/CP were down).
if ! qa_preflight; then
    qa_log "preflight failed — skipping $QA_CATEGORY (environment not ready)"
    echo "VERDICT: FAIL $QA_CATEGORY: preflight (control plane / mount / dest guard) not satisfied"
    qa_end
    exit 0
fi

# Unique, collision-proof dest under the mount (cleaned by trap + qa_cleanup).
DEST="$QA_DEST_ROOT/$(qa_unique_tag QA)"
mkdir -p "$DEST" 2>/dev/null
qa_info "dest = $DEST"

# Staged source root for this category (off the mount).
SRC_ROOT="$QA_STAGE/unicode-names"
mkdir -p "$SRC_ROOT" 2>/dev/null

# Per-name source payload size (small — this category stresses NAMES, not size).
NAME_SIZE="${QA_UNICODE_SIZE:-65536}"   # 64 KiB

# Manifest consumed by qa_verify_custody: "relpath<TAB>md5", relpath in the
# NFD form the mount stores so the literal -f lookup inside qa_verify_custody
# resolves to the landed file.
MANIFEST="$QA_CAT_DIR/unicode.manifest"
: > "$MANIFEST"

# Parallel record of the ORIGINAL (as-staged) leaf for each manifest entry, so a
# custody MISS can be reported with the byte sequence the user actually typed.
# Format: "nfd_relpath<TAB>orig_leaf<TAB>hex(orig_leaf)".
NAMEMAP="$QA_CAT_DIR/unicode.namemap"
: > "$NAMEMAP"

TAB="$(printf '\t')"

# _add_name LEAF — stage one source file named LEAF under $SRC_ROOT, record its
# md5 against the NFD-normalized leaf in the manifest (the form the mount keeps),
# and remember the original leaf for diagnostics. Echoes nothing; updates files.
_add_name() {
    local leaf="$1"
    local spath="$SRC_ROOT/$leaf"
    local md nfdleaf
    # Stage the bytes off-mount; qa_stage_file echoes the md5. Give some entries
    # xattrs so ._AppleDouble forks ride along the real Finder copy.
    md="$(qa_stage_file "$spath" "$NAME_SIZE" "com.apple.metadata:_kMDItemUserTags=AABB")"
    if [ -z "$md" ]; then
        qa_fail "staging failed for name leaf hex=$(_hexname "$leaf")"
        return 1
    fi
    nfdleaf="$(_nfd "$leaf")"
    [ -z "$nfdleaf" ] && nfdleaf="$leaf"   # perl fallback: assume already NFD
    printf '%s\t%s\n' "$nfdleaf" "$md" >> "$MANIFEST"
    printf '%s\t%s\t%s\n' "$nfdleaf" "$leaf" "$(_hexname "$leaf")" >> "$NAMEMAP"
    return 0
}

# ---------------------------------------------------------------------------
# CASE SET 1 — build the pathological name corpus (each is its own source file).
# ---------------------------------------------------------------------------
qa_sec "staging pathological name corpus"

# café written TWO ways — both must round-trip.
CAFE_NFC="$(_nfc 'cafe'$'\xcc\x81'_nfc.mov)"   # precomposed é (U+00E9)
CAFE_NFD="$(_nfd 'cafe'$'\xcc\x81'_nfd.mov)"   # decomposed e + U+0301
# Guard against a perl-less environment: fall back to literal byte forms.
[ -z "$CAFE_NFC" ] && CAFE_NFC=$'caf\xc3\xa9_nfc.mov'
[ -z "$CAFE_NFD" ] && CAFE_NFD=$'cafe\xcc\x81_nfd.mov'

# 255-BYTE max-length leaf (UTF-8). Build "u<...>.dat" padded to exactly 255 B.
_LONG_BASE="ultra_long_clip_"
_LONG_EXT=".dat"
_pad_target=$(( 255 - ${#_LONG_BASE} - ${#_LONG_EXT} ))
[ "$_pad_target" -lt 1 ] && _pad_target=200
LONG255="${_LONG_BASE}$(printf 'x%.0s' $(seq 1 "$_pad_target"))${_LONG_EXT}"
# Verify it really is <= 255 bytes (ASCII here, so bytes == chars).
if [ "$(_byte_len "$LONG255")" -gt 255 ]; then
    # Trim to fit (defensive; should not trigger for ASCII).
    LONG255="$(printf '%s' "$LONG255" | cut -c1-251).dat"
fi

# The corpus. Each entry is a single leaf the Finder must copy + the mount keep.
# NOTE: '/' and ':' are NOT usable as literal leaf bytes (path separator / HFS
# colon-mapped) — we exercise the HFS colon edge via a name CONTAINING a colon,
# which Finder maps to '/' on display; we assert it lands without a -36/-43.
# Literal `._*` source leaves are also excluded: Finder reserves that namespace
# for AppleDouble metadata and silently omits an ordinary `._foo` file when
# duplicating between two local APFS directories too. Requiring it here tested
# Finder's platform policy, not JuiceMount's filename handling.
_NAMES="
emoji_🎬🔥_clip.mov
${CAFE_NFC}
${CAFE_NFD}
日本語_素材_4K.mov
영상_원본.mov
rtl_مرحبا_bidi.mov
rtl_שלום_עולם.mov
spaces and  tabs.mov
amp_&_and_semi;_clip.mov
parens_(take_2)_[v3].mov
.leading_dot_hidden.mov
trailing_space_clip .mov
colon_10:30am_take.mov
single'quote.mov
double\"quote.mov
hash#and%percent.mov
${LONG255}
"

_staged=0
# bash 3.2-safe newline iteration (no mapfile). IFS=newline so spaces/quotes in
# a leaf survive as a single token.
_oldifs="$IFS"; IFS='
'
for _leaf in $_NAMES; do
    [ -z "$_leaf" ] && continue
    if _add_name "$_leaf"; then
        _staged=$((_staged+1))
        qa_log "  staged leaf hex=$(_hexname "$_leaf")  bytes=$(_byte_len "$_leaf")"
    fi
done
IFS="$_oldifs"

if [ "$_staged" -lt 1 ]; then
    qa_fail "no pathological names could be staged under $SRC_ROOT"
    echo "VERDICT: FAIL $QA_CATEGORY: staging produced zero names ($SRC_ROOT)"
    qa_end
    exit 0
fi
qa_pass "staged $_staged pathological-name source files under $SRC_ROOT"

# Sanity: the 255-byte name must actually exist on the off-mount source (proves
# the local FS accepted it; the mount is the real test below).
if [ -f "$SRC_ROOT/$LONG255" ]; then
    qa_pass "255-byte max-length leaf staged ok (len=$(_byte_len "$LONG255") bytes)"
else
    qa_fail "255-byte max-length leaf could not be staged: $SRC_ROOT/$LONG255"
fi

# ---------------------------------------------------------------------------
# CASE SET 2 — REAL Finder copy of the whole corpus folder onto the mount.
# ---------------------------------------------------------------------------
qa_sec "real Finder duplicate of pathological-name corpus onto mount"

mark=$(qa_log_mark)

# Finder duplicates the FOLDER (recursing, creating ._AppleDouble + forks just
# like a user drag). Landed leaf will be "$DEST/unicode-names".
out="$(qa_finder_copy "$SRC_ROOT" "$DEST")"
rc=$?
LANDED="$DEST/unicode-names"

if [ "$rc" -ne 0 ]; then
    qa_fail "Finder duplicate FAILED rc=$rc for corpus -> $DEST (result: $out)"
else
    qa_pass "Finder duplicate returned rc=0 for corpus -> $DEST"
fi

# Inspect the Finder result/error string for gated codes (-48/-36/-5000/-1407/-43).
if qa_finder_result_clean "$out"; then
    qa_pass "Finder result string clean (no -36/-43/-48/-1407/-5000) for $LANDED"
else
    qa_fail "Finder result names a gated error code for $LANDED: $out"
fi

# ---------------------------------------------------------------------------
# CASE SET 3 — MANDATORY drain gate, THEN chain-of-custody verify.
# ---------------------------------------------------------------------------
qa_sec "drain gate + chain of custody"

if qa_wait_drain; then
    qa_pass "spool fully drained (pending_files==0 && in_progress==0) before verify"
else
    qa_fail "spool did NOT drain before custody verify for $LANDED (see qa_wait_drain residuals)"
fi

# Custody: every NFD-named entry must land + read back byte-identical. Because
# the manifest relpaths are the NFD form the mount stores, qa_verify_custody's
# literal `-f "$DEST/unicode-names/$rel"` resolves to the landed file.
if qa_verify_custody "$MANIFEST" "$LANDED"; then
    qa_pass "custody clean: every pathological name landed + md5==source ($LANDED)"
else
    # qa_verify_custody already qa_fail'd each MISSING/WRONG with its DEST path.
    # Augment with the original byte sequence for any missing leaf so the verdict
    # can name what the user actually typed.
    while IFS="$TAB" read -r _nfdrel _orig _hex; do
        [ -z "$_nfdrel" ] && continue
        if [ ! -f "$LANDED/$_nfdrel" ]; then
            qa_fail "custody MISSING name: orig='$_orig' hex=$_hex (looked for NFD '$LANDED/$_nfdrel')"
        fi
    done < "$NAMEMAP"
fi

# ---------------------------------------------------------------------------
# CASE SET 4 — explicit NFC-vs-NFD round-trip + the KNOWN NFC-lookup gap.
# ---------------------------------------------------------------------------
qa_sec "NFC vs NFD round-trip + known NFC-lookup-vs-NFD-stored gap"

# 4a. BOTH café spellings must have landed (each stored under its NFD leaf).
_cafe_nfc_stored="$(_nfd "$CAFE_NFC")"; [ -z "$_cafe_nfc_stored" ] && _cafe_nfc_stored="$CAFE_NFC"
_cafe_nfd_stored="$(_nfd "$CAFE_NFD")"; [ -z "$_cafe_nfd_stored" ] && _cafe_nfd_stored="$CAFE_NFD"

if [ -f "$LANDED/$_cafe_nfc_stored" ]; then
    qa_pass "NFC-origin café landed (stored NFD): $LANDED/$_cafe_nfc_stored"
else
    qa_fail "NFC-origin café did NOT land: $LANDED/$_cafe_nfc_stored (hex orig=$(_hexname "$CAFE_NFC"))"
fi
if [ -f "$LANDED/$_cafe_nfd_stored" ]; then
    qa_pass "NFD-origin café landed (stored NFD): $LANDED/$_cafe_nfd_stored"
else
    qa_fail "NFD-origin café did NOT land: $LANDED/$_cafe_nfd_stored (hex orig=$(_hexname "$CAFE_NFD"))"
fi

# 4b. EXPLICIT documented gap assertion: look the café leaf up by its NFC byte
# spelling WITHOUT normalizing. The mount stores NFD; a naive NFC lookup may
# miss. We assert the NORMALIZED lookup succeeds (the correct client behavior),
# and qa_warn — NOT qa_fail — if the raw NFC lookup misses, recording the gap so
# it is driven to zero rather than silently masked.
_cafe_nfc_raw="$(_nfc "$CAFE_NFC")"; [ -z "$_cafe_nfc_raw" ] && _cafe_nfc_raw="$CAFE_NFC"
if [ -f "$LANDED/$_cafe_nfc_raw" ]; then
    qa_info "raw NFC-spelled lookup happened to resolve (filesystem normalized on lookup): $_cafe_nfc_raw"
else
    qa_warn "KNOWN GAP: raw NFC-spelled lookup MISSED an NFD-stored leaf — caller must NFD-normalize before lookup (orig hex=$(_hexname "$CAFE_NFC"))"
fi
# The authoritative behavior we DO require: normalize-then-find succeeds.
if [ -f "$LANDED/$_cafe_nfc_stored" ]; then
    qa_pass "normalize-before-lookup resolves the NFC-origin café leaf (correct client behavior)"
else
    qa_fail "even normalize-before-lookup MISSED the café leaf: $LANDED/$_cafe_nfc_stored"
fi

# 4c. No normalization COLLISION: the NFC-origin and NFD-origin café files are
# DISTINCT source files (different leaves: _nfc.mov vs _nfd.mov) and must remain
# two distinct landed files with their own (matching) md5s — proves we did not
# collapse two inputs into one on the mount.
if [ -f "$LANDED/$_cafe_nfc_stored" ] && [ -f "$LANDED/$_cafe_nfd_stored" ]; then
    _m_nfc_src="$(md5 -q "$SRC_ROOT/$CAFE_NFC" 2>/dev/null)"
    _m_nfc_dst="$(md5 -q "$LANDED/$_cafe_nfc_stored" 2>/dev/null)"
    _m_nfd_src="$(md5 -q "$SRC_ROOT/$CAFE_NFD" 2>/dev/null)"
    _m_nfd_dst="$(md5 -q "$LANDED/$_cafe_nfd_stored" 2>/dev/null)"
    if [ -n "$_m_nfc_dst" ] && [ "$_m_nfc_src" = "$_m_nfc_dst" ] \
       && [ -n "$_m_nfd_dst" ] && [ "$_m_nfd_src" = "$_m_nfd_dst" ]; then
        qa_pass "no NFC/NFD collision: both café files distinct + byte-identical to source"
    else
        qa_fail "NFC/NFD café content mismatch (nfc src=$_m_nfc_src dst=$_m_nfc_dst | nfd src=$_m_nfd_src dst=$_m_nfd_dst) at $LANDED"
    fi
else
    qa_fail "NFC/NFD café collision/loss: one spelling absent at $LANDED (nfc=$_cafe_nfc_stored nfd=$_cafe_nfd_stored)"
fi

# ---------------------------------------------------------------------------
# CASE SET 5 — findability + snappiness of the pathological-name directory.
# ---------------------------------------------------------------------------
qa_sec "findability + snappy listing of pathological-name dir"

# The landed dir must list near-instantly from the local metadata DB (the user
# must never see the Finder spinner), and the listing must SUCCEED (rc != -1).
list_ms="$(qa_dir_listing_ms "$LANDED")"
if [ "$list_ms" = "-1" ]; then
    qa_fail "readdir FAILED on pathological-name dir: $LANDED"
elif [ "$list_ms" -lt "$QA_SNAPPY_MS" ]; then
    qa_pass "pathological-name dir listed snappily: ${list_ms}ms (< ${QA_SNAPPY_MS}ms) — $LANDED"
else
    qa_fail "pathological-name dir listing TOO SLOW: ${list_ms}ms (>= ${QA_SNAPPY_MS}ms) — $LANDED"
fi

# Every staged leaf must be ENUMERABLE in the landed dir (findable by listing).
# Compare expected (NFD relpaths' leaf) against the actual readdir output.
_listing="$QA_CAT_DIR/landed-listing.txt"
ls -1f "$LANDED" > "$_listing" 2>/dev/null || true
_notfound=0
while IFS="$TAB" read -r _nfdrel _orig _hex; do
    [ -z "$_nfdrel" ] && continue
    # The manifest relpath here IS the leaf (flat corpus). Normalize both sides
    # to NFD before comparing (the mount stores NFD; ls returns NFD).
    _want="$(_nfd "$_nfdrel")"; [ -z "$_want" ] && _want="$_nfdrel"
    if grep -Fxq "$_want" "$_listing" 2>/dev/null; then
        :
    else
        qa_fail "name not findable in listing: orig='$_orig' hex=$_hex (NFD '$_want') in $LANDED"
        _notfound=$((_notfound+1))
    fi
done < "$NAMEMAP"
if [ "$_notfound" -eq 0 ]; then
    qa_pass "all staged pathological names are findable in the landed dir listing"
fi

# ---------------------------------------------------------------------------
# CASE SET 6 — zero-random-Finder-error gate over the whole test window.
# ---------------------------------------------------------------------------
qa_sec "error-signature scan over the test window"

scan="$(qa_error_scan "$mark" "$QA_CATEGORY")"
qa_log "$scan"

# Parse the tally. For THIS category we hold ALL signatures to zero — none of
# the known-open edges (._-heavy STALE ratio; -48 delete-lag) are exercised
# here, so any occurrence is a genuine failure. We name the path from the
# dumped errscan file.
_get() { printf '%s' "$scan" | tr ' ' '\n' | awk -F= -v k="$1" '$1==k{print $2}'; }
e_stale="$(_get STALE)";   e_phantom="$(_get PHANTOM)"
e_70="$(_get E100070)";    e_60="$(_get E100060)"
e_48="$(_get E48)";        e_36="$(_get E36)"
e_5000="$(_get E5000)";    e_perm="$(_get PERM)"
_errfile="$QA_CAT_DIR/errscan-${QA_CATEGORY}.txt"

_scan_clean=1
for _pair in "STALE:$e_stale" "PHANTOM:$e_phantom" "E100070:$e_70" "E100060:$e_60" \
             "E48:$e_48" "E36:$e_36" "E5000:$e_5000" "PERM:$e_perm"; do
    _sig="${_pair%%:*}"; _cnt="${_pair#*:}"
    [ -z "$_cnt" ] && _cnt=0
    if [ "$_cnt" != "0" ]; then
        _firstpath="$(grep -m1 -oE 'path=[^ ]+' "$_errfile" 2>/dev/null | head -n1)"
        [ -z "$_firstpath" ] && _firstpath="(see $_errfile)"
        qa_fail "error signature $_sig=$_cnt in unicode window — offending $_firstpath"
        _scan_clean=0
    fi
done
if [ "$_scan_clean" = "1" ]; then
    qa_pass "zero Finder-error signatures in the unicode test window (clean)"
fi

# Mount must still be healthy after the pathological-name barrage.
if qa_health >/dev/null 2>&1; then
    qa_pass "mount still healthy after pathological-name corpus copy"
else
    qa_fail "mount UNHEALTHY after pathological-name corpus copy (/health not healthy:true)"
fi

# ===========================================================================
# VERDICT
# ===========================================================================
qa_end
# qa_end set $? to QA_FAIL; capture it before any other command clobbers it.
_fails=$QA_FAIL
if [ "$_fails" -eq 0 ]; then
    echo "VERDICT: PASS $QA_CATEGORY"
else
    # Surface a representative offending path/reason in the single-line verdict.
    _reason="$(grep -m1 '^\[FAIL\]' "$QA_CAT_DIR/.summary" 2>/dev/null)"
    echo "VERDICT: FAIL $QA_CATEGORY: $_fails failed assertion(s); see $QA_CAT_DIR (errscan-${QA_CATEGORY}.txt, .summary) — first failure named in the [FAIL] lines above, e.g. landed dir $DEST/unicode-names"
fi

exit 0
