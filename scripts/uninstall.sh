#!/usr/bin/env bash
#
# uninstall.sh — remove JuiceMount's per-user state from this Mac.
#
# Careful by design:
#   1. Stops the app (SIGTERM, wait, escalate only if needed).
#   2. Unmounts the NFS volume and the internal FUSE mount.
#   3. Shows exactly what it WILL remove (with sizes), then asks once.
#   4. If the write spool still holds files, those are UPLOADS THAT NEVER
#      REACHED THE SERVER — deleting them loses data. That removal requires
#      its own explicit confirmation (or --delete-pending-uploads).
#
# What it removes:
#   - LaunchAgent            ~/Library/LaunchAgents/com.juicemount.agent.plist
#   - Sudoers rule           /etc/sudoers.d/juicemount-mount   (needs sudo)
#   - App state              ~/.juicemount        (incl. metadata.db — the
#                            local metadata cache; rebuilt from Redis on a
#                            future reinstall, pin list lives here too)
#   - App support            ~/Library/Application Support/JuiceMount
#                            (incl. spool/ — see the warning above)
#   - Logs                   ~/Library/Logs/JuiceMount
#   - JuiceFS chunk cache    ~/.juicefs/cache     (can be hundreds of GB;
#                            size is shown before you confirm)
#   - UserDefaults           defaults delete com.juicemount.app
#
# What it deliberately leaves alone (remove yourself if you want them gone):
#   - /Applications/JuiceMount.app          (drag to Trash)
#   - the juicefs binary                    (brew uninstall juicefs)
#   - macFUSE                               (System Settings or its installer)
#   - everything on your server             (Redis + MinIO are untouched)
#
# Usage:
#   ./scripts/uninstall.sh                 # interactive
#   ./scripts/uninstall.sh --dry-run       # show the plan, change nothing
#   ./scripts/uninstall.sh --yes           # skip the main confirmation
#   ./scripts/uninstall.sh --yes --delete-pending-uploads
#                                          # fully unattended, EVEN IF the
#                                          # spool holds un-uploaded files

set -u

# Refuse to run as root: $HOME would resolve to /var/root, every removal
# would silently miss the real user's data, and the launchctl gui-domain
# bootout targets the wrong UID. Run as the user who runs JuiceMount.
if [ "$(id -u)" -eq 0 ]; then
    echo "uninstall.sh: run as your normal user, not root (sudo is requested" >&2
    echo "only for the one sudoers-file removal step)." >&2
    exit 2
fi
# set -u catches UNSET, not EMPTY — and an empty $HOME would aim every
# rm -rf below at filesystem-root paths like /.juicemount.
if [ -z "${HOME}" ]; then
    echo "uninstall.sh: $HOME is empty — refusing to construct removal paths." >&2
    exit 2
fi

# ---------------------------------------------------------------------------
# Flags

DRY_RUN=0
ASSUME_YES=0
DELETE_PENDING_UPLOADS=0

for arg in "$@"; do
    case "$arg" in
        --dry-run)                DRY_RUN=1 ;;
        --yes|-y)                 ASSUME_YES=1 ;;
        --delete-pending-uploads) DELETE_PENDING_UPLOADS=1 ;;
        -h|--help)
            sed -n '2,38p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "unknown flag: $arg (try --help)" >&2
            exit 2
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Paths (all per-user; nothing here touches the server)

LAUNCH_AGENT="$HOME/Library/LaunchAgents/com.juicemount.agent.plist"
LAUNCH_AGENT_LABEL="com.juicemount.agent"
SUDOERS_FILE="/etc/sudoers.d/juicemount-mount"
STATE_DIR="$HOME/.juicemount"
APP_SUPPORT_DIR="$HOME/Library/Application Support/JuiceMount"
SPOOL_DIR="$APP_SUPPORT_DIR/spool"
LOG_DIR="$HOME/Library/Logs/JuiceMount"
JUICEFS_CACHE_DIR="$HOME/.juicefs/cache"
DEFAULTS_DOMAIN="com.juicemount.app"
FUSE_INTERNAL="$STATE_DIR/fuse-internal"

# ---------------------------------------------------------------------------
# Helpers

info()  { printf '==> %s\n' "$*"; }
note()  { printf '    %s\n' "$*"; }
warn()  { printf 'WARNING: %s\n' "$*" >&2; }

# Run a destructive command, or just narrate it under --dry-run.
run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '    [dry-run] %s\n' "$*"
    else
        "$@"
    fi
}

du_h() {
    # Human size of a path, or "-" when absent/empty. -x stays on one
    # filesystem: ~/.juicemount contains the live FUSE mountpoint, and
    # without -x the inventory would slowly traverse (and mis-attribute)
    # the entire server volume's size to the local state dir.
    if [ -e "$1" ]; then
        du -sxh "$1" 2>/dev/null | awk '{print $1}'
    else
        echo "-"
    fi
}

confirm() {
    # confirm <prompt> — returns 0 on explicit yes.
    local reply
    if [ ! -t 0 ] && [ ! -r /dev/tty ]; then
        warn "not running interactively and confirmation required — aborting. Use --yes (and --delete-pending-uploads if applicable)."
        exit 1
    fi
    printf '%s [y/N] ' "$1"
    if [ -t 0 ]; then read -r reply; else read -r reply < /dev/tty; fi
    case "$reply" in
        y|Y|yes|YES) return 0 ;;
        *)           return 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# 1. Stop the app if running

info "Checking for a running JuiceMount instance"
if pgrep -x JuiceMount >/dev/null 2>&1; then
    note "JuiceMount is running — sending SIGTERM (graceful quit drains state cleanly)…"
    if [ "$DRY_RUN" -eq 1 ]; then
        note "[dry-run] pkill -TERM -x JuiceMount"
    else
        pkill -TERM -x JuiceMount 2>/dev/null || true
        # Wait up to 20 s for a clean exit (the app may be finishing writes).
        waited=0
        while pgrep -x JuiceMount >/dev/null 2>&1 && [ "$waited" -lt 20 ]; do
            sleep 1
            waited=$((waited + 1))
        done
        if pgrep -x JuiceMount >/dev/null 2>&1; then
            warn "still running after ${waited}s — sending SIGKILL"
            pkill -KILL -x JuiceMount 2>/dev/null || true
            sleep 2
        else
            note "stopped after ${waited}s."
        fi
    fi
else
    note "not running."
fi

# ---------------------------------------------------------------------------
# 2. Unmount NFS volume(s) + the internal FUSE mount

unmount_path() {
    # unmount_path <mountpoint> — retry loop; force-unmount via diskutil.
    local mp="$1" attempt
    for attempt in 1 2 3; do
        if ! mount | grep -qF " on $mp "; then
            return 0
        fi
        note "unmounting $mp (attempt $attempt)…"
        if [ "$DRY_RUN" -eq 1 ]; then
            note "[dry-run] diskutil unmount force $mp"
            return 0
        fi
        if diskutil unmount force "$mp" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    if mount | grep -qF " on $mp "; then
        warn "could not unmount $mp — close any apps using it and re-run. Continuing without deleting mounted state."
        return 1
    fi
    return 0
}

info "Unmounting JuiceMount volumes"

# NFS volumes served by the local JuiceMount NFS server (127.0.0.1:/).
# (loop over whatever is mounted; usually exactly one /Volumes/<name>)
mount | sed -nE 's|^127\.0\.0\.1:/ on (.*) \(nfs.*|\1|p' | while read -r nfs_mp; do
    [ -n "$nfs_mp" ] && unmount_path "$nfs_mp" || true
done

# The internal JuiceFS FUSE mount.
if mount | grep -qF " on $FUSE_INTERNAL "; then
    unmount_path "$FUSE_INTERNAL" || true
else
    note "no FUSE mount at $FUSE_INTERNAL."
fi

# ---------------------------------------------------------------------------
# 3. Inventory — show the plan before touching anything

info "The following will be removed:"
printf '    %-58s %8s\n' "PATH" "SIZE"
printf '    %-58s %8s\n' "$LAUNCH_AGENT" "$(du_h "$LAUNCH_AGENT")"
printf '    %-58s %8s\n' "$SUDOERS_FILE (needs sudo)" "$(du_h "$SUDOERS_FILE")"
printf '    %-58s %8s\n' "$STATE_DIR  (metadata.db + pin db live here)" "$(du_h "$STATE_DIR")"
printf '    %-58s %8s\n' "$APP_SUPPORT_DIR" "$(du_h "$APP_SUPPORT_DIR")"
printf '    %-58s %8s\n' "$LOG_DIR" "$(du_h "$LOG_DIR")"
printf '    %-58s %8s\n' "$JUICEFS_CACHE_DIR  (JuiceFS chunk cache)" "$(du_h "$JUICEFS_CACHE_DIR")"
printf '    %-58s %8s\n' "UserDefaults domain $DEFAULTS_DOMAIN" "-"
echo ""
note "metadata.db is only a local cache of the server's metadata — it is"
note "rebuilt from Redis on a future install. Your media on the server is"
note "NOT touched by this script."
echo ""
note "Left alone: /Applications/JuiceMount.app (drag to Trash yourself),"
note "the Homebrew juicefs binary, macFUSE, and everything server-side."
echo ""

# Spool check — pending uploads are data that exists ONLY on this Mac.
SPOOL_FILES=0
if [ -d "$SPOOL_DIR" ]; then
    SPOOL_FILES=$(find "$SPOOL_DIR" -type f ! -name 'manifest.log' 2>/dev/null | wc -l | tr -d ' ')
fi
if [ "$SPOOL_FILES" -gt 0 ]; then
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    warn "the write spool still contains $SPOOL_FILES file(s), $(du_h "$SPOOL_DIR") at:"
    warn "  $SPOOL_DIR"
    warn "These are writes that were ACKed to Finder but NEVER finished"
    warn "uploading to your server. Deleting them is PERMANENT DATA LOSS."
    warn "To drain them instead: reinstall/relaunch JuiceMount, enable the"
    warn "spool, and wait for 'Pending uploads' to reach zero."
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo ""
fi

if [ "$DRY_RUN" -eq 1 ]; then
    info "--dry-run: nothing was changed. Re-run without --dry-run to uninstall."
    exit 0
fi

# ---------------------------------------------------------------------------
# 4. One confirmation for the plan…

if [ "$ASSUME_YES" -ne 1 ]; then
    confirm "Proceed with uninstall?" || { echo "aborted."; exit 1; }
fi

# …plus a SEPARATE, explicit confirmation if un-uploaded spool data exists.
REMOVE_APP_SUPPORT=1
if [ "$SPOOL_FILES" -gt 0 ]; then
    if [ "$DELETE_PENDING_UPLOADS" -eq 1 ]; then
        warn "--delete-pending-uploads given: deleting $SPOOL_FILES un-uploaded file(s)."
    elif confirm "DELETE the $SPOOL_FILES un-uploaded file(s) in the spool (PERMANENT)?"; then
        :
    else
        note "keeping $APP_SUPPORT_DIR (contains the spool). Everything else proceeds."
        REMOVE_APP_SUPPORT=0
    fi
fi

# ---------------------------------------------------------------------------
# 5. LaunchAgent

info "LaunchAgent"
if [ -f "$LAUNCH_AGENT" ]; then
    # Modern bootout first; legacy unload as fallback. Both are no-ops if
    # the agent isn't loaded.
    run launchctl bootout "gui/$(id -u)/$LAUNCH_AGENT_LABEL" 2>/dev/null || true
    run launchctl unload "$LAUNCH_AGENT" 2>/dev/null || true
    run rm -f "$LAUNCH_AGENT"
    note "removed $LAUNCH_AGENT"
else
    note "not installed."
fi

# ---------------------------------------------------------------------------
# 6. Sudoers rule (root-owned; needs sudo)

info "Sudoers rule"
if [ -f "$SUDOERS_FILE" ] || sudo -n test -f "$SUDOERS_FILE" 2>/dev/null; then
    if sudo -n true 2>/dev/null; then
        run sudo rm -f "$SUDOERS_FILE"
        note "removed $SUDOERS_FILE"
    elif [ -t 0 ] && [ "$ASSUME_YES" -ne 1 ]; then
        note "removing $SUDOERS_FILE requires your admin password:"
        if run sudo rm -f "$SUDOERS_FILE"; then
            note "removed $SUDOERS_FILE"
        else
            warn "could not remove $SUDOERS_FILE — remove it manually: sudo rm $SUDOERS_FILE"
        fi
    else
        warn "skipping $SUDOERS_FILE (no passwordless sudo in unattended mode)."
        warn "Remove it manually: sudo rm $SUDOERS_FILE"
    fi
else
    note "not present."
fi

# ---------------------------------------------------------------------------
# 7. Per-user state, app support, logs, chunk cache, defaults

remove_dir() {
    # remove_dir <path> — refuses to delete a path that is still a mountpoint
    # (or contains one), so a failed unmount can't turn into data loss.
    local p="$1"
    if [ ! -e "$p" ]; then
        note "$p — not present."
        return 0
    fi
    if mount | grep -qF " on $p " || mount | grep -qF " on $p/"; then
        warn "$p still contains an active mount — NOT deleting. Unmount and re-run."
        return 1
    fi
    run rm -rf "$p"
    note "removed $p"
}

info "Application state"
remove_dir "$STATE_DIR" || true

info "Application Support"
if [ "$REMOVE_APP_SUPPORT" -eq 1 ]; then
    remove_dir "$APP_SUPPORT_DIR" || true
else
    note "kept (un-uploaded spool files preserved)."
fi

info "Logs"
remove_dir "$LOG_DIR" || true

info "JuiceFS chunk cache"
remove_dir "$JUICEFS_CACHE_DIR" || true

info "UserDefaults"
if defaults read "$DEFAULTS_DOMAIN" >/dev/null 2>&1; then
    run defaults delete "$DEFAULTS_DOMAIN" 2>/dev/null || true
    note "deleted defaults domain $DEFAULTS_DOMAIN"
else
    note "no defaults stored."
fi

# ---------------------------------------------------------------------------

echo ""
info "Uninstall complete."
note "Still installed (by design):"
note "  /Applications/JuiceMount.app — drag to Trash to finish removal"
note "  juicefs binary               — brew uninstall juicefs"
note "  macFUSE                      — remove via System Settings / its installer"
note "Server-side data (Redis + MinIO) was not touched."
