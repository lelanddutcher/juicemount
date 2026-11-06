#!/bin/bash
# Install a verified JuiceMount bundle without stopping any running component.
# Defaults: build/JuiceMount.app -> /Applications/JuiceMount.app.
# Use --source /absolute/App.app --destination /absolute/App.app for owned tests.
set -euo pipefail
umask 077
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
APP_SOURCE="$PROJECT_ROOT/build/JuiceMount.app"
APP_DEST=/Applications/JuiceMount.app
LAUNCHD=0
WORK=''
LOCK=''
die() { echo "install.sh: $*" >&2; exit 1; }
finish() {
    local rc=$?
    trap - EXIT
    if [ -n "$WORK" ]; then echo "Installer artifacts retained: $WORK" >&2; fi
    if [ -n "$LOCK" ]; then rmdir "$LOCK" || { echo "Installer lock retained: $LOCK" >&2; rc=1; }; fi
    exit "$rc"
}
trap finish EXIT
while [ "$#" -gt 0 ]; do
    case "$1" in
        --source|--destination)
            [ "$#" -ge 2 ] || die "$1 requires an absolute bundle path"
            if [ "$1" = --source ]; then APP_SOURCE="$2"; else APP_DEST="$2"; fi
            shift 2 ;;
        --launchd) LAUNCHD=1; shift ;;
        *) die "usage: scripts/install.sh [--source /path/App.app] [--destination /path/App.app] [--launchd]" ;;
    esac
done
# Do not resolve a caller's symlink, '..', or alias into a different target.
# Every existing component, including the bundle root, must be real.
check_path() {
    local path="$1" rest component current=''
    [[ "$path" = /* && "$path" != *[[:cntrl:]]* && "$path" != */ && "$path" != *//* ]] || die "unsafe absolute path: $path"
    rest="${path#/}"
    while [ -n "$rest" ]; do
        component="${rest%%/*}"
        [[ "$component" != . && "$component" != .. && -n "$component" ]] || die "unsafe path component"
        current="$current/$component"
        [ ! -L "$current" ] || die "symlink path refused: $current"
        if [[ "$rest" = */* ]]; then
            [ -d "$current" ] || die "missing parent directory: $current"
            rest="${rest#*/}"
        else rest=''; fi
    done
}
check_path "$APP_SOURCE"
check_path "$APP_DEST"
[[ "$APP_SOURCE" = *.app && "$APP_DEST" = *.app ]] || die "source and destination must end in .app"
[ -d "$APP_SOURCE" ] || die "source bundle missing: $APP_SOURCE"
[[ "$APP_SOURCE" != "$APP_DEST" && "$APP_SOURCE/" != "$APP_DEST/"* && "$APP_DEST/" != "$APP_SOURCE/"* ]] || die "self-copy or nested source/destination refused"
[ ! -e "$APP_DEST" ] || [ -d "$APP_DEST" ] || die "destination is not a bundle directory"
[ "$LAUNCHD" = 0 ] || [ "$APP_DEST" = /Applications/JuiceMount.app ] || die "--launchd requires /Applications/JuiceMount.app"
PARENT="${APP_DEST%/*}"
[ -n "$PARENT" ] || die "filesystem-root destination refused"

# ps failure or malformed output is uncertainty, never evidence of absence.
# The privileged eject helper is deliberately not stopped or re-registered.
require_stopped() {
    local processes line command count=0
    processes="$(ps -axww -o pid=,comm=)" || die "cannot query processes; Stop/Quit must be verified before installation"
    while IFS= read -r line; do
        [[ "$line" =~ ^[[:space:]]*[0-9]+[[:space:]]+(.+)$ ]] || die "unrecognized process query output"
        command="${BASH_REMATCH[1]}"
        count=$((count + 1))
        case "$command" in
            JuiceMount|*/JuiceMount|juicemount-service|*/juicemount-service|juicemount-mount-probe|*/juicemount-mount-probe|*/Contents/MacOS/juicefs)
                die "JuiceMount is still running; resolve pending uploads, then use Stop and Quit before installation" ;;
        esac
    done <<< "$processes"
    [ "$count" -gt 0 ] || die "empty process query"
}
# Match the existing product helper's Developer ID requirement. An ad-hoc or
# other-team build cannot operate that helper and must not replace the app.
REQUIREMENT='anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = "PKUE74YS72" and identifier "com.juicemount.app" and !(entitlement["com.apple.security.get-task-allow"] exists)'
verify_bundle() {
    local app="$1" identifier executable
    [ -f "$app/Contents/Info.plist" ] && [ ! -L "$app/Contents" ] && [ ! -L "$app/Contents/Info.plist" ] || die "invalid bundle layout: $app"
    identifier="$(plutil -extract CFBundleIdentifier raw -o - "$app/Contents/Info.plist")" || die "cannot read bundle identifier"
    executable="$(plutil -extract CFBundleExecutable raw -o - "$app/Contents/Info.plist")" || die "cannot read bundle executable"
    [ "$identifier" = com.juicemount.app ] && [ "$executable" = JuiceMount ] || die "unexpected bundle identity"
    [ -d "$app/Contents/MacOS" ] && [ ! -L "$app/Contents/MacOS" ] && [ -f "$app/Contents/MacOS/JuiceMount" ] && [ ! -L "$app/Contents/MacOS/JuiceMount" ] && [ -x "$app/Contents/MacOS/JuiceMount" ] || die "invalid app executable"
    codesign --verify --deep --strict -R "=$REQUIREMENT" "$app" || die "JuiceMount Developer ID signature verification failed: $app"
}
identity() { stat -f '%d:%i' "$1"; }
# macOS mv treats an existing directory as a container. renamex_np with
# RENAME_EXCL (sys/stdio.h, 0x4) instead refuses every existing destination.
# JXA is system-provided; this calls libc only, with no application/Apple Event.
rename_exclusive() {
    osascript -l JavaScript -e '
function run(argv) {
    if (argv.length !== 2) throw new Error("exclusive rename requires two paths");
    ObjC.bindFunction("renamex_np", ["int", ["char *", "char *", "unsigned int"]]);
    if ($.renamex_np(argv[0], argv[1], 4) !== 0)
        throw new Error("exclusive rename refused; source and destination retained");
}
' "$1" "$2"
}
require_stopped
verify_bundle "$APP_SOURCE"
lock_path="$PARENT/.${APP_DEST##*/}.install-lock"
mkdir -m 0700 "$lock_path" || die "another install or retained lock exists: $lock_path"
LOCK="$lock_path"
WORK="$(mktemp -d "$PARENT/.juicemount-install.XXXXXXXX")" || die "cannot create destination-filesystem staging directory"
STAGED="$WORK/new.app"
BACKUP="$WORK/previous.app"
OLD_ID=''
if [ -e "$APP_DEST" ]; then OLD_ID="$(identity "$APP_DEST")" || die "cannot identify previous bundle"; fi
# Never clean this directory on failure: partial copies are diagnostic evidence.
ditto "$APP_SOURCE" "$STAGED" || die "copy failed; installed bundle was not changed"
verify_bundle "$STAGED"
diff -qr "$APP_SOURCE" "$STAGED" || die "staged copy differs from source; installed bundle was not changed"
NEW_ID="$(identity "$STAGED")" || die "cannot identify staged bundle"
require_stopped
check_path "$APP_DEST"
if [ -n "$OLD_ID" ]; then
    [ -d "$APP_DEST" ] && [ "$(identity "$APP_DEST")" = "$OLD_ID" ] || die "installed bundle changed while staging"
    rename_exclusive "$APP_DEST" "$BACKUP" || die "cannot retain previous bundle; inspect retained paths"
    [ ! -e "$APP_DEST" ] && [ ! -L "$APP_DEST" ] && [ "$(identity "$BACKUP")" = "$OLD_ID" ] || die "previous-bundle move was not verified"
else
    [ ! -e "$APP_DEST" ] && [ ! -L "$APP_DEST" ] || die "destination appeared while staging"
fi
# Exclusive native renames refuse concurrent destination creation atomically.
# Do not remove a conflicting destination, including during failed promotion.
if ! rename_exclusive "$STAGED" "$APP_DEST" || [ -e "$STAGED" ] || [ -L "$APP_DEST" ] || [ "$(identity "$APP_DEST")" != "$NEW_ID" ]; then
    if [ -n "$OLD_ID" ] && [ ! -e "$APP_DEST" ] && [ ! -L "$APP_DEST" ]; then
        rename_exclusive "$BACKUP" "$APP_DEST" || die "promotion and restoration failed; previous bundle retained at $BACKUP"
        [ ! -e "$BACKUP" ] && [ "$(identity "$APP_DEST")" = "$OLD_ID" ] || die "restoration not verified; inspect retained paths"
        die "promotion failed; previous bundle restored, failed stage retained"
    fi
    die "promotion not verified; no destination overwritten; inspect $WORK and $APP_DEST"
fi
echo "Installed verified JuiceMount: $APP_DEST"
[ -z "$OLD_ID" ] || echo "Previous bundle retained: $BACKUP (not permission to replay older app-state schemas)"

# Optional existing managed-start facility. Never unload an existing agent.
# Registration is deliberately after successful app promotion and canonical-only.
if [ "$LAUNCHD" = 1 ]; then
    [ -n "${HOME:-}" ] || die "HOME is missing"
    AGENT_DEST="$HOME/Library/LaunchAgents/com.juicemount.agent.plist"
    check_path "$HOME"
    [ ! -e "$AGENT_DEST" ] && [ ! -L "$AGENT_DEST" ] || die "LaunchAgent already exists; manage it separately (app installed)"
    agents="$(launchctl list)" || die "cannot query LaunchAgents (app installed)"
    [[ "$agents" != *com.juicemount.agent* ]] || die "LaunchAgent already loaded; manage it separately (app installed)"
    for directory in "$HOME/Library" "$HOME/Library/LaunchAgents" "$HOME/Library/Logs" "$HOME/Library/Logs/JuiceMount"; do
        check_path "$directory"
        [ -d "$directory" ] || mkdir "$directory"
    done
    check_path "$AGENT_DEST"
    check_path "$HOME/Library/Logs/JuiceMount"
    cp "$PROJECT_ROOT/scripts/com.juicemount.agent.plist" "$WORK/agent.plist"
    plutil -replace StandardOutPath -string "$HOME/Library/Logs/JuiceMount/agent.log" "$WORK/agent.plist"
    plutil -replace StandardErrorPath -string "$HOME/Library/Logs/JuiceMount/agent.err" "$WORK/agent.plist"
    rename_exclusive "$WORK/agent.plist" "$AGENT_DEST"
    [ ! -e "$WORK/agent.plist" ] || die "LaunchAgent destination appeared (app installed)"
    launchctl load "$AGENT_DEST" || die "LaunchAgent load failed (app installed; artifacts retained)"
fi
echo "Launch when ready: open \"$APP_DEST\""
