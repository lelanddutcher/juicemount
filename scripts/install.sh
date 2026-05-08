#!/bin/bash
#
# install.sh — install JuiceMount.app to /Applications and optionally set up
# the LaunchAgent for auto-start at login.
#
# Usage:
#   ./scripts/install.sh              # install app only
#   ./scripts/install.sh --launchd    # install app + LaunchAgent

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_SOURCE="$PROJECT_ROOT/build/JuiceMount.app"
APP_DEST="/Applications/JuiceMount.app"
LAUNCH_AGENT_SRC="$PROJECT_ROOT/scripts/com.juicemount.agent.plist"
LAUNCH_AGENT_DEST="$HOME/Library/LaunchAgents/com.juicemount.agent.plist"

if [ ! -d "$APP_SOURCE" ]; then
    echo "ERROR: $APP_SOURCE not found. Run scripts/build-app.sh first."
    exit 1
fi

echo "==> Installing JuiceMount.app to /Applications..."

# If running, quit it first
if pgrep -x JuiceMount >/dev/null; then
    echo "    Quitting running JuiceMount instance..."
    osascript -e 'tell application "JuiceMount" to quit' 2>/dev/null || \
        pkill -x JuiceMount || true
    sleep 1
fi

# Copy
if [ -d "$APP_DEST" ]; then
    rm -rf "$APP_DEST"
fi
cp -R "$APP_SOURCE" "$APP_DEST"
echo "    Installed: $APP_DEST"

# Optionally install LaunchAgent
if [ "$1" = "--launchd" ]; then
    echo ""
    echo "==> Installing LaunchAgent..."
    mkdir -p "$HOME/Library/LaunchAgents"

    # If already loaded, unload first
    if launchctl list | grep -q com.juicemount.agent; then
        launchctl unload "$LAUNCH_AGENT_DEST" 2>/dev/null || true
    fi

    cp "$LAUNCH_AGENT_SRC" "$LAUNCH_AGENT_DEST"
    launchctl load "$LAUNCH_AGENT_DEST"
    echo "    Loaded: $LAUNCH_AGENT_DEST"
    echo "    JuiceMount will now start automatically at login."
else
    echo ""
    echo "    Tip: run with --launchd to also enable start-at-login via LaunchAgent."
    echo "         Or use the in-app preference: JuiceMount → Preferences → General → Start at login"
fi

echo ""
echo "==> Done. Launch with:"
echo "    open $APP_DEST"
