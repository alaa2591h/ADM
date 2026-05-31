#!/usr/bin/env bash
# install-native-host.sh
# Installs the APEX native messaging host manifest for Chromium and/or Firefox.
#
# Usage:
#   ./install-native-host.sh [--binary /path/to/apex-native-host]
#
# The script auto-detects the OS and installs to the correct system directory.
# Run with sudo on Linux if installing system-wide, or without for user-local.

set -euo pipefail

BINARY_PATH="${1:-$(which apex-native-host 2>/dev/null || echo /usr/local/bin/apex-native-host)}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST_NAME="com.apex.downloadmanager"

if [[ ! -x "$BINARY_PATH" ]]; then
  echo "[warn] Native host binary not found at: $BINARY_PATH"
  echo "       Build it first: cargo build -p apex-native-host --release"
  echo "       Then copy to:   $BINARY_PATH"
fi

case "$(uname -s)" in
  Linux)
    CHROMIUM_DIR="$HOME/.config/chromium/NativeMessagingHosts"
    CHROME_DIR="$HOME/.config/google-chrome/NativeMessagingHosts"
    FIREFOX_DIR="$HOME/.mozilla/native-messaging-hosts"
    ;;
  Darwin)
    CHROMIUM_DIR="$HOME/Library/Application Support/Chromium/NativeMessagingHosts"
    CHROME_DIR="$HOME/Library/Application Support/Google/Chrome/NativeMessagingHosts"
    FIREFOX_DIR="$HOME/Library/Application Support/Mozilla/NativeMessagingHosts"
    ;;
  *)
    echo "Unsupported OS: $(uname -s)"
    exit 1
    ;;
esac

install_manifest() {
  local src="$1"
  local dest_dir="$2"
  mkdir -p "$dest_dir"
  sed "s|/usr/local/bin/apex-native-host|$BINARY_PATH|g" \
    "$src" > "$dest_dir/$HOST_NAME.json"
  echo "[ok] Installed manifest: $dest_dir/$HOST_NAME.json"
}

install_manifest "$SCRIPT_DIR/com.apex.downloadmanager.chromium.json" "$CHROMIUM_DIR"
install_manifest "$SCRIPT_DIR/com.apex.downloadmanager.chromium.json" "$CHROME_DIR"
install_manifest "$SCRIPT_DIR/com.apex.downloadmanager.firefox.json"  "$FIREFOX_DIR"

echo ""
echo "Done. Restart your browser to pick up the new manifest."
echo "Binary expected at: $BINARY_PATH"
