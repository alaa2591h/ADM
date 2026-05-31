#!/usr/bin/env bash
# =============================================================================
#  upx-compress.sh — Optional UPX binary compression step
#
#  Called automatically by the GitHub Actions workflow before Inno Setup
#  packaging. Safe to skip if UPX is not installed.
#
#  Usage:
#    ./installer/upx-compress.sh [build_dir]
#
#  Environment:
#    UPX_SKIP=1   — set to skip compression entirely
#    UPX_LEVEL    — compression level flag (default: --best)
# =============================================================================

set -euo pipefail

BUILD_DIR="${1:-build/Release}"
UPX_LEVEL="${UPX_LEVEL:---best}"
UPX_SKIP="${UPX_SKIP:-0}"

BINARIES=(
    "apex-daemon.exe"
    "apex.exe"
    "apex-native-host.exe"
)

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [[ "$UPX_SKIP" == "1" ]]; then
    echo "[upx] UPX_SKIP=1 — skipping compression."
    exit 0
fi

if ! command -v upx &>/dev/null; then
    echo "[upx] UPX not found in PATH — skipping (non-fatal)."
    exit 0
fi

UPX_VER=$(upx --version 2>&1 | head -1)
echo "[upx] Using: $UPX_VER"
echo "[upx] Level: $UPX_LEVEL"
echo "[upx] Dir  : $BUILD_DIR"
echo ""

# ── Compress each binary ──────────────────────────────────────────────────────
COMPRESSED=0
SKIPPED=0

for BIN in "${BINARIES[@]}"; do
    FULL_PATH="$BUILD_DIR/$BIN"

    if [[ ! -f "$FULL_PATH" ]]; then
        echo "[upx] SKIP (not found): $BIN"
        ((SKIPPED++)) || true
        continue
    fi

    BEFORE=$(stat -c%s "$FULL_PATH" 2>/dev/null || stat -f%z "$FULL_PATH")

    if upx "$UPX_LEVEL" --strip-relocs=0 "$FULL_PATH" 2>&1 | grep -q "already packed"; then
        echo "[upx] SKIP (already packed): $BIN"
        ((SKIPPED++)) || true
        continue
    fi

    AFTER=$(stat -c%s "$FULL_PATH" 2>/dev/null || stat -f%z "$FULL_PATH")
    RATIO=$(echo "scale=1; 100 - ($AFTER * 100 / $BEFORE)" | bc)

    printf "[upx] %-32s  %7d → %7d bytes  (-%s%%)\n" \
        "$BIN" "$BEFORE" "$AFTER" "$RATIO"
    ((COMPRESSED++)) || true
done

echo ""
echo "[upx] Done. Compressed: $COMPRESSED  Skipped: $SKIPPED"
