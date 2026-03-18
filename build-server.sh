#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_DIR="${ROOT_DIR}/native/server"
LINUX_FFMPEG="${ROOT_DIR}/gosrc/server/runtime/ffmpeg/linux-amd64/ffmpeg"

if ! command -v cmake >/dev/null 2>&1; then
  echo "cmake was not found in PATH. Install build-essential and cmake first." >&2
  exit 1
fi

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "ffmpeg was not found in PATH. Install ffmpeg first." >&2
  exit 1
fi

chmod +x "${SERVER_DIR}/build.sh"
if [ -f "${LINUX_FFMPEG}" ]; then
  chmod +x "${LINUX_FFMPEG}"
fi

cd "${SERVER_DIR}"
./build.sh

echo
echo "Build completed."
echo "Binary: ${SERVER_DIR}/build/jslive_native_server"
echo "Config: ${SERVER_DIR}/build/server.conf"
