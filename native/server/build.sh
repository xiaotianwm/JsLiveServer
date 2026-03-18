#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"
BUILD_SYSTEM_DIR="${BUILD_DIR}/cmake"

if ! command -v cmake >/dev/null 2>&1; then
  echo "cmake was not found in PATH. Install CMake first, then rerun this script." >&2
  exit 1
fi

mkdir -p "${BUILD_SYSTEM_DIR}"

cmake -S "${PROJECT_ROOT}" -B "${BUILD_SYSTEM_DIR}" -DCMAKE_BUILD_TYPE=Release
cmake --build "${BUILD_SYSTEM_DIR}" --config Release --parallel

echo "CMake build completed. Packaged output directory: ${BUILD_DIR}"
