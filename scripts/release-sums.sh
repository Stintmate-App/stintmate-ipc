#!/usr/bin/env bash
# Writes SHA256SUMS (sha256sum format, bare file names) for the given files into <dir>.
# Usage: release-sums.sh <dir> <file>...   (files are names inside <dir>)
set -euo pipefail
dir=${1:?usage: release-sums.sh <dir> <file>...}
shift
[ $# -gt 0 ] || { echo "release-sums: no files given" >&2; exit 1; }
(cd "$dir" && sha256sum --text -- "$@" > SHA256SUMS)
