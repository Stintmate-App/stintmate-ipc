#!/usr/bin/env bash
# Prints a table of the SHA-256 of the assets in <dir> and, when <published-tag> is given, the
# hashes of that published release's assets, marking the ones that differ. A difference is
# reported, never an error: rebuilt binaries need not be byte-identical.
# Usage: release-report.sh <dir> [<published-tag>]
set -euo pipefail
dir=${1:?usage: release-report.sh <dir> [<published-tag>]}
pub=${2:-}
declare -A theirs=()
if [ -n "$pub" ]; then
  while read -r name digest; do theirs[$name]=${digest#sha256:}; done < <(
    gh release view "$pub" --json assets --jq '.assets[] | "\(.name) \(.digest // "none")"')
fi
printf '%-28s %-64s %s\n' asset sha256 "published ($pub)"
for f in "$dir"/*; do
  name=$(basename "$f")
  mine=$(sha256sum "$f" | cut -d' ' -f1)
  if [ -z "$pub" ]; then mark=-
  elif [ -z "${theirs[$name]:-}" ]; then mark="not in the published release"
  elif [ "${theirs[$name]}" = none ]; then mark="published digest unavailable"
  elif [ "${theirs[$name]}" = "$mine" ]; then mark=same
  else mark="DIFFERS ${theirs[$name]}"; fi
  printf '%-28s %-64s %s\n' "$name" "$mine" "$mark"
done
