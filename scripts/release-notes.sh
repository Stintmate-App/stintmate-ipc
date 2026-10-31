#!/usr/bin/env bash
# Prints the body of the `## <version>` section of a changelog (without the heading).
# Usage: release-notes.sh <CHANGELOG.md> <version>   (version without the leading "v")
set -euo pipefail
file=${1:?usage: release-notes.sh <CHANGELOG.md> <version>}
version=${2:?usage: release-notes.sh <CHANGELOG.md> <version>}
notes=$(awk -v h="## $version" '
  $0 == h || index($0, h " ") == 1 { found = 1; next }
  found && /^## / { exit }
  found { print }
' "$file")
# Trim blank lines at both ends.
notes=$(printf '%s\n' "$notes" | sed -e '/./,$!d' | sed -e ':a' -e '/^\n*$/{$d;N;ba' -e '}')
if [ -z "$notes" ]; then
  echo "release-notes: $file has no non-empty '## $version' section" >&2
  exit 1
fi
printf '%s\n' "$notes"
