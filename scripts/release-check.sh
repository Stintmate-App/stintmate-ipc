#!/usr/bin/env bash
# Checks a release tag before anything is built. Run it in a full clone (tags and origin/main
# fetched); it reads the tag's files with `git show`, so it does not need the tag checked out.
# Usage: release-check.sh <tag> <dry_run: true|false>
# Prints key=value lines (version, sha, published) on stdout; refusals go to stderr, exit 1.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
tag=${1:?usage: release-check.sh <tag> <dry_run>}
dry_run=${2:-true}
fail() { echo "release-check: $*" >&2; exit 1; }

case $tag in
  *+*) fail "'$tag' is a metadata tag: a '+' tag changes no code and has no release assets. Release the tag without the '+' part." ;;
esac
[[ $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "'$tag' is not a release tag (expected vMAJOR.MINOR.PATCH)"
version=${tag#v}

sha=$(git rev-parse --verify --quiet "refs/tags/$tag^{commit}") || fail "tag '$tag' does not exist"
main_ref=${RELEASE_MAIN_REF:-origin/main}
git merge-base --is-ancestor "$sha" "$main_ref" || fail "tag '$tag' ($sha) is not an ancestor of $main_ref"

cargo_version=$(git show "$sha:Cargo.toml" | awk '
  /^\[workspace\.package\]/ { f = 1; next }
  /^\[/ { f = 0 }
  f && /^version[ \t]*=/ { gsub(/^[^"]*"|".*$/, ""); print; exit }
')
[ "$cargo_version" = "$version" ] || fail "tag '$tag' differs from the workspace version in Cargo.toml (v${cargo_version:-?})"

git show "$sha:CHANGELOG.md" > "${TMPDIR:-/tmp}/release-check-changelog.$$"
trap 'rm -f "${TMPDIR:-/tmp}/release-check-changelog.$$"' EXIT
"$here/release-notes.sh" "${TMPDIR:-/tmp}/release-check-changelog.$$" "$version" > /dev/null \
  || fail "CHANGELOG.md has no '## $version' section at '$tag'"

published=false
if out=$(gh release view "$tag" --json isDraft --jq .isDraft 2>&1); then
  [ "$out" = "false" ] && published=true
elif ! grep -qi 'not found' <<< "$out"; then
  fail "cannot read the releases of this repository: $out"
fi
if [ "$published" = true ] && [ "$dry_run" != true ]; then
  fail "a published release for '$tag' exists; it is never replaced (run with dry_run to test)"
fi

echo "version=$version"
echo "sha=$sha"
echo "published=$published"
