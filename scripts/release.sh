#!/usr/bin/env bash
# Build release binaries of the demo, and optionally publish them as a GitHub release.
#
#   scripts/release.sh v0.1.0             # build dist/ only
#   scripts/release.sh v0.1.0 --publish   # also: signed tag, push the tag, GitHub release
#
# Builds from a clean checkout of HEAD: linux/amd64, linux/arm64, darwin/arm64, darwin/amd64,
# with CGO disabled, so the binaries run on any recent Linux or macOS without Go installed.
# With --publish the tag must not exist yet, or must already point at HEAD.
# Needs: Go (the version in go.mod), git, tar, shasum or sha256sum; gh for --publish.
set -euo pipefail

version="${1:-}"
publish="${2:-}"
[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "usage: $0 vX.Y.Z [--publish]" >&2; exit 2; }
[[ -z "$publish" || "$publish" == "--publish" ]] || { echo "unknown option: $publish" >&2; exit 2; }

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$root"
name="go-redis-road-test"
dist="$root/dist"

[[ -z "$(git status --porcelain)" ]] || { echo "the working tree is not clean; commit or stash first" >&2; exit 1; }
head="$(git rev-parse HEAD)"
if git rev-parse -q --verify "refs/tags/$version" >/dev/null; then
  [[ "$(git rev-parse "$version^{commit}")" == "$head" ]] ||
    { echo "tag $version exists and does not point at HEAD" >&2; exit 1; }
elif [[ -n "$publish" ]]; then
  git tag -s "$version" -m "$name $version"
  git verify-tag "$version"
fi

sum() { if command -v sha256sum >/dev/null; then sha256sum "$@"; else shasum -a 256 "$@"; fi; }

case "$dist" in "$root"/dist) ;; *) echo "unexpected dist dir: $dist" >&2; exit 1 ;; esac
rm -rf -- "$dist"
mkdir -p -- "$dist"

for target in linux/amd64 linux/arm64 darwin/arm64 darwin/amd64; do
  os="${target%/*}" arch="${target#*/}"
  out="$dist/${name}_${version}_${os}_${arch}"
  mkdir -p -- "$out"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags "-s -w" -o "$out/$name" .
  cp -- README.md "$out/"
  tar -C "$dist" -czf "$out.tar.gz" "$(basename "$out")"
  rm -rf -- "$out"
  echo "built $(basename "$out").tar.gz"
done
(cd "$dist" && sum ./*.tar.gz | sed 's# \./# #' >SHA256SUMS)
cat "$dist/SHA256SUMS"

if [[ -n "$publish" ]]; then
  git push origin "refs/tags/$version"
  gh release create "$version" "$dist"/*.tar.gz "$dist/SHA256SUMS" \
    --verify-tag --title "$name $version" \
    --notes "Prebuilt binaries of the demo, built against the go-redis version in go.mod. Download the one for your machine, then: tar xzf ${name}_${version}_<os>_<arch>.tar.gz and run ./${name}_${version}_<os>_<arch>/${name} --help."
fi
