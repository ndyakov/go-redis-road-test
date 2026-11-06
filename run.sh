#!/usr/bin/env bash
# Build and run the demo against any go-redis: a released version, a branch or
# commit, or a local checkout. Your go.mod is not changed: the build happens in
# a scratch copy.
#
#   ./run.sh                          # the version in go.mod
#   ./run.sh v9.24.0 --web 127.0.0.1:8080
#   ./run.sh latest --scripted
#   ./run.sh master --web 127.0.0.1:8080
#   ./run.sh ../go-redis --web 127.0.0.1:8080     # a local checkout (unreleased code)
#
# Everything after the version is passed to the demo.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
want="${1:-}"
label=""
if [[ -n "$want" && "$want" != -* ]]; then shift; else want=""; fi

work="$(mktemp -d "${TMPDIR:-/tmp}/go-redis-engines.XXXXXX")"
case "$work" in
  "${TMPDIR:-/tmp}"/go-redis-engines.?*) ;;
  *) echo "run.sh: unexpected scratch dir: $work" >&2; exit 1 ;;
esac
cleanup() { rm -rf -- "$work"; }
trap cleanup EXIT

# copy the sources, not a previous build
mkdir -p -- "$work/web"
cp -p -- "$here"/web/* "$work/web/"
cp -p -- "$here"/*.go "$here/go.mod" "$here/go.sum" "$work/"

cd "$work"
if [[ -n "$want" ]]; then
  if [[ -d "$want" ]]; then
    src="$(cd "$want" && pwd)"
    [[ -f "$src/go.mod" ]] && grep -q '^module github.com/redis/go-redis/v9$' "$src/go.mod" ||
      { echo "run.sh: $want is not a go-redis checkout" >&2; exit 1; }
    go mod edit -replace "github.com/redis/go-redis/v9=$src"
    # go-redis's OTel module from the same checkout (otel.go)
    go mod edit -replace "github.com/redis/go-redis/extra/redisotel-native/v9=$src/extra/redisotel-native"
    label="local $(git -C "$src" describe --tags --match 'v[0-9]*' --always --dirty 2>/dev/null || echo checkout)"
  else
    [[ "$want" =~ ^[A-Za-z0-9._/+-]+$ ]] || { echo "run.sh: bad version: $want" >&2; exit 1; }
    go get "github.com/redis/go-redis/v9@$want" >/dev/null
    # go-redis's OTel module (otel.go) at the SAME commit: a branch name like
    # "master" can resolve to an older commit for the submodule (proxy cache),
    # so pin it to go-redis's resolved version (a tag, or a pseudo-version's commit)
    gv="$(go list -m -f '{{.Version}}' github.com/redis/go-redis/v9)"
    if [[ "$gv" =~ -[0-9]{14}-([0-9a-f]{12})$ ]]; then
      orev="${BASH_REMATCH[1]}"
    else
      orev="$gv"
    fi
    go get "github.com/redis/go-redis/extra/redisotel-native/v9@$orev" >/dev/null
  fi
  go mod tidy >/dev/null
fi

# options that only some versions have: pick the adapter by looking at the source
dir="$(go list -m -f '{{if .Replace}}{{.Replace.Dir}}{{else}}{{.Dir}}{{end}}' github.com/redis/go-redis/v9)"
tags=""
if grep -q '^[[:space:]]*FullDuplexPipes[[:space:]]\+int' "$dir"/autopipeline*.go 2>/dev/null; then
  tags="goredis_fdpipes"
fi
if grep -q '^[[:space:]]*PipelineMinIdleConns[[:space:]]\+int' "$dir"/options.go 2>/dev/null; then
  tags="${tags:+$tags,}goredis_pipeminidle"
fi
ver="$(go list -m -f '{{if .Replace}}{{.Replace.Path}}{{else}}{{.Version}}{{end}}' github.com/redis/go-redis/v9)"
over="$(go list -m -f '{{if .Replace}}{{.Replace.Path}}{{else}}{{.Version}}{{end}}' github.com/redis/go-redis/extra/redisotel-native/v9)"
echo "run.sh: go-redis $ver${tags:+ (tags: $tags)}, redisotel-native $over" >&2

go build ${tags:+-tags "$tags"} ${label:+-ldflags "-X 'main.goRedisLabel=$label'"} -o "$work/engines" .
"$work/engines" "$@"
