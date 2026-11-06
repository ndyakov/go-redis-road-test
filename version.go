package main

import (
	"runtime/debug"
	"strings"
)

// goRedisLabel is set by run.sh (-ldflags -X) for local checkouts, which Go
// records only as "(devel)": "local" plus git describe of the checkout.
var goRedisLabel string

// goRedisVersion is the go-redis this binary was built with.
func goRedisVersion() string {
	if goRedisLabel != "" {
		return goRedisLabel
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "go-redis (unknown version)"
	}
	for _, dep := range bi.Deps {
		if dep.Path != "github.com/redis/go-redis/v9" {
			continue
		}
		if r := dep.Replace; r != nil {
			if r.Version != "" {
				return r.Version
			}
			return "local " + shortPath(r.Path)
		}
		return dep.Version
	}
	return "go-redis (unknown version)"
}

func shortPath(p string) string {
	parts := strings.Split(strings.TrimRight(p, "/"), "/")
	if len(parts) > 2 {
		parts = parts[len(parts)-2:]
	}
	return strings.Join(parts, "/")
}
