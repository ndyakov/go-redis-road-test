package main

// The one-line start of each engine, the same lines as on the talk slides.
// startEngine (engines.go) uses the same API calls; these standalone examples
// keep them in one place to read. Each accepts a client configured by the caller
// for the Redis address, credentials and optional TLS. Create it once and reuse
// it across requests; the caller closes it when finished:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", PoolSize: 500})
//	defer rdb.Close()
//	value, err := usePool(ctx, rdb)

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Step 1: a pool. Each call takes a connection, sends one command, waits.
func usePool(ctx context.Context, rdb *redis.Client) (string, error) {
	return rdb.Get(ctx, "user:42").Result()
}

// Step 2: batching. Callers that arrive while a batch is on the road share the next one.
func useBatching(ctx context.Context, rdb *redis.Client) (string, error) {
	ap, err := rdb.AutoPipeline()
	if err != nil {
		return "", err
	}
	defer ap.Close()
	return ap.Get(ctx, "user:42").Result()
}

// Step 2, async face: calls return at once; Result waits.
func useBatchingAsync(ctx context.Context, rdb *redis.Client) (string, string, error) {
	aap, err := rdb.AsyncAutoPipeline()
	if err != nil {
		return "", "", err
	}
	defer aap.Close()
	a := aap.Get(ctx, "user:1") // sent, no wait
	b := aap.Get(ctx, "user:2") // same trip
	u1, err := a.Result()
	if err != nil {
		return "", "", err
	}
	u2, err := b.Result()
	return u1, u2, err
}

// Step 3: full duplex. One connection; the writer never waits for a reply.
func useFullDuplex(ctx context.Context, rdb *redis.Client) (string, error) {
	ap, err := rdb.AutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		return "", err
	}
	defer ap.Close()
	return ap.Get(ctx, "user:42").Result()
}

var _ = []any{usePool, useBatching, useBatchingAsync, useFullDuplex}
