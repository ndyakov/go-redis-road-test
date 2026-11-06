package main

// The one-line start of each engine, the same lines as on the talk slides.
// The demo calls the same functions in startEngine (engines.go); this file
// keeps them in one place to read.

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Step 1: a pool. Each call takes a connection, sends one command, waits.
func usePool(ctx context.Context) (string, error) {
	rdb := redis.NewClient(&redis.Options{PoolSize: 500})
	defer rdb.Close()
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
