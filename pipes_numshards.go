//go:build !goredis_fdpipes

package main

import "github.com/redis/go-redis/v9"

// go-redis v9.23.0 selects full-duplex pipes with NumShards. run.sh builds with
// -tags goredis_fdpipes when the go-redis source has FullDuplexPipes (v9.24+).
const pipesField = "NumShards"

func setPipes(o *redis.AutoPipelineOptions, n int) { o.NumShards = n }
