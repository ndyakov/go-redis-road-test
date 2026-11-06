//go:build !goredis_pipeminidle

package main

import "github.com/redis/go-redis/v9"

// go-redis v9.23.0 has no PipelineMinIdleConns; the setting is ignored.
const hasPipeMinIdle = false

func setPipeMinIdle(*redis.Options, int) {}
