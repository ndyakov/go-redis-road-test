//go:build goredis_pipeminidle

package main

import "github.com/redis/go-redis/v9"

// go-redis v9.24+ has Options.PipelineMinIdleConns: warm spare pipeline-pool
// connections. Creating a full-duplex autopipeliner raises it to at least 1 by
// itself; a negative value keeps no spare. run.sh sets this build tag when the
// go-redis source has the field.
const hasPipeMinIdle = true

func setPipeMinIdle(o *redis.Options, n int) { o.PipelineMinIdleConns = n }
