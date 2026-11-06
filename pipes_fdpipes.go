//go:build goredis_fdpipes

package main

import "github.com/redis/go-redis/v9"

// go-redis v9.24+ selects full-duplex pipes with FullDuplexPipes (NumShards
// still works there, for compatibility). run.sh sets this build tag when the
// go-redis source has the field.
const pipesField = "FullDuplexPipes"

func setPipes(o *redis.AutoPipelineOptions, n int) { o.FullDuplexPipes = n }
