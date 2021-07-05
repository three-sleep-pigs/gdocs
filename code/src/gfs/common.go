package gfs

import (
	"time"
)
type DataBufferID struct {
	Handle int64
	Time int
}

// system config
const (
	// master
	DeletedFilePrefix  = "__del__"
	MinimumNumReplicas = 2
	DefaultNumReplicas = 3
	LeaseExpire = 1 * time.Minute
	MaxChunkSize = 32 << 20 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize = MaxChunkSize / 4

)