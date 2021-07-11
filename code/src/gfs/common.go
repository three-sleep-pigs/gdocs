package gfs

import (
	"time"
)

type DataBufferID struct {
	Handle int64
	Time   int
}

type ChunkReplicaInfo struct {
	Primary 	string
	Expire 		time.Time
	Secondaries []string
}

// system config
const (
	// master
	DeletedFilePrefix  = "__del__"
	MinimumNumReplicas = 2
	DefaultNumReplicas = 3
	LeaseExpire        = 1 * time.Minute
	MaxChunkSize       = 32 << 20 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize      = MaxChunkSize / 4

	ServerCheckInterval = 400 * time.Millisecond
	StoreMetaInterval   = 1 * time.Hour
	MetaFileName        = "gfs-master.meta"
	ServerTimeout       = 1 * time.Second

	// chunk server
	HeartbeatInterval   = 100 * time.Millisecond
	GarbageCollectionInterval = 2 * time.Hour

	// client
	ReplicaBufferTick 	= 500 * time.Millisecond
	ClientTryTimeout = 2*LeaseExpire + 3*ServerTimeout
)
