package gfs

type rpcChunkMetadata struct {
	chunkHandle 	int64

	version 	int64
	checksum	int64
}

// system config
const (
	// master
	DeletedFilePrefix  = "__del__"
)