package gfs

type rpcChunkMetadata struct {
	ChunkHandle 	int64

	Version 	int64
	Checksum	int64
}

// system config
const (
	// master
	DeletedFilePrefix  = "__del__"
)