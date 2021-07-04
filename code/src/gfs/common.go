package gfs

type rpcChunkMetadata struct {
	chunkHandle 	int64

	version 	int64
	checksum	int64
}