package gfs

import "time"

// chunk server -----> master

type HeartbeatArg struct {
	address          string // chunk server address
	toExtendLeases  []int64 // leases to be extended
	toRemoveChunks []int64 // chunks to be removed
}

type HeartbeatReply struct {
	garbage []int64
}

// master -----> chunk server

type ReportSelfArg struct {
}

type ReportSelfReply struct {
	chunks []rpcChunkMetadata
}

// client -----> master chunk

type GetReplicasArg struct {
	handle int64
}
type GetReplicasReply struct {
	primary     string
	// end time of lease
	expire      time.Time
	secondaries []string
}

type GetFileInfoArg struct {
	path string
}
type GetFileInfoReply struct {
	isDir  bool
	size int64
	chunkNum int64
}

type GetChunkHandleArg struct {
	path  string
	Index int64
}
type GetChunkHandleReply struct {
	handle int64
}

// client -----> master namespace

type CreateFileArg struct {
	path string
}
type CreateFileReply struct{

}

type DeleteFileArg struct {
	path string
}
type DeleteFileReply struct{

}

type RenameFileArg struct {
	source string
	target string
}
type RenameFileReply struct{

}

type MkdirArg struct {
	path string
}
type MkdirReply struct{

}
