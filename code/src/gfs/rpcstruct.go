package gfs

import "time"

// chunk server -----> master

type HeartbeatArg struct {
	Address          string // chunk server address
	ToExtendLeases  []int64 // leases to be extended
	ToRemoveChunks []int64 // chunks to be removed
}

type HeartbeatReply struct {
	Garbage []int64
}

// master -----> chunk server

type ReportSelfArg struct {
}

type ReportSelfReply struct {
	Chunks []rpcChunkMetadata
}

// client -----> master chunk

type GetReplicasArg struct {
	Handle int64
}
type GetReplicasReply struct {
	Primary     string
	// end time of lease
	Expire      time.Time
	Secondaries []string
}

type GetFileInfoArg struct {
	Path string
}
type GetFileInfoReply struct {
	IsDir  bool
	Size int64
	ChunkNum int64
}

type GetChunkHandleArg struct {
	Path  string
	Index int64
}
type GetChunkHandleReply struct {
	Handle int64
}

// client -----> master namespace

type CreateFileArg struct {
	Path string
}
type CreateFileReply struct{

}

type DeleteFileArg struct {
	Path string
}
type DeleteFileReply struct{

}

type RenameFileArg struct {
	Source string
	Target string
}
type RenameFileReply struct{

}

type MkdirArg struct {
	Path string
}
type MkdirReply struct{

}
