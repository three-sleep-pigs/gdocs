package gfs

import "time"

type rpcChunkMetadata struct {
	ChunkHandle int64

	Version  int64
	Checksum int64
}

// chunk server -----> master

type HeartbeatArg struct {
	Address        string  // chunk server address
	ToExtendLeases []int64 // leases to be extended
	ToRemoveChunks []int64 // chunks to be removed
}

type HeartbeatReply struct {
	Garbage        []int64
	InvalidHandles []int64
	NotPrimary     []int64
}

// master -----> chunk server

type ReportSelfArg struct {
}

type ReportSelfReply struct {
	Chunks []rpcChunkMetadata
}

type CheckVersionArg struct {
	Handle  int64
	Version int64
}

type CheckVersionReply struct {
	Stale bool
}

type CreateChunkArg struct {
	Handle int64
}

type CreateChunkReply struct {
	ErrorCode int
}

type SendCopyArg struct {
	Handle  int64
	Address string
}

type SendCopyReply struct {
	ErrorCode int
}

// client -----> master chunk

type GetReplicasArg struct {
	Handle int64
}
type GetReplicasReply struct {
	Primary string
	// end time of lease
	Expire      time.Time
	Secondaries []string
}

type GetFileInfoArg struct {
	Path string
}
type GetFileInfoReply struct {
	IsDir    bool
	Size     int64
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
type CreateFileReply struct {
}

type DeleteFileArg struct {
	Path string
}
type DeleteFileReply struct {
}

type RenameFileArg struct {
	Source string
	Target string
}
type RenameFileReply struct {
}

type MkdirArg struct {
	Path string
}
type MkdirReply struct {
}

// client -----> chunk server

type WriteChunkArg struct {
	DbID        DataBufferID
	Offset      int64
	Secondaries []string
}

type WriteChunkReply struct {
}

type AppendChunkArg struct {
	DbID        DataBufferID
	Secondaries []string
}

type AppendChunkReply struct {
	Offset    int64
	ErrorCode int
}

type ApplyMutationArg struct {
	DbID   DataBufferID
	Offset int64
}

type ApplyMutationReply struct {
}

// chunk server -----> chunk server

type ApplyCopyArg struct {
	Handle  int64
	Version int64
	Data    []byte
}

type ApplyCopyReply struct {
}

type ForwardDataArg struct {
	DataID     DataBufferID
	Data       []byte
	ChainOrder []string
}
type ForwardDataReply struct {
	ErrorCode int
}
type ReadChunkArg struct {
	Handle int64
	Offset int64
	Length int
}
type ReadChunkReply struct {
	Data      []byte
	Length    int
	ErrorCode int
}
