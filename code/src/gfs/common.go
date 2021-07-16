package gfs

import (
	"os"
	"syscall"
	"time"
)

type DataBufferID struct {
	Handle int64
	Time   int64
}

type ChunkReplicaInfo struct {
	Primary 	 string
	Expire 		 time.Time
	Secondaries  []string
	BufferExpire time.Time
}

type TYPE int

const (
	CLIENT = 0
	MASTER = 1
	CHUNKSERVER = 2
)

const (
	DEBUG = false
	ClientDirectory	= "../debug/"
	ClientDebugFilePrefix = "DEBUG_client_"
	ClientMSGPrefix = "[CLIENT] "
	MasterDirectory	= "../debug/"
	MasterDebugFilePrefix = "DEBUG_master_"
	MasterMSGPrefix = "[MASTER] "
	ChunkServerDirectory	= "../debug/"
	ChunkServerDebugFilePrefix = "DEBUG_chunk_server_"
	ChunkServerMSGPrefix = "[CHUNKSERVER] "
	DebugFileSuffix = ".txt"
)

func DebugMsgToFile(msg string, t TYPE, description string) {
	if !DEBUG {
		return
	}
	var openPath string
	var toWrite string
	switch t {
	case CLIENT:
		openPath = ClientDirectory + ClientDebugFilePrefix + description + DebugFileSuffix
		toWrite = ClientMSGPrefix + msg + "\n"
		break
	case MASTER:
		openPath = MasterDirectory + MasterDebugFilePrefix + description + DebugFileSuffix
		toWrite = MasterMSGPrefix + msg + "\n"
		break
	case CHUNKSERVER:
		openPath = ChunkServerDirectory + ChunkServerDebugFilePrefix + description + DebugFileSuffix
		toWrite = ChunkServerMSGPrefix + msg + "\n"
		break
	default:
		return
	}
	file, err := os.OpenFile(openPath, os.O_WRONLY | os.O_CREATE | os.O_APPEND , 0777)
	if err != nil {
		return
	}
	defer file.Close()
	// use file lock to support concurrent write
	syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	file.WriteString(toWrite)
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
	ServerTimeout       = 1 * time.Second

	// chunk server
	HeartbeatInterval   = 100 * time.Millisecond
	StoreMetaInterval   = 1 * time.Hour
	GarbageCollectionInterval = 2 * time.Hour
	MutationMaxTime 	= 1 * time.Second

	// client
	ReplicaBufferTick 	= 500 * time.Millisecond
	ReplicaBufferExpire = 1 * time.Second
)

// error code
const (
	Success = iota
	UnknownError
	AppendExceedChunkSize
	ReadEOF
)

// Masters the addr of masters
var Masters = []string{"127.0.0.1:8080","127.0.0.1:8081","127.0.0.1:8082"}