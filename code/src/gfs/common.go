package gfs

import (
	"os"
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

type TYPE int

const (
	CLIENT = 0
	MASTER = 1
	CHUNKSERVER = 2
)

const (
	DEBUG = true
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
	defer file.Close()
	if err != nil {
		return
	}

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
	StoreMetaInterval   = 1 * time.Hour
	MetaFileName        = "gfs-master.meta"
	ServerTimeout       = 1 * time.Second

	// chunk server
	HeartbeatInterval   = 100 * time.Millisecond
	GarbageCollectionInterval = 2 * time.Hour

	// client
	ReplicaBufferTick 	= 500 * time.Millisecond
)

// error code
const (
	Success = iota
	UnknownError
	AppendExceedChunkSize
	ReadEOF
)