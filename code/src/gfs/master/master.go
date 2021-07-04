package master

import (
	"../../gfs"
	"net"
	"sync"
	"time"
)

// Master struct
type Master struct {
	address    string // master server address
	serverRoot string
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to ture if server is shutdown

	// from full path to file metadata
	fnsLock        sync.RWMutex
	fileNamespace  map[string]*FileMetadata
	// from chunk handle to chunk metadata
	cnsLock        sync.RWMutex
	chunkNamespace map[int64]*ChunkMetadata
	// from chunk server address to chunk server info
	csiLock 	   sync.RWMutex
	chunkServerInfos	map[string]*ChunkServerInfo

	// list of chunk handles need a new replicas
	rnlLock 	sync.RWMutex
	replicasNeedList []int64
}

type FileMetadata struct {
	sync.RWMutex

	isDir	bool

	// if it is a file
	size	int64
	chunkHandles	[]int64
}

type PersistentFileMetadata struct {
	path 	string

	isDir	bool

	// if it is a file
	size	int64
	chunkHandles	[]int64
}

type ChunkMetadata struct {
	sync.RWMutex

	location []string	// set of replica locations
	primary  string	// primary chunkserver
	expire   time.Time	// lease expire time
	version  int64
	checksum int64
}

type PersistentChunkMetadata struct {
	chunkHandle 	int64

	version 	int64
	checksum	int64
}

type ChunkServerInfo struct {
	sync.RWMutex

	lastHeartbeat time.Time
	chunks        map[int64]bool // set of chunks that the chunkserver has
	garbage       []int64
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address string, serverRoot string) *Master {
	// TODO: small
	return nil
}

// serverCheck checks all chunkserver according to last heartbeat time
// then removes all the information of the disconnnected servers
func (m *Master) serverCheck() error {
	// TODO: small
	return nil
}

// reReplication performs re-replication, ck should be locked in top caller
// new lease will not be granted during copy
func (m *Master) reReplication(handle int64) error {
	// TODO: small
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	// TODO: big
	return nil
}

// RPCGetReplicas is called by client to find all chunk server that holds the chunk.
// lease holder and secondaries of a chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	// TODO: big

	return nil
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	// TODO: big

	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	// TODO: big
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	// TODO: big
	return nil
}

// RPCDeleteFile is called by client to delete a file
func (m *Master) RPCDeleteFile(args gfs.DeleteFileArg, reply *gfs.DeleteFileReply) error {
	// TODO: big
	return nil
}

// RPCRenameFile is called by client to rename a file
func (m *Master) RPCRenameFile(args gfs.RenameFileArg, reply *gfs.RenameFileReply) error {
	// TODO: big
	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	// TODO: big
	return nil
}