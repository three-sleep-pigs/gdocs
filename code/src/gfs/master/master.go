package master

import (
	"../../gfs"
	"fmt"
	"net"
	"strings"
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
	refcnt	int64
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
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	defer m.unlockParentsRLocks(fileMetadatas)
	if !ok {
		return err
	}

	m.fnsLock.Lock()
	_, exist := m.fileNamespace[args.Path]
	if exist {
		err = fmt.Errorf("path %s has already existed", args.Path)
		m.fnsLock.Unlock()
		return err
	}
	m.fileNamespace[args.Path] = &FileMetadata{isDir: false, size: 0, chunkHandles: nil}
	m.fnsLock.Unlock()
	return nil
}

// RPCDeleteFile is called by client to delete a file
func (m *Master) RPCDeleteFile(args gfs.DeleteFileArg, reply *gfs.DeleteFileReply) error {
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	defer m.unlockParentsRLocks(fileMetadatas)
	if !ok {
		return err
	}

	m.fnsLock.Lock()
	fileMetadata, exist := m.fileNamespace[args.Path]
	if !exist {
		err = fmt.Errorf("path %s has not existed", args.Path)
		m.fnsLock.Unlock()
		return err
	}
	if fileMetadata.isDir {
		err = fmt.Errorf("path %s is not a file", args.Path)
		m.fnsLock.Unlock()
		return err
	}
	delete(m.fileNamespace, args.Path)
	// lazy delete
	m.fileNamespace[gfs.DeletedFilePrefix + args.Path] = fileMetadata
	m.fnsLock.Unlock()
	return nil
}

// RPCRenameFile is called by client to rename a file
func (m *Master) RPCRenameFile(args gfs.RenameFileArg, reply *gfs.RenameFileReply) error {
	sourceParents := getParents(args.Source)
	ok, sourceFileMetadatas, err := m.acquireParentsRLocks(sourceParents)
	defer m.unlockParentsRLocks(sourceFileMetadatas)
	if !ok {
		return err
	}
	targetParents := getParents(args.Target)
	ok, targetFileMetadatas, err := m.acquireParentsRLocks(targetParents)
	defer m.unlockParentsRLocks(targetFileMetadatas)
	if !ok {
		return err
	}
	m.fnsLock.Lock()
	sourceFileMetadata, sourceExist := m.fileNamespace[args.Source]
	if !sourceExist {
		err = fmt.Errorf("source path %s has not existed", args.Source)
		m.fnsLock.Unlock()
		return err
	}
	if sourceFileMetadata.isDir {
		err = fmt.Errorf("source path %s is not a file", args.Source)
		m.fnsLock.Unlock()
		return err
	}
	_, targetExist := m.fileNamespace[args.Target]
	if targetExist {
		err = fmt.Errorf("target path %s has already existed", args.Target)
		m.fnsLock.Unlock()
		return err
	}
	delete(m.fileNamespace, args.Source)
	m.fileNamespace[args.Target] = sourceFileMetadata
	m.fnsLock.Unlock()
	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	// TODO: big
	return nil
}
func getParents(path string) []string {
	splits := strings.Split(path, "/")
	var parents []string
	var add string
	for i := 1; i < len(splits)-1; i++ {
		if i > 1 {
			add = fmt.Sprintf("%s/%s", parents[i-2], splits[i])
		} else {
			add = splits[i]
		}
		parents = append(parents, add)
	}
	return parents
}

// acquire parents read lock
func (m *Master) acquireParentsRLocks(parents []string) (bool, []*FileMetadata, error)  {
	var fileMetadatas []*FileMetadata
	for _, value := range parents {
		m.fnsLock.RLock()
		fileMetadata, ok := m.fileNamespace[value]
		m.fnsLock.RUnlock()
		if !ok {
			return false, nil, fmt.Errorf("path %s doesn't exist", value)
		}
		if !fileMetadata.isDir {
			return false, nil, fmt.Errorf("path %s is not d dir", value)
		}
		fileMetadatas = append(fileMetadatas, fileMetadata)
	}
	for _, fileMetadata := range fileMetadatas {
		fileMetadata.RLock()
	}
	return true, fileMetadatas, nil
}

// unlock parents read lock
func (m *Master) unlockParentsRLocks(fileMetadatas []*FileMetadata) {
	for _, fileMetadata := range fileMetadatas {
		fileMetadata.RUnlock()
	}
}