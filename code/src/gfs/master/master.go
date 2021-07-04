package master

import (
	"../../gfs"
	"../cmap"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// TODO: log, checkpoint, recovery

// Master struct
type Master struct {
	address    string // master server address
	serverRoot string
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to ture if server is shutdown
	nhLock 		sync.Mutex
	nextHandle	int64

	// all keys from the following 3 maps are string
	// initialization in new and serve
	// from full path to file metadata
	fileNamespace  cmap.ConcurrentMap
	// from chunk handle to chunk metadata
	chunkNamespace cmap.ConcurrentMap
	// from chunk server address to chunk server info
	chunkServerInfos	cmap.ConcurrentMap

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
	m := &Master{
		address: address, 
		serverRoot: serverRoot, 
		nextHandle: 0,
		shutdown: make(chan struct{}),
		dead: false,
	}

	// initial 3 concurrent maps
	m.fileNamespace = cmap.New()
	m.chunkNamespace = cmap.New()
	m.chunkServerInfos = cmap.New()
	m.loadMeta()

	// register rpc server
	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		// TODO: handle error
		return nil
	}
	m.l = l

	// TODO: merge 2 go func and add shutdown function
	
	// handle rpc
	go func() {
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				if !m.dead {
					// TODO: handle error
				}
			}
		}
	}()

	// handle timed task
	go func() {
		serverCheckTicker := time.Tick(gfs.ServerCheckInterval)
		storeMetaTicker := time.Tick(gfs.StoreMetaInterval)
		for {
			var err error
			select {
			case <-m.shutdown:
				return
			case <-serverCheckTicker:
				err = m.serverCheck()
			case <-storeMetaTicker:
				err = m.storeMeta()
			}
			if err != nil {
				// TODO: handle error
			}
		}
	}()

	return m
}

// loadMeta loads metadata from disk
func (m *Master) loadMeta() error {
	return nil
}

// storeMeta stores metadata to disk
func (m *Master) storeMeta() error {
	return nil
}

// Shutdown shuts down master
func (m *Master) Shutdown() error {
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
	// new chunk server info
	var isFirst bool
	chunkServerInfo := new(ChunkServerInfo)
	chunkServerInfo.Lock()
	defer chunkServerInfo.Unlock()
	chunkServerInfo.lastHeartbeat = time.Now()
	chunkServerInfo.chunks = make(map[int64]bool)
	chunkServerInfo.garbage = nil
	// check and set
	ok := m.chunkServerInfos.SetIfAbsent(args.Address, chunkServerInfo)
	if !ok {
		// server exist
		isFirst = true
		// no method to delete chunk server info so can not check ok
		chunkServerInfoFound, _:= m.chunkServerInfos.Get(args.Address)
		chunkServerInfo = chunkServerInfoFound.(*ChunkServerInfo)
		chunkServerInfo.Lock()
		defer chunkServerInfo.RLock()
		// update time
		chunkServerInfo.lastHeartbeat = time.Now()
		// send garbage
		reply.Garbage = chunkServerInfo.garbage
		for _, v := range chunkServerInfo.garbage {
			chunkServerInfo.chunks[v] = false
		}
		chunkServerInfo.garbage = make([]int64, 0)
	}

	if isFirst {
		// if is first heartbeat, let chunkserver report itself
		var r gfs.ReportSelfReply
		err := gfs.Call(args.Address, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &r)
		if err != nil {
			return err
		}

		for _, v := range r.Chunks {
			chunkMetadataFound, e := m.chunkNamespace.Get(fmt.Sprintf("%d", v.ChunkHandle))
			if !e {
				continue
			}
			chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
			chunkMetadata.Lock()
			if v.Checksum == chunkMetadata.checksum {
				if v.Version == chunkMetadata.version {
					chunkMetadata.location = append(chunkMetadata.location, args.Address)
				} else {
					chunkServerInfo.garbage = append(chunkServerInfo.garbage, v.ChunkHandle)
				}
			} else {
				chunkServerInfo.garbage = append(chunkServerInfo.garbage, v.ChunkHandle)
			}
			chunkMetadata.Unlock()
		}
	} else {
		for _, handle := range args.ToExtendLeases {
			// extend lease
			chunkMetadataFound, exist:= m.chunkNamespace.Get(fmt.Sprintf("%d", handle))
			if !exist {
				return fmt.Errorf("invalid chunk handle %d", handle)
			}
			chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
			chunkMetadata.Lock()
			if chunkMetadata.primary != args.Address {
				chunkMetadata.Unlock()
				return fmt.Errorf("%s does not hold the lease for chunk %d", args.Address, handle)
			}
			chunkMetadata.expire = time.Now().Add(gfs.LeaseExpire)
			chunkMetadata.Unlock()
		}
	}
	return nil
}

// RPCGetReplicas is called by client to find all chunk server that holds the chunk.
// lease holder and secondaries of a chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", args.Handle))
	if !ok {
		return fmt.Errorf("cannot find chunk %d", args.Handle)
	}
	chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
	var staleServers []string
	chunkMetadata.Lock()
	defer chunkMetadata.Unlock()
	// check expire
	if chunkMetadata.expire.Before(time.Now()) {
		// expire is old
		chunkMetadata.version++
		// prepare new rpc arg
		checkVersionArg := gfs.CheckVersionArg{Handle: args.Handle, Version: chunkMetadata.version}
		// chunk server having new version
		var newList []string
		// lock for newList
		var lock sync.Mutex
		// wait group to make sure all goroutines end
		var wg sync.WaitGroup
		wg.Add(len(chunkMetadata.location))
		for _, v := range chunkMetadata.location {
			go func(addr string) {
				var ret gfs.CheckVersionReply
				// call rpc to let all chunk servers check their own version
				err := gfs.Call(addr, "ChunkServer.RPCCheckVersion", checkVersionArg, &ret)
				if err == nil && ret.Stale == false {
					lock.Lock()
					newList = append(newList, addr)
					lock.Unlock()
				} else {
					// add to garbage collection
					// must exist no need to check ok
					chunkServerInfoFound, _:= m.chunkServerInfos.Get(addr)
					chunkServerInfo := chunkServerInfoFound.(*ChunkServerInfo)
					chunkServerInfo.Lock()
					chunkServerInfo.garbage = append(chunkServerInfo.garbage, args.Handle)
					chunkServerInfo.Unlock()
					staleServers = append(staleServers, addr)
				}
				wg.Done()
			}(v)
		}
		wg.Wait()

		// change location in metadata
		chunkMetadata.location = make([]string, len(newList))
		for i, value := range newList {
			chunkMetadata.location[i] = value
		}
		// check if satisfy min replica num
		if len(chunkMetadata.location) < gfs.MinimumNumReplicas {
			m.rnlLock.Lock()
			m.replicasNeedList = append(m.replicasNeedList, args.Handle)
			m.rnlLock.Unlock()

			if len(chunkMetadata.location) == 0 {
				// !! ATTENTION !!
				chunkMetadata.version--
				return fmt.Errorf("no replica of %v", args.Handle)
			}
		}

		// choose primary, !!error handle no replicas!!
		chunkMetadata.primary = chunkMetadata.location[0]
		chunkMetadata.expire = time.Now().Add(gfs.LeaseExpire)
	}
	reply.Primary = chunkMetadata.primary
	reply.Expire = chunkMetadata.expire
	for _, v := range chunkMetadata.location {
		if v != chunkMetadata.primary {
			reply.Secondaries = append(reply.Secondaries, v)
		}
	}
	return nil
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	defer m.unlockParentsRLocks(fileMetadatas)
	if !ok {
		return err
	}
	fileMetadataFound, exist := m.fileNamespace.Get(args.Path)
	if !exist {
		return fmt.Errorf("path %s does not exsit", args.Path)
	}
	fileMetadata := fileMetadataFound.(*FileMetadata)
	fileMetadata.RLock()
	defer fileMetadata.RUnlock()
	reply.Size = fileMetadata.size
	reply.ChunkNum = int64(len(fileMetadata.chunkHandles))
	reply.IsDir = fileMetadata.isDir
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	defer m.unlockParentsRLocks(fileMetadatas)
	if !ok {
		return err
	}
	fileMetadataFound, exist := m.fileNamespace.Get(args.Path)
	if !exist {
		return fmt.Errorf("path %s does not exsit", args.Path)
	}
	fileMetadata := fileMetadataFound.(*FileMetadata)
	fileMetadata.Lock()
	defer fileMetadata.Unlock()
	if int(args.Index) == len(fileMetadata.chunkHandles) {
		addrs, e := m.chooseServers(gfs.DefaultNumReplicas)
		if e != nil {
			return e
		}

		reply.Handle, addrs, err = m.CreateChunk(fileMetadata, addrs)
		if err != nil {
			// TODO: solve some create chunks successfully while some fail
			return fmt.Errorf("create chunk for path %s failed", args.Path)
		}

	} else {
		if args.Index < 0 || int(args.Index) >= len(fileMetadata.chunkHandles) {
			return fmt.Errorf("invalid index for %s[%d]", args.Path, args.Index)
		}
		reply.Handle = fileMetadata.chunkHandles[args.Index]
	}
	return nil
}

// ChooseServers returns servers to store new chunk
// called when a new chunk is create
func (m *Master) chooseServers(num int) ([]string, error) {
	if num > m.chunkServerInfos.Count() {
		return nil, fmt.Errorf("no enough servers for %d replicas", num)
	}

	var ret []string
	all := m.chunkServerInfos.Keys()
	choose, err := gfs.Sample(len(all), num)
	if err != nil {
		return nil, err
	}
	for _, v := range choose {
		ret = append(ret, all[v])
	}

	return ret, nil
}

// CreateChunk creates a new chunk for path. servers for the chunk are denoted by addrs
// returns the handle of the new chunk, and the servers that create the chunk successfully
func (m *Master) CreateChunk(fileMetadata *FileMetadata, addrs []string) (int64, []string, error) {
	m.nhLock.Lock()
	handle := m.nextHandle
	m.nextHandle++
	m.nhLock.Unlock()
	// update file info
	fileMetadata.chunkHandles = append(fileMetadata.chunkHandles, handle)
	chunkMetadata := new(ChunkMetadata)
	chunkMetadata.Lock()
	defer chunkMetadata.Unlock()
	chunkMetadata.version = 0
	chunkMetadata.refcnt = 1
	// TODO: chunk metadata checksum
	chunkMetadata.checksum = 0
	m.chunkNamespace.Set(fmt.Sprintf("%d", handle), chunkMetadata)

	var errList string
	var success []string
	for _, v := range addrs {
		var r gfs.CreateChunkReply

		err := gfs.Call(v, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{handle}, &r)
		if err == nil {
			chunkMetadata.location = append(chunkMetadata.location, v)
			success = append(success, v)
			chunkServerInfoFound, infoOk := m.chunkServerInfos.Get(v)
			if infoOk {
				chunkServerInfo := chunkServerInfoFound.(*ChunkServerInfo)
				chunkServerInfo.Lock()
				chunkServerInfo.chunks[handle] = true
				chunkServerInfo.Unlock()
			}
		} else {
			errList += err.Error() + ";"
		}
	}

	if errList == "" {
		return handle, success, nil
	} else {
		// replicas are no enough, add to need list
		m.rnlLock.Lock()
		m.replicasNeedList = append(m.replicasNeedList, handle)
		m.rnlLock.Unlock()
		return handle, success, fmt.Errorf(errList)
	}
}


// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	defer m.unlockParentsRLocks(fileMetadatas)
	if !ok {
		return err
	}
	fileMetadata := new(FileMetadata)
	fileMetadata.isDir = false
	fileMetadata.size = 0
	fileMetadata.chunkHandles = nil
	ok = m.fileNamespace.SetIfAbsent(args.Path, fileMetadata)
	if !ok {
		return fmt.Errorf("path %s has already existed", args.Path)
	}
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

	fileMetadataFound, exist := m.fileNamespace.Get(args.Path)
	if !exist {
		return fmt.Errorf("path %s does not exsit", args.Path)
	}
	var fileMetadata = fileMetadataFound.(*FileMetadata)
	fileMetadata.RLock()
	if fileMetadata.isDir {
		fileMetadata.RUnlock()
		return fmt.Errorf("path %s is not a file", args.Path)
	}
	fileMetadata.RUnlock()
	// may fail but without error throw
	m.fileNamespace.Remove(args.Path)
	// lazy delete
	// may fail
	m.fileNamespace.SetIfAbsent(gfs.DeletedFilePrefix + args.Path, fileMetadata)
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
	sourceFileMetadataFound, sourceExist := m.fileNamespace.Get(args.Source)
	if !sourceExist {
		err = fmt.Errorf("source path %s has not existed", args.Source)
		return err
	}
	sourceFileMetadata := sourceFileMetadataFound.(*FileMetadata)
	if sourceFileMetadata.isDir {
		err = fmt.Errorf("source path %s is not a file", args.Source)
		return err
	}
	setOk := m.fileNamespace.SetIfAbsent(args.Target, sourceFileMetadata)
	if !setOk {
		err = fmt.Errorf("target path %s has already existed", args.Target)
		return err
	}
	m.fileNamespace.Remove(args.Source)
	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	defer m.unlockParentsRLocks(fileMetadatas)
	if !ok {
		return err
	}
	fileMetadata := new(FileMetadata)
	fileMetadata.isDir = true
	fileMetadata.size = 0
	fileMetadata.chunkHandles = nil
	ok = m.fileNamespace.SetIfAbsent(args.Path, fileMetadata)
	if !ok {
		return fmt.Errorf("path %s has already existed", args.Path)
	}
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
		fileMetadataFound, ok := m.fileNamespace.Get(value)
		fileMetadata := fileMetadataFound.(*FileMetadata)
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