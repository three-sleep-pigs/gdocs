package master

import (
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"../../gfs"
	"../cmap"
)

// TODO: log, checkpoint, recovery
// TODO: lazy delete

// Master struct
type Master struct {
	address    string // master server address
	serverRoot string
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to ture if server is shutdown
	nhLock     sync.Mutex
	nextHandle int64

	// all keys from the following 3 maps are string
	// initialization in new and serve
	// from full path to file metadata
	fileNamespace cmap.ConcurrentMap
	// from chunk handle to chunk metadata
	chunkNamespace cmap.ConcurrentMap
	// from chunk server address to chunk server info
	chunkServerInfos cmap.ConcurrentMap

	// list of chunk handles need a new replicas
	rnlLock          sync.RWMutex
	replicasNeedList []int64
}

type FileMetadata struct {
	sync.RWMutex

	isDir bool

	// if it is a file
	size         int64
	chunkHandles []int64
}

type ChunkMetadata struct {
	sync.RWMutex

	location []string  // set of replica locations
	primary  string    // primary chunkserver
	expire   time.Time // lease expire time
	version  int64
	checksum int64
	refcnt   int64
}

type ChunkServerInfo struct {
	sync.RWMutex

	lastHeartbeat time.Time
	chunks        map[int64]bool // set of chunks that the chunkserver has
	garbage       []int64
}

type PersistentFileMetadata struct {
	path string

	isDir bool

	// if it is a file
	size         int64
	chunkHandles []int64
}

type PersistentChunkMetadata struct {
	chunkHandle int64

	version  int64
	checksum int64
	refcnt   int64
}

type PersistentMetadata struct {
	nextHandle int64
	chunkMeta  []PersistentChunkMetadata
	fileMeta   []PersistentFileMetadata
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address string, serverRoot string) *Master {
	m := &Master{
		address:    address,
		serverRoot: serverRoot,
		nextHandle: 0,
		shutdown:   make(chan struct{}),
		dead:       false,
	}

	// initial 3 concurrent maps
	m.fileNamespace = cmap.New()
	m.chunkNamespace = cmap.New()
	m.chunkServerInfos = cmap.New()
	// TODO: Check RootDir before loadMeta
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
					// TODO: log error
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
				{
					if m.dead {     // check if shutdown
						return
					}
					err = m.serverCheck()
				}
			case <-storeMetaTicker:
				{
					if m.dead {     // check if shutdown
						return
					}
					err = m.storeMeta()
				}
			}
			if err != nil {
				// TODO: log error
			}
		}
	}()

	return m
}

// loadMeta loads metadata from disk
func (m *Master) loadMeta() error {
	filename := path.Join(m.serverRoot, gfs.MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentMetadata
	dec := gob.NewDecoder(file)
	err = dec.Decode(&meta)
	if err != nil {
		return err
	}

	m.nextHandle = meta.nextHandle

	for _, pf := range meta.fileMeta {
		f := new(FileMetadata)
		f.isDir = pf.isDir
		f.size = pf.size
		f.chunkHandles = pf.chunkHandles
		e := m.fileNamespace.SetIfAbsent(pf.path, f)
		if !e {
			// TODO: handle exist error
			continue
		}
	}

	for _, pc := range meta.chunkMeta {
		c := new(ChunkMetadata)
		c.version = pc.version
		c.checksum = pc.checksum
		c.refcnt = pc.refcnt
		e := m.chunkNamespace.SetIfAbsent(fmt.Sprintf("%d", pc.chunkHandle), c)
		if !e {
			// TODO: handle exist error
			continue
		}
	}

	return nil
}

// storeMeta stores metadata to disk
// FIXME: consistency problem
// But the file is reliable after shutdown.
func (m *Master) storeMeta() error {
	// TODO: use lock to protect metadata file
	filename := path.Join(m.serverRoot, gfs.MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentMetadata

	for tuple := range m.fileNamespace.IterBuffered() {
		f := tuple.Val.(*FileMetadata)
		f.RLock()
		meta.fileMeta = append(meta.fileMeta, PersistentFileMetadata{
			path:         tuple.Key,
			isDir:        f.isDir,
			size:         f.size,
			chunkHandles: f.chunkHandles,
		})
		f.RUnlock()
	}

	for tuple := range m.chunkNamespace.IterBuffered() {
		h, err := strconv.ParseInt(tuple.Key, 10, 64)
		if err != nil {
			// TODO: handle error
			continue
		}
		c := tuple.Val.(*ChunkMetadata)
		c.RLock()
		meta.chunkMeta = append(meta.chunkMeta, PersistentChunkMetadata{
			chunkHandle: h,
			version:     c.version,
			checksum:    c.checksum,
			refcnt:      c.refcnt,
		})
		c.RUnlock()
	}

	m.nhLock.Lock()
	meta.nextHandle = m.nextHandle
	m.nhLock.Unlock()

	enc := gob.NewEncoder(file)
	err = enc.Encode(meta)
	return err
}

// Shutdown shuts down master
// FIXME: Shutdown shouldn't be called concurrently because TOCTTOU of m.dead
func (m *Master) Shutdown() error {
	if !m.dead {
		m.dead = true
		// close will cause case <- shutdown part get value 0
		// end the rpc goroutine and the server check, store goroutine
		close(m.shutdown)
		m.l.Close()
		err := m.storeMeta()
		return err
	}
	return nil
}

//TODO: check consistency of file meta and chunk meta
//TODO: check consistency of chunkserverinfo and location in chunkmeta

// serverCheck checks chunkserver and removes chunkinfo of disconnnected servers
func (m *Master) serverCheck() error {
	// detect dead servers
	var deadServer []string
	now := time.Now()
	for tuple := range m.chunkServerInfos.IterBuffered() {
		cs := tuple.Val.(*ChunkServerInfo)
		cs.RLock()
		if cs.lastHeartbeat.Add(gfs.ServerTimeout).Before(now) {
			deadServer = append(deadServer, tuple.Key)
		}
		cs.RUnlock()
	}
	// FIXME: TOCTOU may receive heartbeat after releasing chunkServerInfo RLock
	// xjq: maybe acquire WLock at first and add a member in chunkServerInfo identifying if it is valid
	// the word, "valid" means there is still a entrance in the relative map
	// in heartbeat part, even we can get entrance from the map
	// if it is not valid, then we will consider it is the first time for the chunk server to send heartbeat to master
	// we can discuss... I just cannot come up with another idea... orz

	// lyc: Remove should be replaced by "valid" flag.
	// And we should consider the consistency of chunkserverinfo and chunkmeta.
	// Chunk server may be added between remove and change chunkmeta.location.
	// We can use chunkServerInfo.Lock to protect remove and change chunkmeta.location.
	// But this may lead to dead lock.

	// remove dead servers
	for _, addr := range deadServer {
		chunkServerInfoFound, ok := m.chunkServerInfos.Get(addr)
		if !ok {
			continue
		}
		chunkServerInfo := chunkServerInfoFound.(*ChunkServerInfo)
		var handles []int64
		chunkServerInfo.RLock()
		for h, v := range chunkServerInfo.chunks {
			if v {
				handles = append(handles, h)
			}
		}
		chunkServerInfo.RUnlock()
		m.chunkServerInfos.Remove(addr)

		// remove dead server from chunk meta and detect chunk need replica
		for _, h := range handles {
			chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", h))
			if !ok {
				continue
			}
			chunkMetadata := chunkMetadataFound.(*ChunkMetadata)

			var newLocation []string
			chunkMetadata.Lock()
			for _, l := range chunkMetadata.location {
				if l != addr {
					newLocation = append(newLocation, l)
				}
			}
			chunkMetadata.location = newLocation
			chunkMetadata.expire = time.Now()
			replicaNum := len(chunkMetadata.location)
			chunkMetadata.Unlock()

			if replicaNum < gfs.MinimumNumReplicas {
				m.rnlLock.Lock()
				m.replicasNeedList = append(m.replicasNeedList, h)
				m.rnlLock.Unlock()
				if replicaNum == 0 {
					// TODO: handle error
				}
			}
		}
	}

	// add replicas
	err := m.reReplicationAll()
	return err
}

// reReplicationAll adds replicas for all chunks to be replicated
func (m *Master) reReplicationAll() error {
	// clear satisfied chunk
	m.rnlLock.Lock()
	var newNeedList []int
	for _, h := range m.replicasNeedList {
		chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", h))
		if !ok {
			continue
		}
		// FIXME: is there need to check replica num here?
		// Maybe we can just easily copy the slice while holding lock
		// and check replica num when we holding the corresponding chunkMetadata as below

		// lyc: It's right! Move replica num check down and delete replicasNeedList if success/satisfied/not exist.
		// Check if satisfied again after success.
		chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
		if len(chunkMetadata.location) < gfs.MinimumNumReplicas {
			newNeedList = append(newNeedList, int(h))
		}
	}

	// make unique
	sort.Ints(newNeedList)
	m.replicasNeedList = make([]int64, 0)
	for i, h := range newNeedList {
		if i == 0 || h != newNeedList[i-1] {
			m.replicasNeedList = append(m.replicasNeedList, int64(h))
		}
	}
	needList := m.replicasNeedList
	m.rnlLock.Unlock()

	for _, h := range needList {
		chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", h))
		if !ok {
			continue
		}
		chunkMetadata := chunkMetadataFound.(*ChunkMetadata)

		// lock chunk so master will not grant lease during copy time
		chunkMetadata.Lock()
		// TODO: check replica num here?
		// FIXME: I'm not quite sure about the expire check as follows
		// expire is only relative with primary, I think
		// so we will not handle chunks without a primary having not enough replicas err?
		// BTW, primary will be selected and expire will be checked only when client ask for all replicas of a chunk
		if chunkMetadata.expire.Before(time.Now()) {
			m.reReplicationOne(h)
			// TODO: handle error
		}
		chunkMetadata.Unlock()
	}

	return nil
}

// reReplicationOne adds replica for one chunk, chunk meta should be locked in top caller
func (m *Master) reReplicationOne(handle int64) error {
	// holding corresponding chunk metadata lock now
	from, to, err := m.chooseReReplication(handle)
	if err != nil {
		return err
	}

	// TODO: check code in chunk server, to may have handle, from may not, version checksum in from and to may be wrong, etc
	var cr gfs.CreateChunkReply
	err = gfs.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &cr)
	if err != nil {
		return err
	}

	var sr gfs.SendCopyReply
	err = gfs.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &sr)
	if err != nil {
		return err
	}

	// TODO: check dead lock for cs and cm lock
	// add handle in chunk server info of to
	chunkServerInfoFound, ok := m.chunkServerInfos.Get(to)
	if !ok {
		return fmt.Errorf("add chunk in removed server %s", to)
	}
	chunkServerInfo := chunkServerInfoFound.(*ChunkServerInfo)
	// FIXME: deadlock will happen
	// same problem as chooseReReplication part
	chunkServerInfo.Lock()
	chunkServerInfo.chunks[handle] = true
	chunkServerInfo.Unlock()

	// add to in location of handle
	chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", handle))
	if !ok {
		return fmt.Errorf("cannot find chunk %v", handle)
	}
	chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
	chunkMetadata.location = append(chunkMetadata.location, to)
	return nil
}

// TODO: check dead lock for cs and cm lock, improve selection strategy
// chooseReReplication chooses from chunk server and to chunk server for chunk to be replicated
func (m *Master) chooseReReplication(handle int64) (from, to string, err error) {
	from = ""
	to = ""
	err = nil
	for tuple := range m.chunkServerInfos.IterBuffered() {
		cs := tuple.Val.(*ChunkServerInfo)
		// FIXME: there will be a dead lock
		// While holding chunkMetadata Lock, acquiring for all chunkServerInfo read lock is too dangerous
		// heartbeat part will acquire chunkMetadata Lock while holding chunkServerInfo Lock
		// For the first heartbeat, after adding to chunk server map, reReplica happens.
		// if chunk server report itself holding the relative chunk handle, bang!!! dead lock
		// for not first heartbeats, if the chunk handle needing to reReplica is in the leases to extend, dead lock will happen.
		// xjq: solve by release chunkServerInfo lock in heartbeat
		// TODO: is there any other chance for some goroutines holding chunkServerInfo lock with acquiring chunkMetaData lock?
		// If so, deadlock will happen
		// detect later... I don't want to see any go code today...
		cs.RLock() // can be deleted
		if cs.chunks[handle] {
			from = tuple.Key
		} else {
			to = tuple.Key
		}
		cs.RUnlock()
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("No enough server for replica %v", handle)
	return
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	// new chunk server info
	isFirst := true
	chunkServerInfoNew := new(ChunkServerInfo)
	chunkServerInfoNew.Lock()
	chunkServerInfoNew.lastHeartbeat = time.Now()
	chunkServerInfoNew.chunks = make(map[int64]bool)
	chunkServerInfoNew.garbage = nil
	chunkServerInfoNew.Unlock()
	// check and set
	ok := m.chunkServerInfos.SetIfAbsent(args.Address, chunkServerInfoNew)
	if !ok {
		// server exist
		isFirst = false
		// no method to delete chunk server info so can not check ok
		chunkServerInfoFound, _ := m.chunkServerInfos.Get(args.Address)
		chunkServerInfoOld := chunkServerInfoFound.(*ChunkServerInfo)
		chunkServerInfoOld.Lock()
		// TODO: the entrance in map may be deleted, check it

		// update time
		chunkServerInfoOld.lastHeartbeat = time.Now()
		// send garbage
		reply.Garbage = chunkServerInfoOld.garbage
		for _, v := range chunkServerInfoOld.garbage {
			chunkServerInfoOld.chunks[v] = false
		}
		chunkServerInfoOld.garbage = make([]int64, 0)
		chunkServerInfoOld.Unlock()
	}
	// FIXME: release chunkServerInfo Lock here to handle deadlock
	// FIXME: there is no need for isFirst
	// if no extend leases in the first heartbeat, no need for isFirst
	// TODO: handle first heartbeat's set of leases to be extended
	if isFirst {
		// if is first heartbeat, let chunk server report itself
		var r gfs.ReportSelfReply
		err := gfs.Call(args.Address, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &r)
		if err != nil {
			return err
		}
		garbage := make([]int64, 0)
		for _, v := range r.Chunks {
			chunkMetadataFound, e := m.chunkNamespace.Get(fmt.Sprintf("%d", v.ChunkHandle))
			if !e {
				// TODO: chunk server report a chunk handle not in chunk namespace
				continue
			}
			chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
			chunkMetadata.Lock()
			if v.Checksum == chunkMetadata.checksum && v.Version == chunkMetadata.version {
				// TODO: after adding address, the needed to reReplica slice may get changed
				// slove: check at reReplica part. no need to worry
				chunkMetadata.location = append(chunkMetadata.location, args.Address)
			} else {
				garbage = append(garbage, v.ChunkHandle)
			}
			chunkMetadata.Unlock()
		}
		// set garbage
		chunkServerInfoNew.Lock()
		chunkServerInfoNew.garbage = garbage
		chunkServerInfoNew.Unlock()
	} else {
		// use slice to avoid handling only front leases to extend
		var invalidHandle []int64 = make([]int64, 0)
		var notPrimary []int64 = make([]int64, 0)
		for _, handle := range args.ToExtendLeases {
			// extend lease
			chunkMetadataFound, exist := m.chunkNamespace.Get(fmt.Sprintf("%d", handle))
			if !exist {
				// append to slice and reply to chunk server
				invalidHandle = append(invalidHandle, handle)
				continue
			}
			chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
			chunkMetadata.Lock()
			if chunkMetadata.primary != args.Address {
				chunkMetadata.Unlock()
				// append to slice and reply to chunk server
				notPrimary = append(notPrimary, handle)
				continue
			}
			chunkMetadata.expire = time.Now().Add(gfs.LeaseExpire)
			chunkMetadata.Unlock()
		}
		if len(invalidHandle) == 0 && len(notPrimary) == 0 {
			return nil
		} else {
			reply.InvalidHandles = make([]int64, len(invalidHandle))
			reply.NotPrimary = make([]int64, len(notPrimary))
			copy(reply.InvalidHandles, invalidHandle)
			copy(reply.NotPrimary, notPrimary)
			return fmt.Errorf("something wrong happened in extend lease, see reply for more information")
		}
	}
	return nil
}

// RPCGetReplicas is called by client to find all chunk server that holds the chunk.
// lease holder and secondaries of a chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	// stale chunk server
	var staleServers []string
	// lock for stale chunk server
	var staleLock sync.Mutex
	chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", args.Handle))
	if !ok {
		return fmt.Errorf("cannot find chunk %d", args.Handle)
	}
	chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
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
					// FIXME: deadlock
					// chunkServerInfo and chunkMetadata
					staleLock.Lock()
					staleServers = append(staleServers, addr)
					staleLock.Unlock()
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
				// TODO: solve no replica err
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
	// add garbage
	for _, v := range staleServers {
		csi, e := m.chunkServerInfos.Get(v)
		if !e {
			continue
		}
		chunkServerInfo := csi.(*ChunkServerInfo)
		chunkServerInfo.Lock()
		// TODO: should set false?
		chunkServerInfo.chunks[args.Handle] = false
		chunkServerInfo.garbage = append(chunkServerInfo.garbage, args.Handle)
		chunkServerInfo.Unlock()
	}
	return nil
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	if !ok {
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
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
	if !ok {
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
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
	m.chunkNamespace.Set(fmt.Sprintf("%d", handle), chunkMetadata)

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
	if !ok {
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
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
	if !ok {
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
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
	m.fileNamespace.SetIfAbsent(gfs.DeletedFilePrefix+args.Path, fileMetadata)
	return nil
}

// RPCRenameFile is called by client to rename a file
func (m *Master) RPCRenameFile(args gfs.RenameFileArg, reply *gfs.RenameFileReply) error {
	sourceParents := getParents(args.Source)
	ok, sourceFileMetadatas, err := m.acquireParentsRLocks(sourceParents)
	if !ok {
		return err
	}
	defer m.unlockParentsRLocks(sourceFileMetadatas)
	targetParents := getParents(args.Target)
	ok, targetFileMetadatas, err := m.acquireParentsRLocks(targetParents)
	if !ok {
		return err
	}
	defer m.unlockParentsRLocks(targetFileMetadatas)
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
	if !ok {
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
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
func (m *Master) acquireParentsRLocks(parents []string) (bool, []*FileMetadata, error) {
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
