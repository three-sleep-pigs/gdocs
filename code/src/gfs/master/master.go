package master

import (
	"../../gfs"
	"../cmap"
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
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
	metaFileLock sync.Mutex // protect metadata file

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
	valid 		  bool
}

type PersistentFileMetadata struct {
	Path string

	IsDir bool

	// if it is a file
	Size         int64
	ChunkHandles []int64
}

type PersistentChunkMetadata struct {
	ChunkHandle int64

	Version  int64
	Checksum int64
	Refcnt   int64
}

type PersistentMetadata struct {
	NextHandle int64
	ChunkMeta  []PersistentChunkMetadata
	FileMeta   []PersistentFileMetadata
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
	gfs.DebugMsgToFile("new a master", gfs.MASTER, m.address)
	// initial 3 concurrent maps
	m.fileNamespace = cmap.New()
	m.chunkNamespace = cmap.New()
	m.chunkServerInfos = cmap.New()

	// initial metadata
	_, err := os.Stat(serverRoot) //check whether rootDir exists, if not, mkdir it
	if err != nil {
		err = os.Mkdir(serverRoot, 0777)
		if err != nil {
			gfs.DebugMsgToFile(fmt.Sprintf("mkdir error <%s>", err), gfs.MASTER, m.address)
			return nil
		}
	}
	err = m.loadMeta()
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("loadMeta error <%s>", err), gfs.MASTER, m.address)
	}

	// register rpc server
	rpcs := rpc.NewServer()
	err = rpcs.Register(m)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("rpc server register error <%s>", err), gfs.MASTER, m.address)
		return nil
	}
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("listen error <%s>", err), gfs.MASTER, m.address)
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
					gfs.DebugMsgToFile(fmt.Sprintf("connect error <%s>", err), gfs.MASTER, m.address)
				}
			}
		}
	}()

	// handle timed task
	go func() {
		serverCheckTicker := time.Tick(gfs.ServerCheckInterval)
		storeMetaTicker := time.Tick(gfs.StoreMetaInterval)
		for {
			select {
			case <-m.shutdown:
				return
			case <-serverCheckTicker:
				{
					if m.dead {     // check if shutdown
						return
					}
					err := m.serverCheck()
					if err != nil {
						gfs.DebugMsgToFile(fmt.Sprintf("serverCheck error <%s>", err), gfs.MASTER, m.address)
					}
				}
			case <-storeMetaTicker:
				{
					if m.dead {     // check if shutdown
						return
					}
					err := m.storeMeta()
					if err != nil {
						gfs.DebugMsgToFile(fmt.Sprintf("storeMeta error <%s>", err), gfs.MASTER, m.address)
					}
				}
			}
		}
	}()
	gfs.DebugMsgToFile("master start to serve", gfs.MASTER, m.address)
	return m
}

// loadMeta loads metadata from disk
func (m *Master) loadMeta() error {
	gfs.DebugMsgToFile("load meta start", gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile("load meta end", gfs.MASTER, m.address)
	filename := path.Join(m.serverRoot, gfs.MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentMetadata
	dec := gob.NewDecoder(file)
	err = dec.Decode(&meta)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("load meta error <%s>", err), gfs.MASTER, m.address)
		return err
	}

	m.nextHandle = meta.NextHandle

	for _, pf := range meta.FileMeta {
		f := new(FileMetadata)
		f.isDir = pf.IsDir
		f.size = pf.Size
		f.chunkHandles = pf.ChunkHandles
		e := m.fileNamespace.SetIfAbsent(pf.Path, f)
		if !e {
			gfs.DebugMsgToFile("set file metadata exist", gfs.MASTER, m.address)
		}
	}

	for _, pc := range meta.ChunkMeta {
		c := new(ChunkMetadata)
		c.version = pc.Version
		c.checksum = pc.Checksum
		c.refcnt = pc.Refcnt
		e := m.chunkNamespace.SetIfAbsent(fmt.Sprintf("%d", pc.ChunkHandle), c)
		if !e {
			gfs.DebugMsgToFile("set chunk metadata exist", gfs.MASTER, m.address)
		}
	}

	return nil
}

// storeMeta stores metadata to disk
func (m *Master) storeMeta() error {
	gfs.DebugMsgToFile("store meta start", gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile("store meta end", gfs.MASTER, m.address)
	m.metaFileLock.Lock() // prevent storeMeta from being called concurrently
	defer m.metaFileLock.Unlock()

	filename := path.Join(m.serverRoot, gfs.MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("store meta error <%s>", err), gfs.MASTER, m.address)
		return err
	}
	defer file.Close()

	var meta PersistentMetadata

	for tuple := range m.fileNamespace.IterBuffered() {
		f := tuple.Val.(*FileMetadata)
		f.RLock()
		meta.FileMeta = append(meta.FileMeta, PersistentFileMetadata{
			Path:         tuple.Key,
			IsDir:        f.isDir,
			Size:         f.size,
			ChunkHandles: f.chunkHandles,
		})
		f.RUnlock()
	}

	for tuple := range m.chunkNamespace.IterBuffered() {
		h, err := strconv.ParseInt(tuple.Key, 10, 64)
		if err != nil {
			gfs.DebugMsgToFile(fmt.Sprintf("parse chunk handle error <%s>", err), gfs.MASTER, m.address)
			continue
		}
		c := tuple.Val.(*ChunkMetadata)
		c.RLock()
		meta.ChunkMeta = append(meta.ChunkMeta, PersistentChunkMetadata{
			ChunkHandle: h,
			Version:     c.version,
			Checksum:    c.checksum,
			Refcnt:      c.refcnt,
		})
		c.RUnlock()
	}

	m.nhLock.Lock()
	meta.NextHandle = m.nextHandle
	m.nhLock.Unlock()

	enc := gob.NewEncoder(file)
	err = enc.Encode(meta)
	return err
}

// Shutdown shuts down master
// FIXME: Shutdown shouldn't be called concurrently because TOCTTOU of m.dead
// no need to fix it
func (m *Master) Shutdown() {
	gfs.DebugMsgToFile("shut down start", gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile("shut down end", gfs.MASTER, m.address)
	if !m.dead {
		m.dead = true
		// close will cause case <- shutdown part get value 0
		// end the rpc goroutine and the server check, store goroutine
		close(m.shutdown)
		err := m.l.Close()
		if err != nil {
			gfs.DebugMsgToFile(fmt.Sprintf("close listener error <%s>", err), gfs.MASTER, m.address)
		}
		err = m.storeMeta()
		if err != nil {
			gfs.DebugMsgToFile(fmt.Sprintf("store metadata error <%s>", err), gfs.MASTER, m.address)
		}
	}
}

// serverCheck checks chunkServer
// set disconnected chunk servers to invalid and remove them from chunk location
// then add replicas for chunks in replicasNeedList
func (m *Master) serverCheck() error {
	gfs.DebugMsgToFile("server check start", gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile("server check end", gfs.MASTER, m.address)
	// detect and remove dead servers
	now := time.Now()
	for tuple := range m.chunkServerInfos.IterBuffered() {
		cs := tuple.Val.(*ChunkServerInfo)
		cs.Lock()
		if cs.valid && cs.lastHeartbeat.Add(gfs.ServerTimeout).Before(now) { // dead server
			gfs.DebugMsgToFile(fmt.Sprintf("server check remove dead server <%s>", tuple.Key), gfs.MASTER, m.address)
			cs.valid = false // set to invalid
			for h, v := range cs.chunks {
				if v { // remove from chunk location
					chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", h))
					if !ok {
						gfs.DebugMsgToFile(fmt.Sprintf("chunk <%v> doesn't exist", h), gfs.MASTER, m.address)
						continue
					}
					chunkMetadata := chunkMetadataFound.(*ChunkMetadata)

					chunkMetadata.Lock()
					var newLocation []string
					for _, l := range chunkMetadata.location {
						if l != tuple.Key {
							newLocation = append(newLocation, l)
						}
					}
					chunkMetadata.location = newLocation
					// update primary
					if chunkMetadata.primary == tuple.Key {
						chunkMetadata.primary = ""
					}
					chunkMetadata.expire = time.Now()

					// add chunk to replicasNeedList if replica is not enough
					if len(chunkMetadata.location) < gfs.MinimumNumReplicas {
						m.rnlLock.Lock()
						m.replicasNeedList = append(m.replicasNeedList, h)
						m.rnlLock.Unlock()
						if len(chunkMetadata.location) == 0 {
							gfs.DebugMsgToFile(fmt.Sprintf("chunk <%d> has no replica", h), gfs.MASTER, m.address)
						}
					}
					chunkMetadata.Unlock()
				}
			}
		}
		cs.Unlock()
	}

	// add replicas
	err := m.reReplicationAll()
	return err
}

// reReplicationAll adds replicas for all chunks to be replicated
func (m *Master) reReplicationAll() error {
	gfs.DebugMsgToFile("reReplicationAll start", gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile("reReplicationAll end", gfs.MASTER, m.address)
	m.rnlLock.Lock()
	oldNeedList := m.replicasNeedList
	m.replicasNeedList = make([]int64, 0)
	m.rnlLock.Unlock()

	var newNeedList []int64
	for _, h := range oldNeedList {
		chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", h))
		if !ok {
			gfs.DebugMsgToFile(fmt.Sprintf("chunk <%d> doesn't exist", h), gfs.MASTER, m.address)
			continue
		}
		chunkMetadata := chunkMetadataFound.(*ChunkMetadata)

		// lock chunk so master will not grant lease during copy time
		chunkMetadata.Lock()
		if len(chunkMetadata.location) < gfs.MinimumNumReplicas {
			if chunkMetadata.expire.Before(time.Now()) {
				err := m.reReplicationOne(h, chunkMetadata)
				if err != nil {
					gfs.DebugMsgToFile(fmt.Sprintf("reReplication error <%s>", err), gfs.MASTER, m.address)
					newNeedList = append(newNeedList, h)
				}
			} else {
				newNeedList = append(newNeedList, h)
			}
		}
		chunkMetadata.Unlock()
	}

	m.rnlLock.Lock()
	m.replicasNeedList = append(m.replicasNeedList, newNeedList...)
	m.rnlLock.Unlock()
	return nil
}

// reReplicationOne adds replica for one chunk, chunk meta should be locked in top caller
func (m *Master) reReplicationOne(handle int64, chunk *ChunkMetadata) error {
	gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%d> start", handle), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%d> end", handle), gfs.MASTER, m.address)
	// holding corresponding chunk metadata lock now
	from, to, err := m.chooseReReplication(handle)
	if err != nil {
		return err
	}

	var cr gfs.CreateChunkReply
	err = gfs.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &cr)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%d> error <%s>", handle, err), gfs.MASTER, m.address)
		return err
	}

	var sr gfs.SendCopyReply
	err = gfs.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &sr)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%d> error <%s>", handle, err), gfs.MASTER, m.address)
		return err
	}

	// add handle in chunk server info of to
	chunkServerInfoFound, ok := m.chunkServerInfos.Get(to)
	if !ok {
		err = fmt.Errorf("add chunk in removed server %s", to)
		gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%d> error <%s>", handle, err), gfs.MASTER, m.address)
		return err
	}
	chunkServerInfo := chunkServerInfoFound.(*ChunkServerInfo)
	chunkServerInfo.Lock()
	if !chunkServerInfo.valid {
		chunkServerInfo.Unlock()
		err = fmt.Errorf("add chunk in invalid server %s", to)
		gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%d> error <%s>", handle, err), gfs.MASTER, m.address)
		return err
	}
	chunkServerInfo.chunks[handle] = true
	chunkServerInfo.Unlock()

	// add to location of chunk
	chunk.location = append(chunk.location, to)
	return nil
}

// TODO: improve selection strategy
// chooseReReplication chooses reReplication src and dst
func (m *Master) chooseReReplication(handle int64) (from, to string, err error) {
	gfs.DebugMsgToFile(fmt.Sprintf("chooseReReplication handle <%d> start", handle), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("chooseReReplication handle <%d> end", handle), gfs.MASTER, m.address)
	from = ""
	to = ""
	err = nil
	for tuple := range m.chunkServerInfos.IterBuffered() {
		cs := tuple.Val.(*ChunkServerInfo)
		cs.RLock()
		if cs.valid {
			if cs.chunks[handle] {
				from = tuple.Key
			} else {
				to = tuple.Key
			}
		}
		cs.RUnlock()
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("no enough server for replica %v", handle)
	gfs.DebugMsgToFile(fmt.Sprintf("chooseReReplication handle <%d> error <%s>", handle, err), gfs.MASTER, m.address)
	return
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server address <%s> start", args.Address), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server address <%s> end", args.Address), gfs.MASTER, m.address)
	isFirst := true
	var chunkServerInfo *ChunkServerInfo
	// no method to delete chunk server info so can not check ok
	chunkServerInfoFound, ok:= m.chunkServerInfos.Get(args.Address)
	if !ok {
		gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server address <%s> ok false so first", args.Address), gfs.MASTER, m.address)
		// new chunk server info
		chunkServerInfoNew := &ChunkServerInfo{lastHeartbeat: time.Now(), chunks: make(map[int64]bool),
			garbage: nil, valid: true}
		m.chunkServerInfos.SetIfAbsent(args.Address, chunkServerInfoNew)
		chunkServerInfo = chunkServerInfoNew
	} else {
		chunkServerInfoOld := chunkServerInfoFound.(*ChunkServerInfo)
		chunkServerInfoOld.Lock()
		if !chunkServerInfoOld.valid {
			gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server address <%s> valid false so first", args.Address), gfs.MASTER, m.address)
			isFirst = true
			// new chunk server info
			chunkServerInfoNew := &ChunkServerInfo{lastHeartbeat: time.Now(), chunks: make(map[int64]bool),
				garbage: nil, valid: true}
			m.chunkServerInfos.SetIfAbsent(args.Address, chunkServerInfoNew)
			chunkServerInfo = chunkServerInfoNew
			chunkServerInfoOld.Unlock()
		} else {
			isFirst = false
			// update time
			chunkServerInfoOld.lastHeartbeat = time.Now()
			// send garbage
			reply.Garbage = chunkServerInfoOld.garbage
			for _, v := range chunkServerInfoOld.garbage {
				chunkServerInfoOld.chunks[v] = false
			}
			chunkServerInfoOld.garbage = make([]int64, 0)
			chunkServerInfo = chunkServerInfoOld
			chunkServerInfoOld.Unlock()
		}
	}
	if isFirst {
		// if is first heartbeat, let chunk server report itself
		var r gfs.ReportSelfReply
		err := gfs.Call(args.Address, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &r)
		if err != nil {
			gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server addreee <%s> start err <%s>", args.Address, err), gfs.MASTER, m.address)
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
				gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server address <%s> append chunk metadata", args.Address), gfs.MASTER, m.address)
				chunkMetadata.location = append(chunkMetadata.location, args.Address)
			} else {
				garbage = append(garbage, v.ChunkHandle)
			}
			chunkMetadata.Unlock()
		}
		// set garbage
		chunkServerInfo.Lock()
		chunkServerInfo.garbage = garbage
		chunkServerInfo.Unlock()
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
			err := fmt.Errorf("something wrong happened in extend lease, see reply for more information")
			gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server addreee <%s> start err <%s>", args.Address, err), gfs.MASTER, m.address)
			return nil
		}
	}
	return nil
}

// RPCGetReplicas is called by client to find all chunk server that holds the chunk.
// lease holder and secondaries of a chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> start", args.Handle), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> end", args.Handle), gfs.MASTER, m.address)
	// stale chunk server
	var staleServers []string
	// lock for stale chunk server
	var staleLock sync.Mutex
	chunkMetadataFound, ok := m.chunkNamespace.Get(fmt.Sprintf("%d", args.Handle))
	if !ok {
		err := fmt.Errorf("cannot find chunk %d", args.Handle)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> error <%s>", args.Handle, err), gfs.MASTER, m.address)
		return err
	}
	chunkMetadata := chunkMetadataFound.(*ChunkMetadata)
	chunkMetadata.Lock()
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
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> call chunk servers len <%d>" , args.Handle, len(chunkMetadata.location)), gfs.MASTER, m.address)
		for _, v := range chunkMetadata.location {
			go func(addr string) {
				var ret gfs.CheckVersionReply
				// call rpc to let all chunk servers check their own version
				err := gfs.Call(addr, "ChunkServer.RPCCheckVersion", checkVersionArg, &ret)
				if err == nil && ret.Stale == false {
					gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> call chunk server <%s>" +
						"check version successfully set version <%d>", args.Handle, addr, checkVersionArg.Version), gfs.MASTER, m.address)
					lock.Lock()
					newList = append(newList, addr)
					lock.Unlock()
				} else {
					// add to garbage collection
					// must exist no need to check ok
					// FIXME: deadlock solved
					// by releasing chunkMetadata Lock before acquiring chunkServerInfo Lock for adding garbage
					// chunkServerInfo and chunkMetadata
					gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> call chunk server <%s>" +
						"check version error <%s> with version <%d>", args.Handle, addr, err, checkVersionArg.Version), gfs.MASTER, m.address)
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
				err := fmt.Errorf("no replica of %v", args.Handle)
				gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> error <%s>", args.Handle, err), gfs.MASTER, m.address)
				chunkMetadata.Unlock()
				return err
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
	gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> release chunk metadata lock", args.Handle), gfs.MASTER, m.address)
	chunkMetadata.Unlock()
	// add garbage
	for _, v := range staleServers {
		csi, e := m.chunkServerInfos.Get(v)
		if !e {
			continue
		}
		chunkServerInfo := csi.(*ChunkServerInfo)
		chunkServerInfo.Lock()
		chunkServerInfo.chunks[args.Handle] = false
		chunkServerInfo.garbage = append(chunkServerInfo.garbage, args.Handle)
		chunkServerInfo.Unlock()
	}
	return nil
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCGetFileInfo path <%s> start", args.Path), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCGetFileInfo path <%s> end", args.Path), gfs.MASTER, m.address)
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	if !ok {
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetFileInfo path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
	fileMetadataFound, exist := m.fileNamespace.Get(args.Path)
	if !exist {
		err = fmt.Errorf("path %s does not exsit", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetFileInfo path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
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
	gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> start", args.Path, args.Index), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> end", args.Path, args.Index), gfs.MASTER, m.address)
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	if !ok {
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> error <%s>", args.Path, args.Index, err), gfs.MASTER, m.address)
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
	fileMetadataFound, exist := m.fileNamespace.Get(args.Path)
	if !exist {
		err = fmt.Errorf("path %s does not exsit", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> error <%s>", args.Path, args.Index, err), gfs.MASTER, m.address)
		return err
	}
	fileMetadata := fileMetadataFound.(*FileMetadata)
	fileMetadata.Lock()
	defer fileMetadata.Unlock()
	if int(args.Index) == len(fileMetadata.chunkHandles) {
		addrs, e := m.chooseServers(gfs.DefaultNumReplicas)
		if e != nil {
			gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> error <%s>", args.Path, args.Index, e), gfs.MASTER, m.address)
			return e
		}

		reply.Handle, addrs, err = m.createChunk(fileMetadata, addrs)
		if err != nil {
			err = fmt.Errorf("create chunk for path %s failed in some chunk servers %s", args.Path, err)
			gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> error <%s>", args.Path, args.Index, err), gfs.MASTER, m.address)
			return err
		}

	} else {
		if args.Index < 0 || int(args.Index) >= len(fileMetadata.chunkHandles) {
			err = fmt.Errorf("invalid index for %s[%d]", args.Path, args.Index)
			gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> error <%s>", args.Path, args.Index, err), gfs.MASTER, m.address)
			return err
		}
		reply.Handle = fileMetadata.chunkHandles[args.Index]
	}
	return nil
}

// ChooseServers returns servers to store new chunk
// called when a new chunk is create
func (m *Master) chooseServers(num int) ([]string, error) {
	gfs.DebugMsgToFile(fmt.Sprintf("chooseServers num <%d> start", num), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("chooseServers num <%d> end", num), gfs.MASTER, m.address)
	if num > m.chunkServerInfos.Count() {
		err := fmt.Errorf("no enough servers for %d replicas", num)
		gfs.DebugMsgToFile(fmt.Sprintf("chooseServers num <%d> error <%s>", num, err), gfs.MASTER, m.address)
		return nil, err
	}

	var ret []string
	all := m.chunkServerInfos.Keys()
	choose, err := gfs.Sample(len(all), num)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("chooseServers num <%d> error <%s>", num, err), gfs.MASTER, m.address)
		return nil, err
	}
	for _, v := range choose {
		ret = append(ret, all[v])
	}

	return ret, nil
}

// CreateChunk creates a new chunk for path. servers for the chunk are denoted by addrs
// returns the handle of the new chunk, and the servers that create the chunk successfully
func (m *Master) createChunk(fileMetadata *FileMetadata, addrs []string) (int64, []string, error) {
	gfs.DebugMsgToFile(fmt.Sprintf("CreateChunk start"), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("CreateChunk end"), gfs.MASTER, m.address)
	m.nhLock.Lock()
	handle := m.nextHandle
	m.nextHandle++
	m.nhLock.Unlock()
	// update file info
	fileMetadata.chunkHandles = append(fileMetadata.chunkHandles, handle)
	chunkMetadata := new(ChunkMetadata)
	// TODO: deadlock
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

		err := gfs.Call(v, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &r)
		if err == nil {
			gfs.DebugMsgToFile(fmt.Sprintf("CreateChunk append location <%s> to chunk metadata", v), gfs.MASTER, m.address)
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
		err := fmt.Errorf(errList)
		gfs.DebugMsgToFile(fmt.Sprintf("CreateChunk error <%s>", err), gfs.MASTER, m.address)
		return handle, success, err
	}
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCCreateFile path <%s> start", args.Path), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCCreateFile path <%s> end", args.Path), gfs.MASTER, m.address)
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	if !ok {
		gfs.DebugMsgToFile(fmt.Sprintf("RPCCreateFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
	fileMetadata := new(FileMetadata)
	fileMetadata.isDir = false
	fileMetadata.size = 0
	fileMetadata.chunkHandles = nil
	ok = m.fileNamespace.SetIfAbsent(args.Path, fileMetadata)
	if !ok {
		err = fmt.Errorf("path %s has already existed", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCCreateFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	return nil
}

// RPCDeleteFile is called by client to delete a file
func (m *Master) RPCDeleteFile(args gfs.DeleteFileArg, reply *gfs.DeleteFileReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> start", args.Path), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> end", args.Path), gfs.MASTER, m.address)
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	if !ok {
		gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	defer m.unlockParentsRLocks(fileMetadatas)
	fileMetadataFound, exist := m.fileNamespace.Get(args.Path)
	if !exist {
		err = fmt.Errorf("path %s does not exsit", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	var fileMetadata = fileMetadataFound.(*FileMetadata)
	fileMetadata.RLock()
	if fileMetadata.isDir {
		fileMetadata.RUnlock()
		err = fmt.Errorf("path %s is not a file", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
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
	gfs.DebugMsgToFile(fmt.Sprintf("RPCRenameFile source <%s> target <%s> start", args.Source, args.Target), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCRenameFile source <%s> target <%s> end", args.Source, args.Target), gfs.MASTER, m.address)
	sourceParents := getParents(args.Source)
	ok, sourceFileMetadatas, err := m.acquireParentsRLocks(sourceParents)
	if !ok {
		gfs.DebugMsgToFile(fmt.Sprintf("RPCRenameFile source <%s> target <%s> error <%s>", args.Source, args.Target, err), gfs.MASTER, m.address)
		return err
	}
	defer m.unlockParentsRLocks(sourceFileMetadatas)
	targetParents := getParents(args.Target)
	ok, targetFileMetadatas, err := m.acquireParentsRLocks(targetParents)
	if !ok {
		gfs.DebugMsgToFile(fmt.Sprintf("RPCRenameFile source <%s> target <%s> error <%s>", args.Source, args.Target, err), gfs.MASTER, m.address)
		return err
	}
	defer m.unlockParentsRLocks(targetFileMetadatas)
	sourceFileMetadataFound, sourceExist := m.fileNamespace.Get(args.Source)
	if !sourceExist {
		err = fmt.Errorf("source path %s has not existed", args.Source)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCRenameFile source <%s> target <%s> error <%s>", args.Source, args.Target, err), gfs.MASTER, m.address)
		return err
	}
	sourceFileMetadata := sourceFileMetadataFound.(*FileMetadata)
	sourceFileMetadata.RLock()
	defer sourceFileMetadata.RUnlock()
	if sourceFileMetadata.isDir {
		err = fmt.Errorf("source path %s is not a file", args.Source)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCRenameFile source <%s> target <%s> error <%s>", args.Source, args.Target, err), gfs.MASTER, m.address)
		return err
	}
	setOk := m.fileNamespace.SetIfAbsent(args.Target, sourceFileMetadata)
	if !setOk {
		err = fmt.Errorf("target path %s has already existed", args.Target)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCRenameFile source <%s> target <%s> error <%s>", args.Source, args.Target, err), gfs.MASTER, m.address)
		return err
	}
	m.fileNamespace.Remove(args.Source)
	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCMkdir path <%s> start", args.Path), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCMkdir path <%s> end", args.Path), gfs.MASTER, m.address)
	parents := getParents(args.Path)
	ok, fileMetadatas, err := m.acquireParentsRLocks(parents)
	if !ok {
		return err
		gfs.DebugMsgToFile(fmt.Sprintf("RPCMkdir path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
	}
	defer m.unlockParentsRLocks(fileMetadatas)
	fileMetadata := new(FileMetadata)
	fileMetadata.isDir = true
	fileMetadata.size = 0
	fileMetadata.chunkHandles = nil
	ok = m.fileNamespace.SetIfAbsent(args.Path, fileMetadata)
	if !ok {
		err = fmt.Errorf("path %s has already existed", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCMkdir path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
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
