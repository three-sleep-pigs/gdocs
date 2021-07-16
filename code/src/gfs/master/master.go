package master

import (
	"../../gfs"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

// TODO: lazy delete

// Master struct
type Master struct {
	address    string // master server address
	l          net.Listener
	zk         *zk.Conn
}

type FileMetadata struct {
	isDir bool

	// if it is a file
	size         int64
	chunkHandles []int64
}

type ChunkMetadata struct {
	location []string  // set of replica locations
	primary  string    // primary chunkserver
	expire   time.Time // lease expire time
	version  int64
	checksum int64
	refcnt   int64
}

type ChunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[int64]bool // set of chunks that the chunkserver has
	garbage       []int64
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address string) *Master {
	m := &Master{
		address:    address,
	}
	gfs.DebugMsgToFile("new a master", gfs.MASTER, m.address)

	// connect zookeeper
	conn, err := Connect()
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("zookeeper connect error <%s>", err), gfs.MASTER, m.address)
		return nil
	}
	m.zk = conn

	// if NextHandle doesn't exist, create it and set it to 0
	acls := zk.WorldACL(zk.PermAll)
	m.zk.Create(NextHandle, []byte("0"), 0, acls)
	// TODO: handle error
	// TODO: create ReplicaNeedList
	list := make([]int64, 0)
	SetIfAbsent(m.zk, ReplicaNeedList, list)

	// register rpc server
	rpcs := rpc.NewServer()
	err = rpcs.Register(m)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("rpc server register error <%s>", err), gfs.MASTER, m.address)
		return nil
	}
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("listen error <%s>", e), gfs.MASTER, m.address)
		return nil
	}
	m.l = l
	
	// handle rpc
	go func() {
		for {
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				gfs.DebugMsgToFile(fmt.Sprintf("connect error <%s>", err), gfs.MASTER, m.address)
			}
		}
	}()

	// handle timed chunk server check
	go func() {
		serverCheckTicker := time.Tick(gfs.ServerCheckInterval)
		for {
			<-serverCheckTicker
			err := m.serverCheck()
			if err != nil {
				gfs.DebugMsgToFile(fmt.Sprintf("serverCheck error <%s>", err), gfs.MASTER, m.address)
			}
		}
	}()
	gfs.DebugMsgToFile("master start to serve", gfs.MASTER, m.address)
	return m
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
	var chunkServerInfo ChunkServerInfo
	// no method to delete chunk server info so can not check ok
	Lock(m.zk, GetKey(args.Address, CHUNKSERVERLOCK))
	ok := Get(m.zk, GetKey(args.Address, CHUNKSERVER), &chunkServerInfo)
	if !ok {
		gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server address <%s> ok false so first", args.Address), gfs.MASTER, m.address)
		// new chunk server info
		chunkServerInfoNew := ChunkServerInfo{lastHeartbeat: time.Now(), chunks: make(map[int64]bool),
			garbage: nil}
		SetIfAbsentInMap(m.zk, GetKey(args.Address, CHUNKSERVER), chunkServerInfoNew, CHUNKSERVER)
	} else {
		isFirst = false
		// update time
		chunkServerInfo.lastHeartbeat = time.Now()
		// send garbage
		reply.Garbage = chunkServerInfo.garbage
		for _, v := range chunkServerInfo.garbage {
			chunkServerInfo.chunks[v] = false
		}
		chunkServerInfo.garbage = make([]int64, 0)
		Set(m.zk, GetKey(args.Address, CHUNKSERVER), chunkServerInfo)
		UnLock(m.zk, GetKey(args.Address, CHUNKSERVERLOCK))
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
			Lock(m.zk, GetKey(fmt.Sprintf("%d", v.ChunkHandle), CHUNKMETADATALOCK))
			var chunkMetadata ChunkMetadata
			e := Get(m.zk, GetKey(fmt.Sprintf("%d", v.ChunkHandle), CHUNKMETADATA), &chunkMetadata)
			if !e {
				// TODO: chunk server report a chunk handle not in chunk namespace
				UnLock(m.zk, GetKey(fmt.Sprintf("%d", v.ChunkHandle), CHUNKMETADATALOCK))
				continue
			}
			if v.Checksum == chunkMetadata.checksum && v.Version == chunkMetadata.version {
				gfs.DebugMsgToFile(fmt.Sprintf("RPCHeartbeat chunk server address <%s> append chunk metadata", args.Address), gfs.MASTER, m.address)
				chunkMetadata.location = append(chunkMetadata.location, args.Address)
			} else {
				garbage = append(garbage, v.ChunkHandle)
			}
			Set(m.zk, GetKey(fmt.Sprintf("%d", v.ChunkHandle), CHUNKMETADATA), chunkMetadata)
			UnLock(m.zk, GetKey(fmt.Sprintf("%d", v.ChunkHandle), CHUNKMETADATALOCK))
		}
		// set garbage
		chunkServerInfo.garbage = garbage
		Set(m.zk, GetKey(args.Address, CHUNKSERVER), chunkServerInfo)
		UnLock(m.zk, GetKey(args.Address, CHUNKSERVERLOCK))
	} else {
		// use slice to avoid handling only front leases to extend
		var invalidHandle []int64 = make([]int64, 0)
		var notPrimary []int64 = make([]int64, 0)
		for _, handle := range args.ToExtendLeases {
			// extend lease
			Lock(m.zk, GetKey(fmt.Sprintf("%d", handle), CHUNKMETADATALOCK))
			var chunkMetadata ChunkMetadata
			e := Get(m.zk, GetKey(fmt.Sprintf("%d", handle), CHUNKMETADATA), &chunkMetadata)
			if !e {
				// append to slice and reply to chunk server
				invalidHandle = append(invalidHandle, handle)
				UnLock(m.zk, GetKey(fmt.Sprintf("%d", handle), CHUNKMETADATALOCK))
				continue
			}
			if chunkMetadata.primary != args.Address {
				// append to slice and reply to chunk server
				notPrimary = append(notPrimary, handle)
				UnLock(m.zk, GetKey(fmt.Sprintf("%d", handle), CHUNKMETADATALOCK))
				continue
			}
			chunkMetadata.expire = time.Now().Add(gfs.LeaseExpire)
			Set(m.zk, GetKey(fmt.Sprintf("%d", handle), CHUNKMETADATA), chunkMetadata)
			UnLock(m.zk, GetKey(fmt.Sprintf("%d", handle), CHUNKMETADATALOCK))
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
			return err
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
	var chunkMetadata ChunkMetadata
	Lock(m.zk, GetKey(fmt.Sprintf("%d", args.Handle), CHUNKMETADATALOCK))
	ok := Get(m.zk, GetKey(fmt.Sprintf("%d", args.Handle), CHUNKMETADATA), &chunkMetadata)
	if !ok {
		err := fmt.Errorf("cannot find chunk %d", args.Handle)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> error <%s>", args.Handle, err), gfs.MASTER, m.address)
		return err
	}
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
			Lock(m.zk, ReplicaNeedListLock)
			var replicasNeedList []string
			Get(m.zk, ReplicaNeedList, &replicasNeedList)
			replicasNeedList = append(replicasNeedList, fmt.Sprintf("%d", args.Handle))
			Set(m.zk, ReplicaNeedList, replicasNeedList)
			UnLock(m.zk, ReplicaNeedListLock)

			if len(chunkMetadata.location) == 0 {
				// TODO: solve no replica err
				chunkMetadata.version--
				Set(m.zk, GetKey(fmt.Sprintf("%d", args.Handle), CHUNKMETADATA), chunkMetadata)
				err := fmt.Errorf("no replica of %v", args.Handle)
				gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> error <%s>", args.Handle, err), gfs.MASTER, m.address)
				UnLock(m.zk, GetKey(fmt.Sprintf("%d", args.Handle), CHUNKMETADATALOCK))
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
	Set(m.zk, GetKey(fmt.Sprintf("%d", args.Handle), CHUNKMETADATA), chunkMetadata)
	gfs.DebugMsgToFile(fmt.Sprintf("RPCGetReplicas chunk handle <%d> release chunk metadata lock", args.Handle), gfs.MASTER, m.address)
	UnLock(m.zk, GetKey(fmt.Sprintf("%d", args.Handle), CHUNKMETADATALOCK))
	// add garbage
	for _, v := range staleServers {
		Lock(m.zk, GetKey(v, CHUNKSERVERLOCK))
		var chunkServerInfo ChunkServerInfo
		e := Get(m.zk, GetKey(v, CHUNKSERVER), &chunkServerInfo)
		if !e {
			UnLock(m.zk, GetKey(v, CHUNKSERVERLOCK))
			continue
		}
		chunkServerInfo.chunks[args.Handle] = false
		chunkServerInfo.garbage = append(chunkServerInfo.garbage, args.Handle)
		Set(m.zk, GetKey(v, CHUNKSERVER), chunkServerInfo)
		UnLock(m.zk, GetKey(v, CHUNKSERVERLOCK))
	}
	return nil
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCGetFileInfo path <%s> start", args.Path), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCGetFileInfo path <%s> end", args.Path), gfs.MASTER, m.address)
	var fileMetadata FileMetadata
	Lock(m.zk, GetKey(args.Path, FILEMETADATALOCK))
	defer UnLock(m.zk, GetKey(args.Path, FILEMETADATALOCK))
	exist := Get(m.zk, GetKey(args.Path, FILEMETADATA), &fileMetadata)
	if !exist {
		err := fmt.Errorf("path %s does not exsit", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetFileInfo path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
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
	var fileMetadata FileMetadata
	Lock(m.zk, GetKey(args.Path, FILEMETADATALOCK))
	defer UnLock(m.zk, GetKey(args.Path, FILEMETADATALOCK))
	exist := Get(m.zk, GetKey(args.Path, FILEMETADATA), &fileMetadata)
	if !exist {
		err := fmt.Errorf("path %s does not exsit", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> error <%s>", args.Path, args.Index, err), gfs.MASTER, m.address)
		return err
	}
	if int(args.Index) == len(fileMetadata.chunkHandles) {
		addrs, e := m.chooseServers(gfs.DefaultNumReplicas)
		if e != nil {
			gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> error <%s>", args.Path, args.Index, e), gfs.MASTER, m.address)
			return e
		}

		reply.Handle, addrs, e = m.createChunk(&fileMetadata, addrs)
		Set(m.zk, GetKey(args.Path, FILEMETADATA), fileMetadata)
		if e != nil {
			e = fmt.Errorf("create chunk for path %s failed in some chunk servers %s", args.Path, err)
			gfs.DebugMsgToFile(fmt.Sprintf("RPCGetChunkHandle path <%s> index <%d> error <%s>", args.Path, args.Index, err), gfs.MASTER, m.address)
			return e
		}

	} else {
		if args.Index < 0 || int(args.Index) >= len(fileMetadata.chunkHandles) {
			err := fmt.Errorf("invalid index for %s[%d]", args.Path, args.Index)
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
	// TODO: check if there is enough chunk servers for replica
	var ret []string
	var all []string
	Get(m.zk, ChunkServerKeyList, &all)
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
	handle, err := GetHandle(m.zk)
	if err != nil {
		return -1, nil, err
	}
	// update file info
	fileMetadata.chunkHandles = append(fileMetadata.chunkHandles, handle)
	var chunkMetadata ChunkMetadata
	chunkMetadata.version = 0
	chunkMetadata.refcnt = 1
	// TODO: checksum
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
			var chunkServerInfo ChunkServerInfo
			infoOk := Get(m.zk, GetKey(v, CHUNKSERVER), &chunkServerInfo)
			if infoOk {
				Lock(m.zk, GetKey(v, CHUNKSERVERLOCK))
				chunkServerInfo.chunks[handle] = true
				Set(m.zk, GetKey(v, CHUNKSERVER), chunkServerInfo)
				UnLock(m.zk, GetKey(v, CHUNKSERVERLOCK))
			}
		} else {
			errList += err.Error() + ";"
		}
	}
	SetIfAbsentInMap(m.zk, GetKey(fmt.Sprintf("%d", handle), CHUNKMETADATA), chunkMetadata, CHUNKMETADATA)

	if errList == "" {
		return handle, success, nil
	} else {
		// replicas are no enough, add to need list
		Lock(m.zk, ReplicaNeedListLock)
		var replicasNeedList []string
		Get(m.zk, ReplicaNeedList, &replicasNeedList)
		replicasNeedList = append(replicasNeedList, fmt.Sprintf("%d", handle))
		Set(m.zk, ReplicaNeedList, replicasNeedList)
		UnLock(m.zk, ReplicaNeedListLock)
		err := fmt.Errorf(errList)
		gfs.DebugMsgToFile(fmt.Sprintf("CreateChunk error <%s>", err), gfs.MASTER, m.address)
		return handle, success, err
	}
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCCreateFile path <%s> start", args.Path), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCCreateFile path <%s> end", args.Path), gfs.MASTER, m.address)
	fileMetadata := new(FileMetadata)
	fileMetadata.isDir = false
	fileMetadata.size = 0
	fileMetadata.chunkHandles = nil
	ok := SetIfAbsentInMap(m.zk, GetKey(args.Path, FILEMETADATA), *fileMetadata, FILEMETADATA)
	if !ok {
		err := fmt.Errorf("path %s has already existed", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCCreateFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	return nil
}

// RPCDeleteFile is called by client to delete a file
func (m *Master) RPCDeleteFile(args gfs.DeleteFileArg, reply *gfs.DeleteFileReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> start", args.Path), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> end", args.Path), gfs.MASTER, m.address)
	var fileMetadata FileMetadata
	key := GetKey(args.Path, FILEMETADATA)
	exist := Get(m.zk, key, &fileMetadata)
	if !exist {
		err := fmt.Errorf("path %s does not exsit", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	Lock(m.zk, GetKey(args.Path, FILEMETADATALOCK))
	if fileMetadata.isDir {
		UnLock(m.zk, key)
		err := fmt.Errorf("path %s is not a file", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCDeleteFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	// may fail but without error throw
	RemoveInMap(m.zk, key, FILEMETADATA)
	UnLock(m.zk, GetKey(args.Path, FILEMETADATALOCK))
	// lazy delete
	// may fail
	SetIfAbsentInMap(m.zk, GetKey(gfs.DeletedFilePrefix+args.Path, FILEMETADATA), fileMetadata, FILEMETADATA)
	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	gfs.DebugMsgToFile(fmt.Sprintf("RPCMkdir path <%s> start", args.Path), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("RPCMkdir path <%s> end", args.Path), gfs.MASTER, m.address)
	fileMetadata := new(FileMetadata)
	fileMetadata.isDir = true
	fileMetadata.size = 0
	fileMetadata.chunkHandles = nil
	ok := SetIfAbsentInMap(m.zk, GetKey(args.Path, FILEMETADATA), *fileMetadata, FILEMETADATA)
	if !ok {
		err := fmt.Errorf("path %s has already existed", args.Path)
		gfs.DebugMsgToFile(fmt.Sprintf("RPCCreateFile path <%s> error <%s>", args.Path, err), gfs.MASTER, m.address)
		return err
	}
	return nil
}