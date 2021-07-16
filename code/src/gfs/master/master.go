package master

import (
	"../../gfs"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"net"
	"net/rpc"
	"strconv"
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

	// If these data structures don't exist, create them
	acls := zk.WorldACL(zk.PermAll)
	m.zk.Create(NextHandle, []byte("0"), 0, acls)
	SetIfAbsent(m.zk, ReplicaNeedList, make([]string, 0))
	SetIfAbsent(m.zk, ChunkMetaKeyList, make([]string, 0))
	SetIfAbsent(m.zk, FileMetaKeyList, make([]string, 0))
	SetIfAbsent(m.zk, ChunkServerKeyList, make([]string, 0))

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
	// use ServerCheckLock to ensure that only one master is running serverCheck
	acls := zk.WorldACL(zk.PermAll)
	_, err := m.zk.Create(ServerCheckLock, []byte("1"), 0, acls)
	if err != nil {
		data, sate, er := m.zk.Get(ServerCheckLock)
		if er != nil {
			return er
		}
		if string(data) == "0" {
			_, e := m.zk.Set(ServerCheckLock, []byte("1"), sate.Version)
			if e != nil {
				return nil
			}
		} else {
			return nil
		}
	}
	defer UnLock(m.zk, ServerCheckLock)

	gfs.DebugMsgToFile("server check start", gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile("server check end", gfs.MASTER, m.address)
	// detect and remove dead servers
	var chunkServerList []string
	Get(m.zk, ChunkServerKeyList, &chunkServerList)
	now := time.Now()
	for _, addr := range chunkServerList {
		Lock(m.zk, GetKey(addr, CHUNKSERVERLOCK))
		var cs ChunkServerInfo
		ok := Get(m.zk, GetKey(addr, CHUNKSERVER), &cs)
		if !ok {
			UnLock(m.zk, GetKey(addr, CHUNKSERVERLOCK))
			continue
		}
		if cs.lastHeartbeat.Add(gfs.ServerTimeout).Before(now) { // dead server
			gfs.DebugMsgToFile(fmt.Sprintf("server check remove dead server <%s>", addr), gfs.MASTER, m.address)
			RemoveInMap(m.zk, GetKey(addr, CHUNKSERVER), CHUNKSERVER)
			for h, v := range cs.chunks {
				if v { // remove from chunk location
					Lock(m.zk, GetKey(fmt.Sprintf("%d", h), CHUNKMETADATALOCK))
					var ck ChunkMetadata
					ok = Get(m.zk, GetKey(fmt.Sprintf("%d", h), CHUNKMETADATA), &ck)
					if !ok {
						gfs.DebugMsgToFile(fmt.Sprintf("chunk <%v> doesn't exist", h), gfs.MASTER, m.address)
						UnLock(m.zk, GetKey(fmt.Sprintf("%d", h), CHUNKMETADATALOCK))
						continue
					}

					var newLocation []string
					for _, l := range ck.location {
						if l != addr {
							newLocation = append(newLocation, l)
						}
					}
					ck.location = newLocation
					// update primary
					if ck.primary == addr {
						ck.primary = ""
					}
					ck.expire = time.Now()
					SetInMap(m.zk, GetKey(fmt.Sprintf("%d", h), CHUNKMETADATA), ck, CHUNKMETADATA)

					// add chunk to replicasNeedList if replica is not enough
					if len(ck.location) < gfs.MinimumNumReplicas {
						Lock(m.zk, ReplicaNeedListLock)
						var needList []string
						Get(m.zk, ReplicaNeedList, &needList)
						needList = append(needList, fmt.Sprintf("%d", h))
						Set(m.zk, ReplicaNeedList, needList)
						UnLock(m.zk, ReplicaNeedListLock)
						if len(ck.location) == 0 {
							gfs.DebugMsgToFile(fmt.Sprintf("chunk <%d> has no replica", h), gfs.MASTER, m.address)
						}
					}
					UnLock(m.zk, GetKey(fmt.Sprintf("%d", h), CHUNKMETADATALOCK))
				}
			}
		}
		UnLock(m.zk, GetKey(addr, CHUNKSERVERLOCK))
	}

	// add replicas
	err = m.reReplicationAll()
	return err
}

// reReplicationAll adds replicas for all chunks to be replicated
func (m *Master) reReplicationAll() error {
	gfs.DebugMsgToFile("reReplicationAll start", gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile("reReplicationAll end", gfs.MASTER, m.address)
	Lock(m.zk, ReplicaNeedListLock)
	var oldNeedList []string
	Get(m.zk, ReplicaNeedList, &oldNeedList)
	Set(m.zk, ReplicaNeedList, make([]string, 0))
	UnLock(m.zk, ReplicaNeedListLock)

	var newNeedList []string
	for _, h := range oldNeedList {
		// lock chunk so master will not grant lease during copy time
		Lock(m.zk, GetKey(h, CHUNKMETADATALOCK))
		var ck ChunkMetadata
		ok := Get(m.zk, GetKey(h, CHUNKMETADATA), &ck)
		if !ok {
			gfs.DebugMsgToFile(fmt.Sprintf("chunk <%s> doesn't exist", h), gfs.MASTER, m.address)
			UnLock(m.zk, GetKey(h, CHUNKMETADATALOCK))
			continue
		}

		if len(ck.location) < gfs.MinimumNumReplicas {
			if ck.expire.Before(time.Now()) {
				err := m.reReplicationOne(h, &ck)
				if err != nil {
					gfs.DebugMsgToFile(fmt.Sprintf("reReplication error <%s>", err), gfs.MASTER, m.address)
					newNeedList = append(newNeedList, h)
				}
			} else {
				newNeedList = append(newNeedList, h)
			}
		}
		UnLock(m.zk, GetKey(h, CHUNKMETADATALOCK))
	}

	Lock(m.zk, ReplicaNeedListLock)
	var needList []string
	Get(m.zk, ReplicaNeedList, &needList)
	needList = append(needList, newNeedList...)
	Set(m.zk, ReplicaNeedList, needList)
	UnLock(m.zk, ReplicaNeedListLock)
	return nil
}

// reReplicationOne adds replica for one chunk, chunk meta should be locked in top caller
func (m *Master) reReplicationOne(handle string, chunk *ChunkMetadata) error {
	gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%s> start", handle), gfs.MASTER, m.address)
	defer gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%s> end", handle), gfs.MASTER, m.address)
	// holding corresponding chunk metadata lock now
	h, _:= strconv.ParseInt(handle, 10, 64)
	from, to, err := m.chooseReReplication(h)
	if err != nil {
		return err
	}

	var cr gfs.CreateChunkReply
	err = gfs.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: h}, &cr)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%s> error <%s>", handle, err), gfs.MASTER, m.address)
		return err
	}

	var sr gfs.SendCopyReply
	err = gfs.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: h, Address: to}, &sr)
	if err != nil {
		gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%s> error <%s>", handle, err), gfs.MASTER, m.address)
		return err
	}

	// add handle in chunk server info of to
	Lock(m.zk, GetKey(to, CHUNKSERVERLOCK))
	var cs ChunkServerInfo
	ok := Get(m.zk, GetKey(to, CHUNKSERVER), &cs)
	if !ok {
		UnLock(m.zk, GetKey(to, CHUNKSERVERLOCK))
		err = fmt.Errorf("add chunk in removed server %s", to)
		gfs.DebugMsgToFile(fmt.Sprintf("reReplicationOne handle <%s> error <%s>", handle, err), gfs.MASTER, m.address)
		return err
	}
	cs.chunks[h] = true
	SetInMap(m.zk, GetKey(to, CHUNKSERVER), cs, CHUNKSERVER)
	UnLock(m.zk, GetKey(to, CHUNKSERVERLOCK))

	// add to location of chunk
	chunk.location = append(chunk.location, to)
	SetInMap(m.zk, GetKey(handle, CHUNKMETADATA), *chunk, CHUNKMETADATA)
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
	var chunkServerList []string
	Get(m.zk, ChunkServerKeyList, &chunkServerList)
	for _, addr := range chunkServerList {
		var cs ChunkServerInfo
		ok := Get(m.zk, GetKey(addr, CHUNKSERVER), &cs)
		if ok {
			if cs.chunks[handle] {
				from = addr
			} else {
				to = addr
			}
		}
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
