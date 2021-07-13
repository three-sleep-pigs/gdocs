package chunkserver

import (
	"../../gfs"
	"../cmap"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

type void struct{}


// ChunkServer struct
type ChunkServer struct {
	lock     sync.RWMutex // protect chunk server info
	metaFileLock sync.Mutex // protect chunk metadata file
	id       string // chunk server id
	master   string // master address
	rootDir  string // path to data storage
	l        net.Listener
	shutdown chan struct{}

	chunks    cmap.ConcurrentMap  // map[int64]*ChunkInfo
	dead     bool                 // set to true if server is shutdown
	garbage  []int64              // handles of garbage chunks to be deleted
	leaseSet map[int64]void       // leases to be extended? I guess...
	db       *downloadBuffer      // expiring download buffer??? fine I have no idea about it...
}

type ChunkInfo struct {
	lock      sync.RWMutex	      // protect corresponding file and chunk metadata
	length    int64
	version   int64               // version number of the chunk in disk
	checksum  int64
	mutations map[int64]*Mutation // mutation buffer
	invalid   bool                // unrecoverable error
}

type Mutation struct {
	Data   []byte
	Offset int64
}

type metadata struct {
	chunkHandle int64
	length      int64
	version     int64
	checksum    int64
}

// NewChunkServer create a new chunk server and return a pointer to it
func NewChunkServer(id string, master string, rootDir string) *ChunkServer {
	cs := &ChunkServer{
		id:       id,
		shutdown: make(chan struct{}),
		master:   master,
		rootDir:  rootDir,
		chunks:    cmap.New(),
		leaseSet: make(map[int64]void),
		db:       newDataBuffer(time.Minute, 30*time.Second),
	}

	// initial chunk metadata
	_, err := os.Stat(rootDir) //check whether rootDir exists, if not, mkdir it
	if err != nil {
		err = os.Mkdir(rootDir, 0644)
		if err != nil {
			fmt.Println("[chunkServer]mkdir error:", err)
			return nil
		}
	}
	err = cs.loadMeta()
	if err != nil {
		fmt.Println("[chunkServer]loadMeta error:", err)
	}

	// register rpc server
	rpcs := rpc.NewServer()
	err = rpcs.Register(cs)
	if err != nil {
		fmt.Println("[chunkServer]rpc server register error:", err)
		return nil
	}
	l, e := net.Listen("tcp", string(cs.id))
	if e != nil {
		fmt.Println("[chunkServer]listen error:", e)
		return nil
	}
	cs.l = l

	// background coroutine loops receiving rpc calls, return immediately when the chunkServer shutdown
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err != nil {
				if cs.dead == false {
					fmt.Println("[chunkServer]connect error:", err)
				}
			} else {
				go func() {
					rpcs.ServeConn(conn)
					err := conn.Close()
					if err != nil {
						fmt.Println("[chunkServer]connect close error:", err)
					}
				}()
			}
		}
	}()

	// background coroutine heartbeat, store metadata, collect garbage regularly
	go func() {
		heartbeat := time.Tick(gfs.HeartbeatInterval)
		storeMeta := time.Tick(gfs.StoreMetaInterval)
		garbageCollection := time.Tick(gfs.GarbageCollectionInterval)
		for {
			select {
			case <-cs.shutdown:
				return
			case <-heartbeat:
				{
					if cs.dead {     // check if shutdown
						return
					}
					err := cs.heartbeat()
					if err != nil {
						fmt.Println("[chunkServer]heartbeat error:", err)
					}
				}
			case <-storeMeta:
				{
					if cs.dead {     // check if shutdown
						return
					}
					err := cs.storeMeta()
					if err != nil {
						fmt.Println("[chunkServer]storeMeta error:", err)
					}
				}
			case <-garbageCollection:
				{
					if cs.dead {     // check if shutdown
						return
					}
					err := cs.garbageCollection()
					if err != nil {
						fmt.Println("[chunkServer]garbageCollection error:", err)
					}
				}
			}
		}
	}()
	return cs
}

// Shutdown called when close the chunk
// FIXME: Shutdown shouldn't be called concurrently because TOCTTOU of cs.dead
// no need to fix it
func (cs *ChunkServer) Shutdown() {
	if cs.dead == false {
		cs.dead = true
		close(cs.shutdown)
		err := cs.l.Close()
		if err != nil {
			fmt.Println("[chunkServer]close listener error:", err)
		}
		err = cs.storeMeta()
		if err != nil {
			fmt.Println("[chunkServer]store metadata error:", err)
		}
	}
}

// loadMeta loads metadata of chunks from disk into chunk map, called by newChunkServer
func (cs *ChunkServer) loadMeta() error {
	filename := path.Join(cs.rootDir, "chunkServer.meta")
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	//decode chunk metadata from file into slice
	var chunkMeta []metadata
	dec := gob.NewDecoder(file)
	err = dec.Decode(&chunkMeta)
	if err != nil {
		return err
	}

	for _, c := range chunkMeta {
		e := cs.chunks.SetIfAbsent(fmt.Sprintf("%d", c.chunkHandle), &ChunkInfo{
			length:  c.length,
			version: c.version,
			checksum: c.checksum,
		})
		if !e {
			fmt.Println("[chunkServer]set chunk metadata exist")
		}
	}
	return nil
}

// storeMeta stores metadata of chunks into disk
func (cs *ChunkServer) storeMeta() error {
	cs.metaFileLock.Lock() // prevent storeMeta from being called concurrently
	defer cs.metaFileLock.Unlock()

	filename := path.Join(cs.rootDir, "chunkServer.meta")
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// construct metadata slice according to chunks map
	var chunkMeta []metadata
	for tuple := range cs.chunks.IterBuffered() {
		h, e := strconv.ParseInt(tuple.Key, 10, 64)
		if e != nil {
			fmt.Println("[chunkServer]parse chunk handle error:", e)
			continue
		}
		c := tuple.Val.(*ChunkInfo)
		c.lock.RLock()
		chunkMeta = append(chunkMeta, metadata{
			chunkHandle: h,
			length:      c.length,
			version:     c.version,
			checksum:    c.checksum,
		})
		c.lock.RUnlock()
	}

	enc := gob.NewEncoder(file)
	err = enc.Encode(chunkMeta)
	return err
}

// garbageCollection deletes chunks in cs.garbage and empty cs.garbage
func (cs *ChunkServer) garbageCollection() error {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	for _, h := range cs.garbage {
		err := cs.deleteChunk(h)
		if err != nil {
			fmt.Println("[chunkServer]delete chunk error:", err)
		}
	}
	cs.garbage = make([]int64, 0)
	return nil
}

// heartbeat calls master regularly to report chunk server's status
func (cs *ChunkServer) heartbeat() error {
	// TODO: add leases to extend

	// make lease slice from cs.leaseSet
	var le []int64
	cs.lock.RLock()
	for v := range cs.leaseSet {
		le = append(le, v)
	}
	cs.lock.RUnlock()

	// call master
	args := &gfs.HeartbeatArg{
		Address:        cs.id,
		ToExtendLeases: le,
	}
	var reply gfs.HeartbeatReply
	err := gfs.Call(cs.master, "Master.RPCHeartbeat", args, &reply)
	if err != nil {
		return err
	}

	// append garbage
	cs.lock.Lock()
	cs.garbage = append(cs.garbage, reply.Garbage...)
	cs.lock.Unlock()
	return nil
}

// deleteChunk deletes a chunk, called by garbageCollection
func (cs *ChunkServer) deleteChunk(handle int64) error {
	v, e := cs.chunks.Pop(fmt.Sprintf("%d", handle))
	if !e {
		return fmt.Errorf("[chunkServer]delete chunk not exist %d", handle)
	}
	c := v.(*ChunkInfo)
	c.lock.Lock()
	defer c.lock.Unlock()
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	err := os.Remove(filename)
	return err
}

// RPCCheckVersion rpc called by master checking whether a chunk is stale
func (cs *ChunkServer) RPCCheckVersion(args gfs.CheckVersionArg, reply *gfs.CheckVersionReply) error {
	v, e := cs.chunks.Get(fmt.Sprintf("%d", args.Handle))
	if !e {
		return fmt.Errorf("[chunkServer]chunk does not exist")
	}
	chunk := v.(*ChunkInfo)
	chunk.lock.Lock()
	defer chunk.lock.Unlock()

	if chunk.invalid {
		return fmt.Errorf("[chunkServer]chunk is abandoned")
	}
	if chunk.version + 1 == args.Version {
		chunk.version++      // not stale: update version
		reply.Stale = false
	} else {
		chunk.invalid = true // stale: set invalid bit
		reply.Stale = true
	}
	return nil
}

// RPCForwardData rpc called by client or another chunkServer, received data is saved in downloadBuffer
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	ok := cs.db.SetIfAbsent(args.DataID, args.Data)
	if !ok {
		fmt.Printf("[chunkServer]data %v already exists\n", args.DataID)
	}

	// send data to next chunkServer(ChainOrder: a chain of chunkServer to send data to)
	if len(args.ChainOrder) > 0 {
		next := args.ChainOrder[0]
		args.ChainOrder = args.ChainOrder[1:]

		err := gfs.Call(next,"ChunkServer.RPCForwardData", args, reply)
		if err != nil {
			return err
		}
	}
	return nil
}

// RPCCreateChunk create a new chunk, save its metadata and open a new file to store its data, called by master
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	chunk := new(ChunkInfo)
	chunk.lock.Lock()
	defer chunk.lock.Unlock()
	chunk.length = 0
	ok := cs.chunks.SetIfAbsent(fmt.Sprintf("%d", args.Handle), chunk)
	if !ok {
		return fmt.Errorf("[chunkServer]create chunk error: chunk%v already exists", args.Handle)
	}
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", args.Handle))
	_, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	return err
}

// RPCReadChunk read chunk according to handle, offset and length given in args, called by client
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	v, ok := cs.chunks.Get(fmt.Sprintf("%d", args.Handle))
	if !ok {
		return fmt.Errorf("[chunkServer]chunk%v doesn't exist", args.Handle)
	}
	chunk := v.(*ChunkInfo)
	chunk.lock.RLock()
	defer chunk.lock.RUnlock()

	if chunk.invalid {
		return fmt.Errorf("[chunkServer]chunk%v is abandoned", args.Handle)
	}
	reply.Data = make([]byte, args.Length)
	err := cs.readChunk(args.Handle, args.Offset, reply.Data, &reply.Length)
	if err == io.EOF {
		reply.ErrorCode = gfs.ReadEOF
		return nil
	}
	return err
}

// RPCSendCopy called by master
// send a whole chunk to an address given in args according to chunk handle
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	// get from concurrent map
	chunkInfoFound, ok := cs.chunks.Get(fmt.Sprintf("%d", args.Handle))
	if ok == false {
		return fmt.Errorf("[chunk server] chunk%d doesn't exist", args.Handle)
	}
	chunkInfo := chunkInfoFound.(*ChunkInfo)
	// get chunkInfo lock here to protect invalid
	chunkInfo.lock.RLock()
	defer chunkInfo.lock.RUnlock()
	if chunkInfo.invalid {
		return fmt.Errorf("[chunk server] chunk%d is abandoned", args.Handle)
	}
	argsCopy := &gfs.ApplyCopyArg{
		Handle:  args.Handle,
		Version: chunkInfo.version,
		Data:    make([]byte, chunkInfo.length),
	}
	var length int
	err := cs.readChunk(args.Handle, 0, argsCopy.Data, &length)
	if err != nil {
		return fmt.Errorf("[chunk server] read chunk error %s", err)
	}
	var r gfs.ApplyCopyReply
	err = gfs.Call(args.Address,"ChunkServer.RPCApplyCopy", argsCopy, &r)
	if err != nil {
		return fmt.Errorf("[chunk server]apply copy rpc call error: %s", err)
	}
	return nil
}

// RPCApplyCopy called by main chunk server
// copy the data from the beginning to chunk handle
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	handle := args.Handle
	//get the chunk to handle
	ckF, ok := cs.chunks.Get(fmt.Sprintf("%d", handle))
	if !ok {
		return fmt.Errorf("chunk %v does not exist", handle)
	}
	ck := ckF.(*ChunkInfo)
	ck.lock.Lock()
	defer ck.lock.Unlock()
	if ck.invalid {
		return fmt.Errorf("chunk %v is invalid", handle)
	}
	//set the version
	ck.version = args.Version
	// FIXME: should file be created?
	// write chunk func doesn't create files
	//write the data at a new chunk's beginning
	err := cs.writeChunk(handle, args.Data, 0)
	if err != nil {
		return err
	}
	return nil
}

// called by RPCReadChunk, chunk.lock is locked in top caller
//read data from a chunk according to offset and length of the data slice
func (cs *ChunkServer) readChunk(handle int64, offset int64, data []byte, length *int) error {
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	file, err := os.Open(filename)
	if err != nil {
		*length = -1
		return err
	}
	defer file.Close()
	*length, err = file.ReadAt(data, offset)
	return err
}

//write data to a chunk according at given offset
func (cs *ChunkServer) writeChunk(handle int64, data []byte, offset int64) error {
	//determine if the size after writing is larger than MaxChunkSize,if so,return errors
	len := offset + int64(len(data))
	if len > gfs.MaxChunkSize {
		// FIXME: we have no log now
		//log.Fatal("Maximum chunk size exceeded!")
		return fmt.Errorf("maxinum chunk size exceeded")
	}

	//open the chunk file
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	// Consider writeChunk and deleteChunk happen concurrently, writeChunk shouldn't create file after os.Remove
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file err %s", err)
	}
	defer file.Close()

	//write the chunk file at the offset
	_, err = file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("write file err %s", err)
	}
	return nil
}

// write data,if err,set the chunk invalid,if success,update the info
func (cs *ChunkServer) sync(handle int64, ck *ChunkInfo, m *Mutation) error {
	//write data of Mutation m at m.offset to chunk
	err := cs.writeChunk(handle, m.Data, m.Offset)

	//if write error,set the chunk invalid
	if err != nil {
		ck.invalid = true
		return err
	}

	len := m.Offset + int64(len(m.Data))
	//update the chunk length
	if len > ck.length {
		ck.length = len
	}
	//TODO: update the checksum

	return nil

}

// RPCWriteChunk called by client
// write the data at given offset of args to chunk handle
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	//get the data block from data buffer and delete the block
	data, err := cs.db.Fetch(args.DbID)
	if err != nil {
		return err
	}

	//determine if the size after writing is larger than MaxChunkSize,if so,return errors
	newLength := args.Offset + int64(len(data))
	if newLength > gfs.MaxChunkSize {
		return fmt.Errorf("maximum chunk size exceeded")
	}

	//get the chunk
	handle := args.DbID.Handle
	ckF, ok := cs.chunks.Get(fmt.Sprintf("%d", handle))
	if !ok {
		return fmt.Errorf("chunk %v does not exist", handle)
	}
	ck := ckF.(*ChunkInfo)
	ck.lock.Lock()
	defer ck.lock.Unlock()
	if ck.invalid {
		return fmt.Errorf("chunk %v is invalid", handle)
	}

	//add mutation
	mutation := &Mutation{data, args.Offset}

	//concurrent call sync at main chunk server
	wait := make(chan error, 1)
	go func() {
		wait <- cs.sync(handle, ck, mutation)
	}()

	//send apply Mutation call to all secondaries
	applyMutationArgs := gfs.ApplyMutationArg{DbID: args.DbID, Offset: args.Offset}
	err = gfs.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", applyMutationArgs)
	if err != nil {
		return err
	}

	err = <-wait
	if err != nil {
		return err
	}
	return nil
}

// RPCAppendChunk called by client
// write the data of args at the end of chunk handle
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	//get the data block from data buffer and delete the block
	data, err := cs.db.Fetch(args.DbID)
	if err != nil {
		return err
	}

	//determine if the size of append data is larger than MaxChunkSize/4,if so,return errors
	if len(data) > gfs.MaxAppendSize {
		return fmt.Errorf("maximum chunk append size excceeded")
	}

	//get the chunk
	handle := args.DbID.Handle
	ckF, ok := cs.chunks.Get(fmt.Sprintf("%d", handle))
	if !ok {
		return fmt.Errorf("chunk %v does not exist", handle)
	}
	ck := ckF.(*ChunkInfo)
	ck.lock.Lock()
	defer ck.lock.Unlock()
	if ck.invalid {
		return fmt.Errorf("chunk %v is invalid", handle)
	}

	newLen := ck.length + int64(len(data)) //size after writing
	offset := ck.length                    //write at the last of data

	//if now chunk is full, return AppendExceedChunkSize
	if offset == gfs.MaxChunkSize {
		reply.ErrorCode = gfs.AppendExceedChunkSize
		return err
	}
	//if newLen is bigger than MaxChunkSize,fill the chunk with 0, return AppendExceedChunkSize
	if newLen > gfs.MaxChunkSize {
		data = []byte{0}
		offset = gfs.MaxChunkSize - 1
		reply.ErrorCode = gfs.AppendExceedChunkSize
	}
	reply.Offset = offset

	mutation := &Mutation{data, offset}
	// apply to local
	wait := make(chan error, 1)
	go func() {
		wait <- cs.sync(handle, ck, mutation)
	}()

	// call secondaries
	applyMutationArgs := gfs.ApplyMutationArg{DbID: args.DbID, Offset: offset}
	err = gfs.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", applyMutationArgs)
	if err != nil {
		return err
	}

	err = <-wait
	if err != nil {
		return err
	}

	return nil
}

// RPCApplyMutation called by main chunk server
// write the data at the given offset to chunkhandle
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	//get data
	data, err := cs.db.Fetch(args.DbID)
	if err != nil {
		return err
	}

	//get the chunk
	handle := args.DbID.Handle
	ckF, ok := cs.chunks.Get(fmt.Sprintf("%d", handle))
	if !ok {
		return fmt.Errorf("chunk %v does not exist", handle)
	}
	ck := ckF.(*ChunkInfo)
	ck.lock.Lock()
	defer ck.lock.Unlock()
	if ck.invalid {
		return fmt.Errorf("chunk %v is invalid", handle)
	}
	mutation := &Mutation{data, args.Offset}

	//apply mutation
	err = cs.sync(handle, ck, mutation)
	return err
}
