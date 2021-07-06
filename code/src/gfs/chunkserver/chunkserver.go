package chunkserver

import (
	"../../gfs"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
)

type void struct {}

// ChunkServer struct
type ChunkServer struct {
	lock     sync.RWMutex
	id       string		// chunk server id
	master   string		// master address
	rootDir  string		// path to data storage
	l        net.Listener
	shutdown chan struct {}

	chunk    map[int64]*ChunkInfo	// chunk information
	dead     bool	// set to true if server is shutdown
	garbage  []int64	// garbage
	leaseSet map[int64]void		// leases to be extended? I guess...
	db       *downloadBuffer	// expiring download buffer??? fine I have no idea about it...
}

type ChunkInfo struct {
	lock      sync.RWMutex
	length    int64
	version   int64		// version number of the chunk in disk
	checksum  map[int64]int64	// why it is a map?
	mutations map[int64]*Mutation	// mutation buffer
	invalid   bool		// unrecoverable error
}

type Mutation struct {
	Data   []byte
	Offset int64
}

type metadata struct{
	chunkHandle int64
	length      int64
	version     int64
	checksum    int64
}

func newChunkserver(id string, master string, rootDir string) *ChunkServer{
	cs := &ChunkServer{
		id:       id,
		shutdown: make(chan struct{}),
		master:   master,
		rootDir:  rootDir,
		chunk:    make(map[int64]*ChunkInfo),
		leaseSet: make(map[int64]void),
		db:       newDataBuffer(time.Minute, 30*time.Second),
	}
	rpcs:=rpc.NewServer()
	rpcs.Register(cs)
	l,e:=net.Listen("tcp",string(cs.id))
	if e!=nil {
		fmt.Println("[chunkserver]listen error:",e)
	}
	cs.l=l

	_,err:=os.Stat(rootDir)
	if err!=nil {
		err = os.Mkdir(rootDir,0644)
		if err!=nil{
			fmt.Println("[chunkserver]mkdir error:",err)
		}
	}
	err=cs.loadmeta()
	if err!=nil{
		fmt.Println("[chunkserver]loadmeta error:",err)
	}
	go func(){
		for{
			select{
				case <-cs.shutdown:
					return
				default:
			}
			conn,err:=cs.l.Accept()
			if err!=nil{
				if cs.dead==false {
					fmt.Println("[chunkserver]connect error:",err)
				}
			}else{
				go func(){
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			}
		}
	}()
	go func(){
		heartbeat:=time.Tick(100*time.Millisecond)
		storemeta:=time.Tick(30*time.Minute)
		garbagecollection:=time.Tick(2*time.Hour)
		for{
			select{
			case <-cs.shutdown:
				return
			case <-heartbeat:
			{
				err:=cs.heartbeat()
				if err!=nil{
					fmt.Println("[chunkserver]heartbeat error:",err)
				}
			}
			case <-storemeta:
			{
				err:=cs.storemeta()
				if err!=nil{
					fmt.Println("[chunkserver]storemeta error:",err)
				}
			}
			case <-garbagecollection:
			{
				err:=cs.garbagecollection()
				if err!=nil{
					fmt.Println("[chunkserver]garbage collection error:",err)
				}
			}
			}
		}
	}()
	return cs
}

func(cs* ChunkServer) Shutdown() {
	if cs.dead==false {
		cs.dead=true
		close(cs.shutdown)
		cs.l.Close()
	}
	err:=cs.storemeta()
	if err!=nil {
		fmt.Println("[chunkserver]store metadata error:",err)
	}
}

func(cs *ChunkServer)loadmeta() error{
	cs.lock.Lock()
	defer cs.lock.Unlock()//?
	filename:=path.Join(cs.rootDir,"chunkserver.meta")//join pathnames together
	file,err:=os.OpenFile(filename,os.O_RDONLY,0644)
	if err!=nil {
		fmt.Println("[chunkserver]open file error:",err)
		return err
	}
	defer file.Close()
	var metadatas []metadata
	dc:=gob.NewDecoder(file)
	err=dc.Decode(&metadatas)
	if err!=nil{
		fmt.Println("[chunkserver]decode file error:",err)
	}
	for _,chunkmeta:=range metadatas{
		cs.chunk[chunkmeta.chunkHandle]=&ChunkInfo{
			length:chunkmeta.length,
			version:chunkmeta.version,
		}
	}
	return nil
}

func (cs*ChunkServer) storemeta()error{
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	filename:=path.Join(cs.rootDir,"chunkserver.meta")
	file,err:=os.OpenFile(filename,os.O_WRONLY|os.O_CREATE,0644)
	if err!=nil{
		fmt.Println("[chunkserver]open file error:",err)
	}
	defer file.Close()
	var metadatas []metadata
	for handle,chunk:=range cs.chunk{
		metadatas=append(metadatas,metadata{
			chunkHandle: handle,
			length:      chunk.length,
			version:     chunk.version,
		})
	}
	enc:=gob.NewEncoder(file)
	err=enc.Encode(metadatas)
	return err
}

func (cs*ChunkServer) garbagecollection()error{
	for _,v:=range cs.garbage {
		err:=cs.deleteChunk(v)
		if err!=nil{
			return err
		}
	}
	cs.garbage=make([]int64,0)
	return nil
}
// heartbeat calls master regularly to report chunk server's status
func (cs*ChunkServer) heartbeat() error {
	le := make([]int64, len(cs.leaseSet))
	for v := range cs.leaseSet {
		le = append(le,v)
	}
	args := &gfs.HeartbeatArg{
		Address:	cs.id,
		ToExtendLeases:	le,
	}
	var reply gfs.HeartbeatReply
	err := gfs.Call(cs.master, "Master.RPCHeartbeat", args, &reply)
	if err != nil {
		return err
	}
	cs.garbage = append(cs.garbage, reply.Garbage...)
	return nil
}

func (cs*ChunkServer)deleteChunk(chunkhandle int64)error{
	cs.lock.Lock()
	delete(cs.chunk,chunkhandle)
	cs.lock.Unlock()
	filename:=path.Join(cs.rootDir,fmt.Sprintf("chunk%v.chk",chunkhandle))
	return os.Remove(filename)
}

func (cs*ChunkServer) RPCCheckVersion(args gfs.CheckVersionArg,reply *gfs.CheckVersionReply) error{
	cs.lock.RLock()
	chunk,ok:=cs.chunk[args.Handle]
	cs.lock.RUnlock()
	if ok==false {
		return fmt.Errorf("[chunkserver]chunk does not exist")
	}
	if chunk.invalid {
		return fmt.Errorf("[chunkserver]chunk is abandoned")
	}
	chunk.lock.Lock()
	defer chunk.lock.Unlock()
	if chunk.version+1==args.Version {
		chunk.version++
		reply.Stale=false
	}else{
		chunk.invalid=true
		reply.Stale=true
	}
	return nil
}

func (cs*ChunkServer) RPCForwardData(args gfs.ForwardDataArg,reply *gfs.ForwardDataReply)error {
	_,ok:=cs.db.Get(args.DataID)
	if ok!=nil {
		return fmt.Errorf("[chunkserver]data %v already exists",args.DataID)
	}
	cs.db.Set(args.DataID,args.Data)
	if len(args.ChainOrder)>0 {
		next:=args.ChainOrder[0]
		args.ChainOrder=args.ChainOrder[1:]
		rpcc,err:=rpc.Dial("tcp",next)
		if err!=nil{
			fmt.Println("[chunkserver]forwarddata rpc call error:",err)
			return err
		}
		err=rpcc.Call("ChunkServer.RPCForwardData",args,reply)
		if err!=nil{
			fmt.Println("[chunkserver]forwarddata rpc call error:",err)
			return err
		}
	}
	return nil
}

func (cs*ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply)error {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	_,ok:=cs.chunk[args.Handle]
	if ok {
		return fmt.Errorf("[chunkserver]create chunk error")
	}
	cs.chunk[args.Handle]=&ChunkInfo{
		length:0,
	}
	filename:=path.Join(cs.rootDir,fmt.Sprintf("chunk%v.chk",args.Handle))
	_,err:=os.OpenFile(filename,os.O_WRONLY|os.O_CREATE,0644)
	return err
}

func (cs*ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply*gfs.ReadChunkReply)error {
	cs.lock.RLock()
	chunk,ok:=cs.chunk[args.Handle]
	cs.lock.RUnlock()
	if ok==false {
		return fmt.Errorf("[chunkserver]chunk%v doesn't exist",args.Handle)
	}
	if chunk.invalid {
		return fmt.Errorf("[chunkserver]chunk%v is abandoned",args.Handle)
	}
	reply.Data=make([]byte,args.Length)
	var err error
	chunk.lock.RLock()
	err=cs.readChunk(args.Handle,args.Offset,reply.Data,&reply.Length)
	chunk.lock.RUnlock()
	if err==io.EOF {
		reply.ErrorCode=500
		return nil
	}
	return err
}

func (cs*ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply)error {
	cs.lock.RLock()
	chunk,ok:=cs.chunk[args.Handle]
	cs.lock.RUnlock()
	if ok==false {
		return fmt.Errorf("[chunkserver]chunk%v doesn't exist",args.Handle)
	}
	if chunk.invalid {
		return fmt.Errorf("[chunkserver]chunk%v is abandoned",args.Handle)
	}
	chunk.lock.RLock()
	defer chunk.lock.RUnlock()
	args1:=&gfs.ApplyCopyArg{
		Handle:args.Handle,
		Version:chunk.version,
		Data:make([]byte,chunk.length),
	}
	var length int
	err:=cs.readChunk(args.Handle,0,args1.Data,&length)
	if err!=nil {
		fmt.Println("[chunkserver]readchunk error",err)
		return err
	}
	rpcc,err:=rpc.Dial("tcp",args.Address)
	if err!=nil{
		fmt.Println("[chunkserver]applycopy rpc call error:",err)
		return err
	}
	var r gfs.ApplyCopyReply
	err=rpcc.Call("ChunkServer.RPCApplyCopy",args1,&r)
	if err!=nil{
		fmt.Println("[chunkserver]applycopy rpc call error:",err)
		return err
	}
	return nil
}

func (cs *ChunkServer) readChunk(handle int64, offset int64, data []byte, length *int) error{
	filename:=path.Join(cs.rootDir,fmt.Sprintf("chunk%v.chk",handle))
	file,err:=os.Open(filename)
	if err!=nil {
		*length = -1
		return err
	}
	defer file.Close()
	*length,err = file.ReadAt(data,offset)
	return err
}

func (cs *ChunkServer) write(id int64, data []byte, offset int64) error {
	cs.lock.RLock()
	ck := cs.chunk[id]
	cs.lock.RUnlock()

	len := offset + int64(len(data))
	if len > gfs.MaxChunkSize {
		log.Fatal("Maximum chunksize exceeded!")
	}

	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", id))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(data, offset)
	if err != nil {
		return err
	}

	if len > ck.length {
		ck.length = len
	}

	return nil
}

func (cs *ChunkServer) sync(id int64, m *Mutation) error {
	err := cs.write(id, m.Data, m.Offset)

	if err != nil {
		cs.lock.RLock()
		ck := cs.chunk[id]
		cs.lock.RUnlock()
		ck.invalid = true
		return err
	}

	return nil

}

func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	data, err := cs.db.Fetch(args.DbID)
	if err != nil {
		return err
	}

	newLength := args.Offset + int64(len(data))
	if newLength > gfs.MaxChunkSize {
		return fmt.Errorf("Maximum chunk size exceeded!")
	}

	handle := args.DbID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.invalid {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	ck.lock.Lock()
	defer ck.lock.Unlock()
	mutation := &Mutation{data, args.Offset}

	wait := make(chan error, 1)
	go func() {
		wait <- cs.sync(handle, mutation)
	}()

	applyMutationArgs := gfs.ApplyMutationArg{args.DbID, args.Offset}
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

func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	data, err := cs.db.Fetch(args.DbID)
	if err != nil {
		return err
	}

	if len(data) > gfs.MaxAppendSize {
		return fmt.Errorf("Maximum chunk append size excceeded!")
	}

	handle := args.DbID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.invalid {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	ck.lock.Lock()
	defer ck.lock.Unlock()
	newLen := ck.length + int64(len(data))
	offset := ck.length
	if newLen > gfs.MaxChunkSize {
		ck.length = gfs.MaxChunkSize
		data = []byte{0}
		offset = gfs.MaxChunkSize - 1
		reply.ErrorCode = 400
	} else {
		ck.length = newLen
	}
	reply.Offset = offset

	mutation := &Mutation{data, offset}
	// apply to local
	wait := make(chan error, 1)
	go func() {
		wait <- cs.sync(handle, mutation)
	}()
	// call secondaries
	applyMutationArgs := gfs.ApplyMutationArg{args.DbID, offset}
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

func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.invalid {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	ck.lock.Lock()
	defer ck.lock.Unlock()

	ck.version = args.Version
	err := cs.write(handle, args.Data, 0)
	if err != nil {
		return err
	}
	return nil
}

func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	data, err := cs.db.Fetch(args.DbID)
	if err != nil {
		return err
	}

	handle := args.DbID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.invalid {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	mutation := &Mutation{data, args.Offset}

	ck.lock.Lock()
	defer ck.lock.Unlock()
	err = cs.sync(handle, mutation)
	return err
}