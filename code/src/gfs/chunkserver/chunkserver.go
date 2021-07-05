package chunkserver

import (
	"fmt"
	"log"
	"os"
	"path"
	"sync"

	"../../gfs"
)

type ChunkServer struct {
	lock    sync.RWMutex
	id      string
	rootDir string
	chunk   map[int64]*ChunkInfo

	db *downloadBuffer
}

type ChunkInfo struct {
	lock      sync.RWMutex
	length    int64
	version   int64
	checksum  map[int64]int64
	mutations map[int64]*Mutation
	invalid   bool
}

type Mutation struct {
	Data   []byte
	Offset int64
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
