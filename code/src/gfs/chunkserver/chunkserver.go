package chunkserver

import (
	"fmt"
	"sync"
	"net"
	"gfs"
	"path"
)

type ChunkServer struct {
	lock sync.RWMutex
	id string
	rootDir string
	chunk map[int64]*ChunkInfo

	db *downloadBuffer
}

type ChunkInfo struct {
	lock sync.RWMutex
	length int64
	version int64
	checksum map[int64] int64
	mutations map[int64] *Mutation
	invalid bool
}

type Mutation struct {
	data []byte
	offset int64
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

func (cs *ChunkServer) sync (id int64,m *Mutation) error {
	err := cs.write(id,m.data,m.offset)

	if err != nil {
		cs.lock.RLock()
		ck := cs.chunk[id]
		cs.lock.RUnlock()
		ck.invalid = true
		return err
	}

	return nil

}

func (cs *ChunkServer) RPCWriteChunk (args gfs.WriteChunkArg,reply *gfs.WriteCHunkReply) error {
	data,err := cs.db.Fetch(args.dbID)
	if err != nil {
		return err
	}

	newLength := args.offset + int64(len(data))
	if newLength > gfs.MaxChunkSize {
		return fmt.Errorf("Maximum chunk size exceeded!")	
	}

	handle := args.dbID.Handle
	cs.lock.Rlock()
	ck,ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.invalid {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	if err = func() error {
		ck.lock.Lock()
		defer ck.lock.Unlock()
		mutation := &Mutation(data,args.offset)

		wait := make(chan error,1)
		go func() {
			wait <- cs.sync(handle,mutation)
		}

		applyMutationArgs := gfs.ApplyMutationArgs{args.dbID,args.offset}
		err = gfs.CallAll(args.Secondaries,"ChunkServer.RPCApplyMutation", applyMutationArgs)
	
		if err != nil {
			return err
		}

		err = <-wait
		if err != nil {
			return err
		}
	}(); err != nil {
		return err
	}
	return nil
}

func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	data, err := cs.db.Fetch(args.dbID)
	if err != nil {
		return err
	}

	if len(data) > gfs.MaxAppendSize {
		return fmt.Errorf("Maximum chunk append size excceeded!")
	}

	handle := args.dbID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.invalid {
		return fmt.Errorf("Chunk %v does not exist or is abandoned", handle)
	}

	if err = func() error {
		ck.lock.Lock()
		defer ck.lock.Unlock()
		newLen := ck.length + gfs.Offset(len(data))
		offset := ck.length
		if newLen > gfs.MaxChunkSize {
			ck.length = gfs.MaxChunkSize
			data = []byte{0}
			ffset = gfs.MaxChunkSize - 1
			reply.ErrorCode = 400
		} else {
			ck.length = newLen
		}
		reply.offset = offset

		mutation := &Mutation{data, offset}
		// apply to local
		wait := make(chan error, 1)
		go func() {
			wait <- cs.sync(handle, mutation)
		}()
		// call secondaries
		applyMutationArgs := gfs.ApplyMutationArg{mtype, args.DataID, offset}
		err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", applyMutationArgs)
		if err != nil {
			return err
		}
		err = <- wait
		if err != nil {
			return err
		}
	}(); err != nil {
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

	log.Infof("Server %v : Apply copy of %v", cs.address, handle)

	ck.version = args.Version
	err := cs.writeChunk(handle, args.Data, 0, true)
	if err != nil {
		return err
	}
	log.Infof("Server %v : Apply done", cs.address)
	return nil
}

func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	data, err := cs.db.Fetch(args.dbID)
	if err != nil {
		return err
	}

	handle := args.DataID.Handle
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