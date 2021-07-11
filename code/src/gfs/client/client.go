package client

import (
	"../../gfs"
	"../chunkserver"
	"fmt"
	"io"
	"math/rand"
	"time"
)

// Client struct is the GFS client-side driver
type Client struct {
	master   string
	replicaBuffer *ReplicaBuffer
}

// NewClient returns a new gfs client.
func NewClient(master string) *Client {
	return &Client{
		master:   master,
		replicaBuffer: newReplicaBuffer(master, gfs.ReplicaBufferTick),
	}
}

// Create is a client API, creates a file
func (c *Client) Create(path string) error {
	var reply gfs.CreateFileReply
	err := gfs.Call(c.master, "Master.RPCCreateFile", gfs.CreateFileArg{Path: path}, &reply)
	if err != nil {
		return err
	}
	return nil
}

// Delete is a client API, deletes a file
func (c *Client) Delete(path string) error {
	var reply gfs.DeleteFileReply
	err := gfs.Call(c.master, "Master.RPCDeleteFile", gfs.DeleteFileArg{Path: path}, &reply)
	if err != nil {
		return err
	}
	return nil
}

// Rename is a client API, deletes a file
func (c *Client) Rename(source string, target string) error {
	var reply gfs.RenameFileReply
	err := gfs.Call(c.master, "Master.RPCRenameFile", gfs.RenameFileArg{Source: source, Target: target}, &reply)
	if err != nil {
		return err
	}
	return nil
}

// Mkdir is a client API, makes a directory
func (c *Client) Mkdir(path string) error {
	var reply gfs.MkdirReply
	err := gfs.Call(c.master, "Master.RPCMkdir", gfs.MkdirArg{Path: path}, &reply)
	if err != nil {
		return err
	}
	return nil
}

// getChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) getChunkHandle(path string, index int64) (int64, error) {
	var reply gfs.GetChunkHandleReply
	err := gfs.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{Path: path, Index: index}, &reply)
	if err != nil {
		return 0, err
	}
	return reply.Handle, nil
}

// readChunk read data from the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) readChunk(handle int64, offset int64, data []byte) (int64, error) {
	var readLen int64

	if gfs.MaxChunkSize - offset > int64(len(data)) {
		readLen = int64(len(data))
	} else {
		readLen = gfs.MaxChunkSize - offset
	}

	l, err := c.replicaBuffer.Get(handle)
	if err != nil {
		return -1, fmt.Errorf("get replicas error")
	}
	var loc string
	if len(l.Secondaries) == 0 {
		loc = l.Primary
	} else {
		// TODO: choose nearest chunk server
		loc = l.Secondaries[rand.Intn(len(l.Secondaries))]
	}

	var r gfs.ReadChunkReply
	r.Data = data
	err = gfs.Call(loc, "ChunkServer.RPCReadChunk", gfs.ReadChunkArg{Handle: handle, Offset: offset, Length: readLen}, &r)
	if err != nil {
		return 0, fmt.Errorf("read chunk from chunk server error")
	}
	if r.ErrorCode == gfs.ReadEOF {
		return int64(r.Length), fmt.Errorf("read EOF")
	}
	return int64(r.Length), nil
}

// Read is a client API, read file at specific offset
// it reads up to len(data) bytes form the File. it return the number of bytes and an error.
func (c *Client) Read(path string, offset int64, data []byte) (n int64, err error) {
	var f gfs.GetFileInfoReply
	err = gfs.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return -1, err
	}

	if offset/gfs.MaxChunkSize > f.Size {
		return -1, fmt.Errorf("read offset exceeds file size")
	}

	var pos int64
	pos = 0
	for pos < int64(len(data)) {
		index := offset / gfs.MaxChunkSize
		chunkOffset := offset % gfs.MaxChunkSize

		if index >= f.Size {
			err = fmt.Errorf("read EOF")
			break
		}

		var handle int64
		handle, err = c.getChunkHandle(path, index)
		if err != nil {
			return -1, err
		}

		var n int64

		for {
			n, err = c.readChunk(handle, chunkOffset, data[pos:])
			if err == nil || err == fmt.Errorf("read EOF") {
				break
			}
		}

		offset += n
		pos += n
		if err != nil {
			break
		}
	}

	if err != nil && err == fmt.Errorf("read EOF") {
		return pos, io.EOF
	} else {
		return pos, err
	}
}

// Write is a client API. write data to file at specific offset
// Size of written data will be returned
func (c *Client) Write(path string, offset int64, data []byte) (size int, error error) {
	var f gfs.GetFileInfoReply
	err := gfs.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return 0, err
	}

	if int64(offset/gfs.MaxChunkSize) > f.ChunkNum {
		return 0, fmt.Errorf("[client]write file %s offset exceeds file size", path)
	}

	begin := 0
	for {
		index := int64(offset / gfs.MaxChunkSize)
		chunkOffset := offset % gfs.MaxChunkSize

		handle, err := c.getChunkHandle(path, index)
		if err != nil {
			return begin, err
		}

		writeMax := int(gfs.MaxChunkSize - chunkOffset)
		var writeLen int
		if begin+writeMax > len(data) {
			writeLen = len(data) - begin
		} else {
			writeLen = writeMax
		}

		for {
			err = c.writeChunk(handle, chunkOffset, data[begin:begin+writeLen])
			if err == nil {
				break
			}
			fmt.Println("[client]write chunk error:", err)
		}

		offset += int64(writeLen)
		begin += writeLen

		if begin == len(data) {
			break
		}
	}

	return begin, nil
}

// writeChunk writes data to the chunk at specific offset.
func (c *Client) writeChunk(handle int64, offset int64, data []byte) error {
	if len(data)+int(offset) > gfs.MaxChunkSize {
		return fmt.Errorf("len(data)+offset = %v > max chunk size", len(data)+int(offset))
	}

	replicaInfo, err := c.replicaBuffer.Get(handle)
	if err != nil {
		return err
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(replicaInfo.Secondaries, replicaInfo.Primary)

	var d gfs.ForwardDataReply
	err = gfs.Call(chain[0], "ChunkServer.RPCForwardData", gfs.ForwardDataArg{DataID: dataID, Data: data, ChainOrder: chain[1:]}, &d)
	if err != nil {
		return err
	}

	err = gfs.Call(replicaInfo.Primary, "ChunkServer.RPCWriteChunk",
		gfs.WriteChunkArg{DbID: dataID, Offset: offset, Secondaries: replicaInfo.Secondaries}, &gfs.WriteChunkReply{})
	return err
}

// Append is a client API, append data to file
// offset of the start of data will be returned if success.
func (c *Client) Append(path string, data []byte) (offset int64, err error) {
	if len(data) > gfs.MaxAppendSize {
		return -1, fmt.Errorf("len(data) = %v > max append size", len(data))
	}

	var f gfs.GetFileInfoReply
	err = gfs.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return
	}

	start := int64(f.ChunkNum - 1)
	if start < 0 {
		start = 0
	}

	var chunkOffset int64
	for {
		var handle int64
		handle, err = c.getChunkHandle(path, start)
		if err != nil {
			return
		}

		var errCode int
		for {
			chunkOffset, err, errCode = c.appendChunk(handle, data)
			if err == nil || errCode == gfs.AppendExceedChunkSize {
				break
			}
			fmt.Println("[client]Append ", handle, " connection error, try again ", err)
			time.Sleep(50 * time.Millisecond)
		}
		if err == nil {
			break
		}

		// retry in next chunk
		start++
		fmt.Println("[client]append retry in next chunk")
	}

	offset = int64(start)*gfs.MaxChunkSize + chunkOffset
	return
}

// appendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
func (c *Client) appendChunk(handle int64, data []byte) (offset int64, error error, errCode int) {
	if len(data) > gfs.MaxAppendSize {
		return -1, fmt.Errorf("len(data) = %v > max append size", len(data)), gfs.UnknownError
	}

	replicaInfo, err := c.replicaBuffer.Get(handle)
	if err != nil {
		return -1, err, gfs.UnknownError
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(replicaInfo.Secondaries, replicaInfo.Primary)

	var d gfs.ForwardDataReply
	err = gfs.Call(chain[0], "ChunkServer.RPCForwardData", gfs.ForwardDataArg{DataID: dataID, Data: data, ChainOrder: chain[1:]}, &d)
	if err != nil {
		return -1, err, gfs.UnknownError
	}

	var a gfs.AppendChunkReply
	err = gfs.Call(replicaInfo.Primary, "ChunkServer.RPCAppendChunk",
		gfs.AppendChunkArg{DbID: dataID, Secondaries: replicaInfo.Secondaries}, &a)
	if err != nil {
		return -1, err, gfs.UnknownError
	}
	if a.ErrorCode == gfs.AppendExceedChunkSize {
		return a.Offset, fmt.Errorf("append over chunks"), a.ErrorCode
	}
	return a.Offset, nil, gfs.Success
}