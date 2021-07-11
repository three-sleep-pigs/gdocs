package client

import (
	"../../gfs"
	"../chunkserver"
	"fmt"
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
	// TODO
	return nil
}

// Delete is a client API, deletes a file
func (c *Client) Delete(path string) error {
	// TODO
	return nil
}

// Rename is a client API, deletes a file
func (c *Client) Rename(source string, target string) error {
	// TODO
	return nil
}

// Mkdir is a client API, makes a directory
func (c *Client) Mkdir(path string) error {
	// TODO
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

// Read is a client API, read file at specific offset
// it reads up to len(data) bytes form the File. it return the number of bytes and an error.
// the error is set to io.EOF if stream meets the end of file
func (c *Client) Read(path string, offset int64, data []byte) (n int, err error) {
	// TODO
	return 0, nil
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