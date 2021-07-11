package client

import (
	"../../gfs"
	"fmt"
	"io"
	"math/rand"
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
func (c *Client) Write(path string, offset int64, data []byte) error {
	// TODO
	return nil
}

// Append is a client API, append data to file
func (c *Client) Append(path string, data []byte) (offset int64, err error) {
	// TODO
	return 0, nil
}