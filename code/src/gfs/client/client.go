package client

import (
	"../../gfs"
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
func (c *Client) Write(path string, offset int64, data []byte) error {
	// TODO
	return nil
}

// Append is a client API, append data to file
func (c *Client) Append(path string, data []byte) (offset int64, err error) {
	// TODO
	return 0, nil
}