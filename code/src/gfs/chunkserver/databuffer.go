package chunkserver

import (
	"../../gfs"
	"fmt"
	"sync"
	"time"
)

type downloadItem struct {
	data   []byte
	expire time.Time
}

type downloadBuffer struct {
	sync.RWMutex
	buffer map[gfs.DataBufferID]downloadItem
	expire time.Duration
	tick   time.Duration
}

// newDownloadBuffer returns a downloadBuffer. Default expire time is expire.
// The downloadBuffer will cleanup expired items every tick.
func newDataBuffer(expire, tick time.Duration) *downloadBuffer {
	buf := &downloadBuffer{
		buffer: make(map[gfs.DataBufferID]downloadItem),
		expire: expire,
		tick:   tick,
	}

	// cleanup
	go func() {
		ticker := time.Tick(tick)
		for {
			<-ticker
			now := time.Now()
			buf.Lock()
			for id, item := range buf.buffer {
				if item.expire.Before(now) {
					delete(buf.buffer, id)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

// NewDataID allocate a new DataID for given handle
func NewDataID(handle int64) gfs.DataBufferID {
	now := time.Now()
	timeStamp := int64(now.Nanosecond()) + int64(now.Second())*1000000000 + int64(now.Minute())*60*1000000000
	return gfs.DataBufferID{Handle: handle, Time: timeStamp}
}

func (buf *downloadBuffer) Set(id gfs.DataBufferID, data []byte) {
	buf.Lock()
	defer buf.Unlock()
	buf.buffer[id] = downloadItem{data, time.Now().Add(buf.expire)}
}

func (buf *downloadBuffer) SetIfAbsent(id gfs.DataBufferID, data []byte) bool {
	buf.Lock()
	defer buf.Unlock()
	_, ok := buf.buffer[id]
	if !ok {
		buf.buffer[id] = downloadItem{data, time.Now().Add(buf.expire)}
	}
	return !ok
}

func (buf *downloadBuffer) Get(id gfs.DataBufferID) ([]byte, error) {
	buf.Lock()
	defer buf.Unlock()
	item, ok := buf.buffer[id]
	if !ok {
		return nil, fmt.Errorf("dataID %v not found in download buffer", id)
	}
	item.expire = time.Now().Add(buf.expire) // touch
	return item.data, nil
}

func (buf *downloadBuffer) Fetch(id gfs.DataBufferID) ([]byte, error) {
	buf.Lock()
	defer buf.Unlock()

	item, ok := buf.buffer[id]
	if !ok {
		return nil, fmt.Errorf("dataID %v not found in download buffer", id)
	}

	delete(buf.buffer, id)
	return item.data, nil
}

func (buf *downloadBuffer) Delete(id gfs.DataBufferID) {
	buf.Lock()
	defer buf.Unlock()
	delete(buf.buffer, id)
}
