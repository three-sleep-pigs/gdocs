package client

import (
	"../../gfs"
	"sync"
	"time"
)

type ReplicaBuffer struct {
	sync.RWMutex
	buffer map[int64]*gfs.ChunkReplicaInfo
	tick   time.Duration
}

// newReplicaBuffer returns a replicaBuffer.
// The downloadBuffer will cleanup expired items every tick.
func newReplicaBuffer(tick time.Duration) *ReplicaBuffer {
	buf := &ReplicaBuffer{
		buffer: make(map[int64]*gfs.ChunkReplicaInfo),
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
				if item.Expire.Before(now) || item.BufferExpire.Before(now) {
					delete(buf.buffer, id)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

func (buf *ReplicaBuffer) Get(handle int64) (*gfs.ChunkReplicaInfo, error) {
	buf.Lock()
	defer buf.Unlock()
	now := time.Now()
	info, ok := buf.buffer[handle]
	if ok && info.BufferExpire.After(now) && info.Expire.After(now) {
		return info, nil
	}
	// ask master to send one
	var l gfs.GetReplicasReply
	err := gfs.CallMaster("Master.RPCGetReplicas", gfs.GetReplicasArg{Handle: handle}, &l)
	if err != nil {
		return nil, err
	}

	info = &gfs.ChunkReplicaInfo{
		Primary: l.Primary,
		Expire: l.Expire,
		Secondaries: l.Secondaries,
		BufferExpire: time.Now().Add(gfs.ReplicaBufferExpire),
	}
	buf.buffer[handle] = info
	return info, nil
}

