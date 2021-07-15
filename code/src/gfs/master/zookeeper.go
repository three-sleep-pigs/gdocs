package master

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"strconv"
	"time"
)

const (
	CHUNKMETADATALOCK = iota
	CHUNKSERVERLOCK
	FILEMETADATALOCK
	CHUNKMETADATA
	CHUNKSERVER
	FILEMETADATA
)

const (
	LockPrefix = "Lock"
	ChunkMetaDataPrefix = "ChunkMeta"
	FileMetaDataPrefix = "FileMeta"
	ChunkServerInfoPrefix = "ChunkServerInfo"
	ReplicaNeedList = "ReplicaNeedList"
	ReplicaNeedListLock = "LockReplicaNeedList"
	NextHandle = "NextHandle"
)

func GetKey(Key string, Type int) string {
	switch Type {
	case CHUNKSERVERLOCK:
		return LockPrefix + ChunkServerInfoPrefix + Key
	case CHUNKSERVER:
		return ChunkServerInfoPrefix + Key
	case CHUNKMETADATALOCK:
		return LockPrefix + ChunkMetaDataPrefix + Key
	case CHUNKMETADATA:
		return ChunkMetaDataPrefix + Key
	case FILEMETADATALOCK:
		return LockPrefix + FileMetaDataPrefix + Key
	case FILEMETADATA:
		return FileMetaDataPrefix + Key
	default:
		return Key
	}
}

func Connect() (*zk.Conn, error) {
	hosts := []string{"127.0.0.1:2181"}
	conn, _, err := zk.Connect(hosts, time.Second*5)
	if err != nil {
		fmt.Println("[zookeeper]connect error:",err)
		return nil, err
	}
	return conn, nil
}

func Lock(conn *zk.Conn, toLock string) error {
	for {
		data, sate, err := conn.Get(toLock)
		if err != nil {
			return err
		}
		if string(data) == "0" {
			_, e := conn.Set(toLock, []byte("1"), sate.Version)
			if e == nil {
				return nil
			}
		}
	}
}

func UnLock(conn *zk.Conn, toUnLock string) error {
	data, sate, err := conn.Get(toUnLock)
	if err != nil {
		return err
	}
	if string(data) == "1" {
		_, e := conn.Set(toUnLock, []byte("0"), sate.Version)
		if e == nil {
			return nil
		} else {
			return e
		}
	} else if string(data) == "0" {
		return nil
	}
	return fmt.Errorf("[zookeeper]unlock fail")
}

// CreateAndLock create a lock and lock it
func CreateAndLock(conn *zk.Conn, toCreate string) error {
	acls := zk.WorldACL(zk.PermAll)
	_, err := conn.Create(toCreate, []byte("1"), 0, acls)
	if err != nil {
		return Lock(conn, toCreate)
	}
	return nil
}

func GetHandle(conn *zk.Conn) (int64, error) {
	for {
		data, sate, err := conn.Get(NextHandle)
		if err != nil {
			return -1, err
		}
		h, e := strconv.ParseInt(string(data), 10, 64)
		if e != nil {
			return -1, e
		}
		_, er := conn.Set(NextHandle, []byte(fmt.Sprintf("%d", h + 1)), sate.Version)
		if er == nil {
			return h, nil
		}
	}
}

func SetIfAbsent(conn *zk.Conn, key string, value interface{}) bool {
	acls := zk.WorldACL(zk.PermAll)
	data, _ := json.Marshal(value)
	_, err := conn.Create(key, data, 0, acls)
	if err != nil {
		return false
	}
	return true
}

func Get(conn *zk.Conn, key string, value interface{}) bool {
	data, _, err := conn.Get(key)
	if err != nil {
		return false
	}
	err = json.Unmarshal(data, value)
	if err != nil {
		return false
	}
	return true
}

// Set will create a new tuple if there is no related tuple
// Set will set the related tuple to new value if it exists
func Set(conn *zk.Conn, key string, value interface{}) error {
	acls := zk.WorldACL(zk.PermAll)
	data, _ := json.Marshal(value)
	_, err := conn.Create(key, data, 0, acls)
	if err == nil {
		return nil
	}
	for {
		_, sate, e := conn.Get(key)
		if e != nil {
			return e
		}
		_, er := conn.Set(key, data, sate.Version)
		if er == nil {
			return nil
		}
	}
}

func Remove(conn *zk.Conn, key string) {
	for {
		_, sate, e := conn.Get(key)
		if e != nil {
			return
		}
		err := conn.Delete(key, sate.Version)
		if err == nil {
			return
		}
	}
}