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
	ChunkMetaKeyList = "ChunkMetaKey"
	FileMetaKeyList = "FileMetaKey"
	ChunkServerKeyList = "ChunkServerKey"
	ServerCheckLock = "LockServerCheck"
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

// Lock create a lock and lock it
func Lock(conn *zk.Conn, toLock string) error {
	acls := zk.WorldACL(zk.PermAll)
	_, err := conn.Create(toLock, []byte("1"), 0, acls)
	if err == nil {
		return nil
	}
	for {
		data, sate, er := conn.Get(toLock)
		if er != nil {
			return er
		}
		if string(data) == "0" {
			_, e := conn.Set(toLock, []byte("1"), sate.Version)
			if e == nil {
				return nil
			}
		}
	}
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

// SetIfAbsent will create a new tuple if there is no related tuple
func SetIfAbsent(conn *zk.Conn, key string, value interface{}) bool {
	acls := zk.WorldACL(zk.PermAll)
	data, _ := json.Marshal(value)
	_, err := conn.Create(key, data, 0, acls)
	if err != nil {
		return false
	}
	return true
}

// Get remember to use & annotation
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
	// assume that key exists in the related key list stored in zookeeper
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

// SetIfAbsentInMap will create a new tuple if there is no related tuple and add it to key list
func SetIfAbsentInMap(conn *zk.Conn, key string, value interface{}, Type int) bool {
	acls := zk.WorldACL(zk.PermAll)
	data, _ := json.Marshal(value)
	_, err := conn.Create(key, data, 0, acls)
	if err != nil {
		return false
	}
	var keyList string
	switch Type {
	case CHUNKSERVER:
		keyList = ChunkServerKeyList
		break
	case CHUNKMETADATA:
		keyList = ChunkMetaKeyList
		break
	case FILEMETADATA:
		keyList = FileMetaKeyList
	default:
		return false
	}
	for {
		rawList, sate, e := conn.Get(keyList)
		if e != nil {
			// TODO: handle consistency error
			return true
		}
		var list []string
		err = json.Unmarshal(rawList, &list)
		if err != nil {
			// TODO: handle consistency error
			return true
		}
		list = append(list, key)
		appended, _ := json.Marshal(list)
		_, er := conn.Set(keyList, appended, sate.Version)
		if er == nil {
			return true
		}
	}
}

// SetInMap will create a new tuple if there is no related tuple and add it to key list
// SetInMap will set the related tuple to new value if it exists
func SetInMap(conn *zk.Conn, key string, value interface{}, Type int) error {
	acls := zk.WorldACL(zk.PermAll)
	data, _ := json.Marshal(value)
	_, err := conn.Create(key, data, 0, acls)
	if err == nil {
		var keyList string
		switch Type {
		case CHUNKSERVER:
			keyList = ChunkServerKeyList
			break
		case CHUNKMETADATA:
			keyList = ChunkMetaKeyList
			break
		case FILEMETADATA:
			keyList = FileMetaKeyList
		default:
			return fmt.Errorf("unknown type")
		}
		for {
			rawList, sate, e := conn.Get(keyList)
			if e != nil {
				return e
			}
			var list []string
			err = json.Unmarshal(rawList, &list)
			if err != nil {
				return err
			}
			list = append(list, key)
			appended, _ := json.Marshal(list)
			_, er := conn.Set(keyList, appended, sate.Version)
			if er == nil {
				return nil
			}
		}
	}
	// assume that key exists in the related key list stored in zookeeper
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

func RemoveInMap(conn *zk.Conn, key string, Type int) {
	for {
		_, sate, e := conn.Get(key)
		if e != nil {
			return
		}
		err := conn.Delete(key, sate.Version)
		if err == nil {
			var keyList string
			switch Type {
			case CHUNKSERVER:
				keyList = ChunkServerKeyList
				break
			case CHUNKMETADATA:
				keyList = ChunkMetaKeyList
				break
			case FILEMETADATA:
				keyList = FileMetaKeyList
			default:
				return
			}
			for {
				rawList, state, er := conn.Get(keyList)
				if er != nil {
					return
				}
				var list []string
				err = json.Unmarshal(rawList, &list)
				if err != nil {
					return
				}
				var newList []string
				for _, v := range list{
					if v != key {
						newList = append(newList, v)
					}
				}
				deleted, _ := json.Marshal(newList)
				_, er = conn.Set(keyList, deleted, state.Version)
				if er == nil {
					return
				}
			}
		}
	}
}