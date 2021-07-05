package gfs

import (
	"time"
)

// system config
const (
	// master
	DeletedFilePrefix  = "__del__"
	MinimumNumReplicas = 2
	DefaultNumReplicas = 3
	LeaseExpire = 1 * time.Minute
	ServerCheckInterval = 400 * time.Millisecond
	StoreMetaInterval = 1 * time.Hour  
	MetaFileName = "gfs-master.meta"
	ServerTimeout = 1 * time.Second
)