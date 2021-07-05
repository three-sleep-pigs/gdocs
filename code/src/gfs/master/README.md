# Master Design
### Yichen Liao and Jiaqi Xu
## Master Struct
``` go
type Master struct {
	address    string // master server address
	serverRoot string
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to ture if server is shutdown
	nhLock 		sync.Mutex
	nextHandle	int64

	// all keys from the following 3 maps are string
	// initialization in new and serve
	// from full path to file metadata
	fileNamespace  cmap.ConcurrentMap
	// from chunk handle to chunk metadata
	chunkNamespace cmap.ConcurrentMap
	// from chunk server address to chunk server info
	chunkServerInfos	cmap.ConcurrentMap

	// list of chunk handles need a new replicas
	rnlLock 	sync.RWMutex
	replicasNeedList []int64
}
```
## Master RPC funcs
#### `chunkserver -----> master`
+ **RPCHeartbeat** is called by *chunkserver* to
  let the *master* know that a *chunkserver* is alive, and reply with garbage
  that the *chunkserver* which needs to clean.
  + At first, according to `chunkserver infos` stored in *master*, we can decide
    if it's the first time the *chunkserver* connects with the master.
  + if it is the first time, *master* store the connecting info(`address`, `last connecting time` etc.)
    and let the *chunkserver* to report itself and update related stored information(`chunk metadata`, `chunkserver infos about stored chunks`).
  + if it is not the first time, check the slice of `leases to extend` in RPC arg, extend the lease, and put the 
    garbage needing *chunkserver* to clean in relpy.
#### `client -----> master`    
+ **RPCGetReplicas** is called by client to find the lease holder of the chunk and secondaries of the chunk
  + check the lease holder's expire time, if it is out of date, choose a new one.
    and reply to *client*
+ **RPCGetFileInfo** is called by client to get file information.
+ **RPCGetChunkHandle** returns the chunk handle of (path, index).
  + If the requested index is bigger than the number of chunks of this path by one, create one.
+ **RPCCreateFile** is called by client to create a new file
+ **RPCDeleteFile** is called by client to delete a file
+ **RPCRenameFile** is called by client to rename a file
+ **RPCMkdir** is called by client to make a new directory
+ ##### BTW, all the file related operation will ask its parents' read lock and its write lock according to the paper
## Master other funcs
