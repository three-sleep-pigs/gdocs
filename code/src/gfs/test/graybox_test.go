package test

import (
	"../../gfs"
	"../chunkserver"
	"../client"
	"../master"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)
// help func
const (
	msNum = 2	// master num
	csNum = 5	// chunk server num
	cNum = 2	// client num
	N = 100
)

// get chunk server parameters for test
func getCsAddrs() []string {
	return []string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085"}
}

func getCsRoots() []string {
	return []string{"../csroot1", "../csroot2", "../csroot3", "../csroot4", "../csroot5"}
}

// get client addrs for test
func getCAddrs() []string {
	return []string{"127.0.0.1:7070", "127.0.0.1:7071"}
}

func getRemoveDirs() []string {
	return []string{"../csroot1", "../csroot2", "../csroot3"}
}

func RunClient() []*client.Client {
	cAddrs := getCAddrs()
	var cs = make([]*client.Client, 0)
	for i := 0; i < csNum; i++ {
		c := client.NewClient(cAddrs[i])
		cs = append(cs, c)
	}
	return cs
}

func RunMaster() []*master.Master {
	msAddrs := getMsAddrs()
	var ms = make([]*master.Master, 0)
	for i := 0; i < csNum; i++ {
		m := master.NewAndServe(msAddrs[i])
		ms = append(ms, m)
	}
	return ms
}

func RunChunkServers() []*chunkserver.ChunkServer{
	csAddrs := getCsAddrs()
	csRoots := getCsRoots()
	var css = make([]*chunkserver.ChunkServer, 0)
	for i := 0; i < csNum; i++ {
		cs := chunkserver.NewChunkServer(csAddrs[i], csRoots[i])
		css = append(css, cs)
	}
	return css
}

func CleanFiles() {
	dirs := getRemoveDirs()
	for i := 0; i < len(dirs); i++ {
		os.RemoveAll(dirs[i])
	}
}

func CleanDebugFiles() {
	os.RemoveAll("../debug")
	os.Mkdir("../debug", 0777)
}

func ShutDown(css []*chunkserver.ChunkServer) {
	for i := 0; i < len(css); i++ {
		css[i].Shutdown()
	}
}

// tests begin
var (
	m     []*master.Master
	cs    []*chunkserver.ChunkServer
	c     []*client.Client
)

func errorAll(ch chan error, n int, t *testing.T) {
	for i := 0; i < n; i++ {
		if err := <-ch; err != nil {
			t.Error(err)
		}
	}
}

func gfsRun() {
	m = RunMaster()
	cs = RunChunkServers()
	c = RunClient()
}

func gfsShutDown() {
	ShutDown(cs)
}

func gfsClean() {
	CleanFiles()
}

/*
 *  TEST SUITE 0 - START THE WHOLE SYSTEM
 */
func TestStart(t *testing.T)  {
	gfsRun()
	for i := 0; i < msNum; i++ {
		if m[i] == nil {
			t.Fatalf("start masters failed")
		}
	}
	for i := 0; i < csNum; i++ {
		if cs[i] == nil {
			t.Fatalf("start chunk servers failed")
		}
	}
	for i := 0; i < cNum; i++ {
		if c[i] == nil {
			t.Fatalf("start clients failed")
		}
	}
	// sleep for a fit time
	time.Sleep(time.Duration(5) * time.Second)
}

/*
 *  TEST SUITE 1 - MASTER FILE NAMESPACE
 */
// create same file in to different masters
func TestCreateFile(t *testing.T) {
	r := rand.Int()
	m1 := m[r % msNum]
	m2 := m[(r + 1) % msNum]
	err := m1.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m2.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err == nil {
		t.Error("the same file has been created twice")
	}
}
// delete file and create it again in to different masters
func TestDeleteFiles(t *testing.T) {
	r := rand.Int()
	m1 := m[r % msNum]
	m2 := m[(r + 1) % msNum]
	// delete files
	err := m1.RPCDeleteFile(gfs.DeleteFileArg{Path: "/test1.txt"}, &gfs.DeleteFileReply{})
	if err != nil {
		t.Error(err)
	}
	//
	err = m2.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
		t.Error("Delete file failed")
	}
}
// mkdir in to different masters
func TestMkdir(t *testing.T) {
	r := rand.Int()
	m1 := m[r % msNum]
	m2 := m[(r + 1) % msNum]
	err := m1.RPCMkdir(gfs.MkdirArg{Path: "/dir1"}, &gfs.MkdirReply{})
	if err != nil {
		t.Error(err)
	}
	err = m2.RPCMkdir(gfs.MkdirArg{Path: "/dir1"}, &gfs.MkdirReply{})
	if err == nil {
		t.Error("the same dir has been created twice")
	}
}

/*
 *  TEST SUITE 2 - MASTER CHUNK
 */
// get chunk handle from two different masters
func TestRPCGetChunkHandle(t *testing.T) {
	r := rand.Int()
	m1 := m[r % msNum]
	m2 := m[(r + 1) % msNum]
	err := m1.RPCCreateFile(gfs.CreateFileArg{Path: "/testGetChunkHandle.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	var r1, r2 gfs.GetChunkHandleReply
	err = m1.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: "/testGetChunkHandle.txt", Index: 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	err = m2.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: "/testGetChunkHandle.txt", Index: 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	if r1.Handle != r2.Handle {
		t.Errorf("got different handle: %v and %v", r1.Handle, r2.Handle)
	}

	err = m1.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: "/testGetChunkHandle.txt", Index: 2}, &r2)
	if err == nil {
		t.Error("discontinuous chunk should not be created")
	}
}

func TestGetReplicas(t *testing.T) {
	r := rand.Int()
	m1 := m[r % msNum]
	m2 := m[(r + 1) % msNum]
	err := m1.RPCCreateFile(gfs.CreateFileArg{Path: "/testGetReplicas.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	var r1 gfs.GetChunkHandleReply
	err = m2.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: "/testGetReplicas.txt", Index: 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	var r2 gfs.GetReplicasReply
	err = m2.RPCGetReplicas(gfs.GetReplicasArg{Handle: r1.Handle}, &r2)
	if err != nil {
		t.Error(err)
	}
}

// check if the content of replicas are the same, returns the number of replicas
func checkReplicas(handle int64, length int64, t *testing.T) int {
	var data [][]byte

	// get replicas location from master
	var l gfs.GetReplicasReply
	err := m[0].RPCGetReplicas(gfs.GetReplicasArg{Handle: handle}, &l)
	if err != nil {
		t.Error(err)
	}

	// read
	args := gfs.ReadChunkArg{Handle: handle, Length: length}
	Locations := make([]string, 0)
	for _, v := range l.Secondaries {
		Locations = append(Locations, v)
	}
	Locations = append(Locations, l.Primary)
	for _, addr := range Locations {
		var r gfs.ReadChunkReply
		err := gfs.Call(addr, "ChunkServer.RPCReadChunk", args, &r)
		if err == nil {
			data = append(data, r.Data)
		}
	}

	// check equality
	for i := 1; i < len(data); i++ {
		if !reflect.DeepEqual(data[0], data[i]) {
			t.Error("replicas are different. ", data[0], "vs", data[i])
		}
	}

	return len(data)
}

func TestReplicaEquality(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	var data [][]byte
	p := "/TestWriteChunk.txt"
	err := m[0].RPCCreateFile(gfs.CreateFileArg{Path: p}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m[0].RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p, Index: 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	n := checkReplicas(r1.Handle, N*2, t)
	if n != gfs.DefaultNumReplicas {
		t.Error("expect", gfs.DefaultNumReplicas, "replicas, got only", len(data))
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

func TestGetFileInfo(t *testing.T) {
	var r1 gfs.GetFileInfoReply
	err := m[0].RPCCreateFile(gfs.CreateFileArg{Path: "/testGetFileInfo.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m[0].RPCGetFileInfo(gfs.GetFileInfoArg{Path: "/testGetFileInfo.txt"}, &r1)
	if err != nil {
		t.Error(err)
	}
}

/*
 *  TEST SUITE 3 - Client API
 */

// if the append would cause the chunk to exceed the maximum size
// this chunk should be pad and the data should be appended to the next chunk
func TestPadOver(t *testing.T) {
	r := rand.Int()
	c1 := c[r % msNum]
	err := c1.Create("/appendOver.txt")
	if err != nil {
		t.Error(err)
	}

	bound := gfs.MaxAppendSize - 1
	buf := make([]byte, bound)
	for i := 0; i < bound; i++ {
		buf[i] = byte(i%26 + 'a')
	}

	for i := 0; i < 4; i++ {
		_, err = c1.Append("/appendOver.txt", buf)
		if err != nil {
			t.Error(err)
		}
	}

	buf = buf[:5]
	var offset int64
	// an append cause pad, and client should retry to next chunk
	offset, err = c1.Append("/appendOver.txt", buf)
	if err != nil {
		t.Error(err)
	}
	if offset != gfs.MaxChunkSize { // i.e. 0 at next chunk
		t.Error("data should be appended to the beginning of next chunk")
	}
}

// big data that invokes several chunks
func TestWriteReadBigData(t *testing.T) {
	r := rand.Int()
	c1 := c[r % msNum]
	err := c1.Create("/bigData.txt")
	if err != nil {
		t.Error(err)
	}
	size := gfs.MaxChunkSize * 3
	expected := make([]byte, size)
	for i := 0; i < size; i++ {
		expected[i] = byte(i%26 + 'a')
	}

	// write large data
	_, err = c1.Write("/bigData.txt", gfs.MaxChunkSize/2, expected)
	if err != nil {
		t.Error(err)
	}
	// read
	buf := make([]byte, size)
	var n int64
	n, err = c1.Read("/bigData.txt", gfs.MaxChunkSize/2, buf)
	if err != nil {
		t.Error(err)
	}

	if n != int64(size) {
		t.Error("read counter is wrong")
	}
	if !reflect.DeepEqual(expected, buf) {
		t.Error("read wrong data")
	}

	// test read at EOF
	n, err = c1.Read("/bigData.txt", gfs.MaxChunkSize/2+int64(size), buf)
	if err == nil {
		t.Error("an error should be returned if read at EOF")
	}

	// test append offset
	var offset int64
	buf = buf[:gfs.MaxAppendSize-1]
	offset, err = c1.Append("/bigData.txt", buf)
	if offset != gfs.MaxChunkSize/2+int64(size) {
		t.Error("append in wrong offset")
	}
	if err != nil {
		t.Error(err)
	}
	// TODO: test over chunk EOF
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

/*
 *  TEST SUITE 4 - Fault Tolerance
 */
// Shutdown primary chunk server during appending
func TestShutdownPrimary(t *testing.T) {
	if c == nil {
		t.Fatalf("start a client fail")
	}
	println("GFS FILES CLEAN")
	gfsClean()
	println("DEBUG FILES CLEAN")
	CleanDebugFiles()
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)

	p := "/shutdown.txt"
	ch := make(chan error, N+3)

	ch <- c.Create(p)

	expected := make(map[int][]byte)
	toDelete := make(map[int][]byte)
	for i := 0; i < N; i++ {
		expected[i] = []byte(fmt.Sprintf("%2d", i))
		toDelete[i] = []byte(fmt.Sprintf("%2d", i))
	}

	// get two replica locations
	var r1 gfs.GetChunkHandleReply
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p, Index: 0}, &r1)
	var l gfs.GetReplicasReply
	ch <- m.RPCGetReplicas(gfs.GetReplicasArg{Handle: r1.Handle}, &l)

	for i := 0; i < N; i++ {
		go func(x int) {
			_, err := c.Append(p, expected[x])
			ch <- err
		}(i)
	}
	time.Sleep(time.Duration(1) * time.Second)
	// choose primary server to shutdown during appending
	for i, v := range cs {
		if csAdd[i] == l.Primary {
			v.Shutdown()
		}
	}

	errorAll(ch, N+3, t)
	time.Sleep(time.Duration(5) * time.Second)

	// check correctness, append at least once
	for x := 0; x < gfs.MaxChunkSize/2 && len(toDelete) > 0; x++ {
		buf := make([]byte, 2)
		n, err := c.Read(p, int64(x*2), buf)
		if err != nil {
			t.Error("read error ", err)
		}
		if n != 2 {
			t.Error("should read exactly 2 bytes but", n, "instead")
		}

		key := -1
		for k, v := range expected {
			if reflect.DeepEqual(buf, v) {
				key = k
				break
			}
		}
		if key == -1 {
			t.Error("incorrect data", buf)
		} else {
			delete(toDelete, key)
		}
	}
	if len(toDelete) != 0 {
		t.Errorf("missing data %v", toDelete)
	}

	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}
// Shutdown replica chunk server during appending
func TestShutdownReplica(t *testing.T) {
	if c == nil {
		t.Fatalf("start a client fail")
	}
	println("GFS FILES CLEAN")
	gfsClean()
	println("DEBUG FILES CLEAN")
	CleanDebugFiles()
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)

	p := "/shutdown.txt"
	ch := make(chan error, N+3)

	ch <- c.Create(p)

	expected := make(map[int][]byte)
	toDelete := make(map[int][]byte)
	for i := 0; i < N; i++ {
		expected[i] = []byte(fmt.Sprintf("%2d", i))
		toDelete[i] = []byte(fmt.Sprintf("%2d", i))
	}

	// get two replica locations
	var r1 gfs.GetChunkHandleReply
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p, Index: 0}, &r1)
	var l gfs.GetReplicasReply
	ch <- m.RPCGetReplicas(gfs.GetReplicasArg{Handle: r1.Handle}, &l)

	for i := 0; i < N; i++ {
		go func(x int) {
			_, err := c.Append(p, expected[x])
			ch <- err
		}(i)
	}
	time.Sleep(time.Duration(1) * time.Second)
	// choose primary server to shutdown during appending
	for i, v := range cs {
		if csAdd[i] == l.Secondaries[0] {
			v.Shutdown()
		}
	}

	errorAll(ch, N+3, t)
	time.Sleep(time.Duration(5) * time.Second)

	// check correctness, append at least once
	for x := 0; x < gfs.MaxChunkSize/2 && len(toDelete) > 0; x++ {
		buf := make([]byte, 2)
		n, err := c.Read(p, int64(x*2), buf)
		if err != nil {
			t.Error("read error ", err)
		}
		if n != 2 {
			t.Error("should read exactly 2 bytes but", n, "instead")
		}

		key := -1
		for k, v := range expected {
			if reflect.DeepEqual(buf, v) {
				key = k
				break
			}
		}
		if key == -1 {
			t.Error("incorrect data", buf)
		} else {
			delete(toDelete, key)
		}
	}
	if len(toDelete) != 0 {
		t.Errorf("missing data %v", toDelete)
	}

	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

/*
 *  TEST SUITE 5 - Persistent Tests
 */
func TestPersistent(t *testing.T) {
	if c == nil {
		t.Fatalf("start a client fail")
	}
	println("GFS FILES CLEAN")
	gfsClean()
	println("DEBUG FILES CLEAN")
	CleanDebugFiles()
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)

	err := c.Create("/persistent.txt")
	if err != nil {
		t.Error(err)
	}
	writeBuf := make([]byte, 26)
	for i := 0; i < 26; i++ {
		writeBuf[i] = byte(i%26 + 'a')
	}
	_, err = c.Write("/persistent.txt", 0, writeBuf)
	if err != nil {
		t.Error(err)
	}
	readBuf := make([]byte, 26)
	_, err = c.Read("/persistent.txt", 0, readBuf)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(writeBuf, readBuf) {
		t.Error("read wrong data before shut down")
	}
	time.Sleep(time.Duration(5) * time.Second)

	println("GFS SHUTDOWN")
	gfsShutDown()
	time.Sleep(time.Duration(5) * time.Second)

	gfsRun()
	println("GFS START AGAIN")
	time.Sleep(time.Duration(5) * time.Second)

	_, err = c.Read("/persistent.txt", 0, readBuf)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(writeBuf, readBuf) {
		t.Error("read wrong data after shut down")
	}
	time.Sleep(time.Duration(5) * time.Second)

	println("GFS SHUTDOWN")
	gfsShutDown()
}

/*
 *  TEST SUITE - Performance Test
 */
func BenchmarkCreate(b *testing.B) {
	println("GFS FILES CLEAN")
	gfsClean()
	if c == nil {
		c = RunClient()
	}
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)
	b.ResetTimer()
	var wg sync.WaitGroup
	n := b.N
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func (i int) {
			err := c.Create(fmt.Sprintf("test%d.txt", i))
			if err != nil {
				println(fmt.Sprintf("Create file <test%d.txt> failed <%s>", i, err))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	println("GFS SHUTDOWN")
	gfsShutDown()
}

func BenchmarkCreateThenDelete(b *testing.B) {
	c = RunClient()
	if c == nil {
		b.Fatalf("start a client fail")
	}
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)
	var wg sync.WaitGroup
	n := b.N
	wg.Add(b.N)
	for i := 0; i < n; i++ {
		go func (i int) {
			c.Create(fmt.Sprintf("test%d.txt", i))
			wg.Done()
		}(i)
	}
	wg.Wait()
	wg.Add(b.N)
	for i := 0; i < n; i++ {
		go func (i int) {
			c.Delete(fmt.Sprintf("test%d.txt", i))
			wg.Done()
		}(i)
	}
	wg.Wait()
	println("GFS SHUTDOWN")
	gfsShutDown()
	println("GFS FILES CLEAN")
	gfsClean()
}