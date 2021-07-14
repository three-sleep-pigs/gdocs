package test

import (
	"../../gfs"
	"../chunkserver"
	"../client"
	"../master"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)
// help func
const (
	msAddr = "127.0.0.1:8080"
	msRootDir = "../msroot"
	csNum = 3
)

func getCsRoots() [3]string {
	return [3]string{"../csroot1", "../csroot2", "../csroot3"}
}

func getCsAddrs() [3]string {
	return [3]string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083"}
}

func getRemoveDirs() [4]string {
	return [4]string{"../msroot","../csroot1", "../csroot2", "../csroot3"}
}

func RunClient() *client.Client {
	return client.NewClient(msAddr, "127.0.0.1:8084")
}

func RunMaster() *master.Master {
	return master.NewAndServe(msAddr, msRootDir)
}

func RunChunkServers() []*chunkserver.ChunkServer{
	csAddrs := getCsAddrs()
	csRoots := getCsRoots()
	var css = make([]*chunkserver.ChunkServer, 0)
	for i := 0; i < csNum; i++ {
		cs := chunkserver.NewChunkServer(csAddrs[i], msAddr, csRoots[i])
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

func ShutDown(m *master.Master, css []*chunkserver.ChunkServer) {
	m.Shutdown()
	for i := 0; i < len(css); i++ {
		css[i].Shutdown()
	}
}

// tests begin
var (
	m     *master.Master
	cs    []*chunkserver.ChunkServer
	c     *client.Client
	csAdd [] string
	root  string // root of tmp file path
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
}

func gfsShutDown() {
	ShutDown(m, cs)
}

func gfsClean() {
	CleanFiles()
}

/*
 *  TEST SUITE 0 - TEST CLIENT START
 */
func TestClientStart(t *testing.T)  {
	c = RunClient()
	if c == nil {
		t.Error("start a client fail")
	}
}
/*
 *  TEST SUITE 1 - MASTER FILE NAMESPACE
 */
func TestCreateFile(t *testing.T) {
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)
	err := m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err == nil {
		t.Error("the same file has been created twice")
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
	println("GFS FILES CLEAN")
	gfsClean()
}

func TestDeleteFiles(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("GFS START")
	gfsRun()
	time.Sleep(time.Duration(5) * time.Second)
	err := m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	// delete files
	err = m.RPCDeleteFile(gfs.DeleteFileArg{Path: "/test1.txt"}, &gfs.DeleteFileReply{})
	if err != nil {
		t.Error(err)
	}
	//
	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
		t.Error("Delete file failed")
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

func TestMkdir(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("GFS START")
	gfsRun()
	time.Sleep(time.Duration(5) * time.Second)
	err := m.RPCMkdir(gfs.MkdirArg{Path: "/dir1"}, &gfs.MkdirReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCMkdir(gfs.MkdirArg{Path: "/dir1"}, &gfs.MkdirReply{})
	if err == nil {
		t.Error("the same dir has been created twice")
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

func TestRenameFile(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("GFS START")
	gfsRun()
	time.Sleep(time.Duration(5) * time.Second)
	err := m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCRenameFile(gfs.RenameFileArg{Source: "/test1.txt", Target: "/test2.txt"}, &gfs.RenameFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/test2.txt"}, &gfs.CreateFileReply{})
	if err == nil {
		t.Error("the same file has been created twice")
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

func TestFileNameSpaceConcurrently(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("GFS START")
	gfsRun()
	time.Sleep(time.Duration(5) * time.Second)
	// create files concurrently
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(num int, t *testing.T) {
			err := m.RPCCreateFile(gfs.CreateFileArg{Path: fmt.Sprintf("/test%d.txt", num)}, &gfs.CreateFileReply{})
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i, t)
	}
	wg.Wait()
	// delete files concurrently
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(num int, t *testing.T) {
			err := m.RPCDeleteFile(gfs.DeleteFileArg{Path: fmt.Sprintf("/test%d.txt", num)}, &gfs.DeleteFileReply{})
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i, t)
	}
	wg.Wait()
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

/*
 *  TEST SUITE 2 - MASTER CHUNK
 */
func TestRPCGetChunkHandle(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("GFS START")
	gfsRun()
	time.Sleep(time.Duration(5) * time.Second)
	err := m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	var r1, r2 gfs.GetChunkHandleReply
	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: "/test1.txt", Index: 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: "/test1.txt", Index: 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	if r1.Handle != r2.Handle {
		t.Errorf("got different handle: %v and %v", r1.Handle, r2.Handle)
	}

	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: "/test1.txt", Index: 2}, &r2)
	if err == nil {
		t.Error("discontinuous chunk should not be created")
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

func TestGetReplicas(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("GFS START")
	gfsRun()
	time.Sleep(time.Duration(5) * time.Second)
	err := m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	var r1 gfs.GetChunkHandleReply
	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: "/test1.txt", Index: 0}, &r1)
	if err != nil {
		t.Error(err)
	}
	var r2 gfs.GetReplicasReply
	err = m.RPCGetReplicas(gfs.GetReplicasArg{Handle: r1.Handle}, &r2)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

func TestGetFileInfo(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("GFS START")
	gfsRun()
	time.Sleep(time.Duration(5) * time.Second)
	var r1 gfs.GetFileInfoReply
	err := m.RPCGetFileInfo(gfs.GetFileInfoArg{Path: "/test1.txt"}, &r1)
	if err == nil {
		t.Error("get not existing file info")
	}
	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCGetFileInfo(gfs.GetFileInfoArg{Path: "/test1.txt"}, &r1)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

/*
 *  TEST SUITE 3 - Client API
 */

// if the append would cause the chunk to exceed the maximum size
// this chunk should be pad and the data should be appended to the next chunk
func TestPadOver(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("DEBUG FILES CLEAN")
	CleanDebugFiles()
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)
	err := c.Create("/appendOver.txt")
	if err != nil {
		t.Error(err)
	}

	bound := gfs.MaxAppendSize - 1
	buf := make([]byte, bound)
	for i := 0; i < bound; i++ {
		buf[i] = byte(i%26 + 'a')
	}

	for i := 0; i < 4; i++ {
		_, err = c.Append("/appendOver.txt", buf)
		if err != nil {
			t.Error(err)
		}
	}

	buf = buf[:5]
	var offset int64
	// an append cause pad, and client should retry to next chunk
	offset, err = c.Append("/appendOver.txt", buf)
	if err != nil {
		t.Error(err)
	}
	if offset != gfs.MaxChunkSize { // i.e. 0 at next chunk
		t.Error("data should be appended to the beginning of next chunk")
	}
	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}

// big data that invokes several chunks
func TestWriteReadBigData(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("DEBUG FILES CLEAN")
	CleanDebugFiles()
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)
	err := c.Create("/bigData.txt")
	if err != nil {
		t.Error(err)
	}
	size := gfs.MaxChunkSize * 3
	expected := make([]byte, size)
	for i := 0; i < size; i++ {
		expected[i] = byte(i%26 + 'a')
	}

	// write large data
	_, err = c.Write("/bigData.txt", gfs.MaxChunkSize/2, expected)
	if err != nil {
		t.Error(err)
	}
	// read
	buf := make([]byte, size)
	var n int64
	n, err = c.Read("/bigData.txt", gfs.MaxChunkSize/2, buf)
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
	n, err = c.Read("/bigData.txt", gfs.MaxChunkSize/2+int64(size), buf)
	if err == nil {
		t.Error("an error should be returned if read at EOF")
	}

	// test append offset
	var offset int64
	buf = buf[:gfs.MaxAppendSize-1]
	offset, err = c.Append("/bigData.txt", buf)
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

// a concurrent producer-consumer number collector for testing race contiditon
func TestConcurrentReadAndAppend(t *testing.T) {
	println("GFS FILES CLEAN")
	gfsClean()
	println("DEBUG FILES CLEAN")
	CleanDebugFiles()
	gfsRun()
	println("GFS START")
	time.Sleep(time.Duration(5) * time.Second)
	// create file
	filePath := "/concurrentTest.txt"
	err := c.Create(filePath)
	if err != nil {
		t.Error(err)
	}
	// concurrent append and read
	readTick := 100 * time.Millisecond
	writeTick := 200 * time.Millisecond
	toWriteBuf := make([]byte, 26)
	for i := 0; i < 26; i++ {
		toWriteBuf[i] = byte(i%26 + 'a')
	}
	go func() {
		readTicker := time.Tick(readTick)
		writeTicker := time.Tick(writeTick)
		num := 0
		for {
			if num == 6 {
				return
			}
			select {
			case <- readTicker:
				buf := make([]byte, num * 26)
				_, e := c.Read(filePath, 0, buf)
				if e != nil {
					t.Error(e)
				}
				var strToConvert string
				strToConvert = string(buf)
				fmt.Println("[READ]", strToConvert)
			case <- writeTicker:
				_, e := c.Append(filePath, toWriteBuf)
				if e != nil {
					t.Error(e)
				}
				num++
			default:
			}
		}
	}()

	time.Sleep(time.Duration(5) * time.Second)
	println("GFS SHUTDOWN")
	gfsShutDown()
}