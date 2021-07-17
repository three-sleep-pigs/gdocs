package main

import (
	"./gfs"
	"./gfs/chunkserver"
	"./gfs/client"
	"./gfs/master"
	"bufio"
	"fmt"
	"os"
)

var ChunkServers = []string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084", "127.0.0.1:8085"}
var Clients = []string{"127.0.0.1:7070"}

func main() {
	var chunkservers []*chunkserver.ChunkServer
	for _, addr := range gfs.Masters {
		master.NewAndServe(addr)
	}
	for index, addr := range ChunkServers {
		cs := chunkserver.NewChunkServer(addr, fmt.Sprintf("./chunk%d", index))
		chunkservers = append(chunkservers, cs)
	}
	for _, addr := range Clients {
		client.NewClient(addr)
	}

	for {
		fmt.Println("print \"q\" to shut down all")
		reader := bufio.NewReader(os.Stdin)
		res,_ := reader.ReadString('\n')
		if res[:1] == "q" {
			for _, cs := range chunkservers {
				cs.Shutdown()
			}
			return
		}
	}
}
