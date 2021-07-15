package main

import (
	"bufio"
	"fmt"
	"os"

	"./gfs/chunkserver"
	"./gfs/client"
	"./gfs/master"
)

func main() {
	m := master.NewAndServe("127.0.0.1:8081", "./master")
	cs1 := chunkserver.NewChunkServer("127.0.0.1:8082", "127.0.0.1:8081", "./chunk1")
	cs2 := chunkserver.NewChunkServer("127.0.0.1:8083", "127.0.0.1:8081", "./chunk2")
	cs3 := chunkserver.NewChunkServer("127.0.0.1:8084", "127.0.0.1:8081", "./chunk3")
	client.NewClient("127.0.0.1:8081", "127.0.0.1:8085")

	for {
		fmt.Println("print \"q\" to shut down all")
		reader := bufio.NewReader(os.Stdin)
		res, _ := reader.ReadString('\n')
		if res[:1] == "q" {
			cs1.Shutdown()
			cs2.Shutdown()
			cs3.Shutdown()
			m.Shutdown()
			return
		}
	}
}
