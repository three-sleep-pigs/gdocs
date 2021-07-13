package main

import (
	"./gfs/chunkserver"
	"./gfs/client"
	"./gfs/master"
	"bufio"
	"fmt"
	"os"
)

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("master      <addr(:port)> <root path>")
	fmt.Println("chunkserver <addr(:port)> <root path> <master addr(:port)>")
	fmt.Println("client      <addr(127.0.0.1:port)>    <master addr(:port)>")
	fmt.Println()
}

func runMaster() {
	if len(os.Args) < 4 {
		printUsage()
		return
	}
	m := master.NewAndServe(os.Args[2], os.Args[3])
	if m == nil {
		fmt.Println("start master fail")
		return
	}

	for {
		fmt.Println("print \"q\" to shut down master")
		reader := bufio.NewReader(os.Stdin)
		res,_ := reader.ReadString('\n')
		if res[:1] == "q" {
			m.Shutdown()
			return
		}
	}
}

func runChunkServer() {
	if len(os.Args) < 5 {
		printUsage()
		return
	}
	cs := chunkserver.NewChunkServer(os.Args[2], os.Args[4], os.Args[3])
	if cs == nil {
		fmt.Println("start chunk server fail")
		return
	}

	for {
		fmt.Println("print \"q\" to shut down chunk server")
		reader := bufio.NewReader(os.Stdin)
		res,_ := reader.ReadString('\n')
		if res[:1] == "q" {
			cs.Shutdown()
			return
		}
	}
}

func runClient() {
	if len(os.Args) < 4 {
		printUsage()
		return
	}
	fmt.Println("print Ctrl+C to shut down client")
	client.NewClient(os.Args[2], os.Args[3])
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}
	switch os.Args[1] {
	case "master":
		runMaster()
	case "chunkserver":
		runChunkServer()
	case "client":
		runClient()
	default:
		printUsage()
	}
}
