package main

import (
	"bufio"
	"fmt"
	"os"

	"./gfs/chunkserver"
	"./gfs/client"
	"./gfs/master"
)

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("master      <addr> <root path>")
	fmt.Println("chunkserver <addr> <root path> <master addr>")
	fmt.Println("client      <addr> <master addr>")
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
		res, _ := reader.ReadString('\n')
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
		res, _ := reader.ReadString('\n')
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
	c := client.NewClient(os.Args[3], os.Args[2])
	if c == nil {
		fmt.Println("start client fail")
		return
	}

	for {
		fmt.Println("print \"q\" to shut down client")
		reader := bufio.NewReader(os.Stdin)
		res, _ := reader.ReadString('\n')
		if res[:1] == "q" {
			return
		}
	}
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
