package gfs

import (
	"fmt"
	"net/rpc"
)

// Call is RPC call helper
func Call(srv string, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", string(srv))
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err
}

// CallAll applies the rpc call to all destinations.
func CallAll(dst []string, rpcname string, args interface{}) error {
	ch := make(chan error)
	for _, d := range dst {
		go func(addr string) {
			ch <- Call(addr, rpcname, args, nil)
		}(d)
	}
	errList := ""
	ok := true
	for _ = range dst {
		if err := <-ch; err != nil {
			ok = false
			errList += err.Error() + ";"
		}
	}

	if ok {
		return nil
	} else {
		return fmt.Errorf(errList)
	}
}

// CallMaster applies the rpc call to a master.
func CallMaster(rpcname string, args interface{}, reply interface{}) error {
	for _, addr := range Masters {
		c, errx := rpc.Dial("tcp", addr)
		if errx != nil {
			continue
		}
		err := c.Call(rpcname, args, reply)
		c.Close()
		return err
	}
	return fmt.Errorf("no master alive")
}