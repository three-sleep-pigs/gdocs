package gfs

import (
	"fmt"
	"math/rand"
)

// Sample randomly chooses k elements from {0, 1, ..., n-1}.
// n should not be less than k.
func Sample(n, k int) ([]int, error) {
	if n < k {
		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
	}
	return rand.Perm(n)[:k], nil
}

// Call is RPC call helper
func Call(srv string, rpcname string, args interface{}, reply interface{}) error {
	client, errx := rpc.Dial("tcp", string(srv))
	if errx != nil {
		return errx
	}
	defer client.Close()

	err := client.Call(rpcname, args, reply)
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