package main

import (
	"flag"
	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
	"net/rpc"
	"sync"
	"time"
)

var serverAddress = flag.String("serveraddr", "localhost:1234", "server address")

const (
	method = "TimestampOracle.Alloc"
)

func main() {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := rpc.DialHTTP("tcp", *serverAddress)
			if err != nil {
				log.Fatal("dialing:", err)
			}

			cnt := 10000
			calls := make(chan *rpc.Call, cnt)
			for i := 0; i < cnt; i++ {
				req := &proto.Request{}
				var resp proto.Response
				client.Go(method, req, &resp, calls)
			}
			for i := 0; i < cnt; i++ {
				<-calls
				//	resp := (<-calls).Reply.(*proto.Response)
				//	log.Debug(resp.Logical)
			}
		}()
	}
	wg.Wait()

	log.Debug(time.Since(start).Seconds())
}
