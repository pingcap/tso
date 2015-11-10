package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/ngaut/tso/client"
)

var serverAddress = flag.String("serveraddr", "localhost:1234", "server address")

const (
	total     = 1000 * 10000
	clientCnt = 100
)

func main() {
	go http.ListenAndServe(":6666", nil)
	var wg sync.WaitGroup
	start := time.Now()
	for x := 0; x < 100; x++ {
		c := client.NewClient(&client.Conf{ServerAddr: *serverAddress})
		wg.Add(1)
		go func() {
			defer wg.Done()
			cnt := total / clientCnt
			prs := make([]*client.PipelineRequest, 0, cnt)
			for i := 0; i < cnt; i++ {
				pr := c.GetTimestamp()
				prs = append(prs, pr)
			}

			for i := 0; i < cnt; i++ {
				<-prs[i].Done
			}
		}()
	}
	wg.Wait()

	log.Debugf("Total %d, use %v/s", total, time.Since(start).Seconds())
}
