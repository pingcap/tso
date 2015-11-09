package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
	"net"
	"sync"
	"time"
)

var serverAddress = flag.String("serveraddr", "localhost:1234", "server address")

func main() {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", *serverAddress)
			if err != nil {
				log.Fatal("dialing:", err)
			}

			r := bufio.NewReaderSize(conn, 8192)
			w := bufio.NewWriterSize(conn, 8192)
			cnt := 1000000
			var req [1]byte
			for i := 0; i < cnt; i++ {
				w.Write(req[:])
			}
			err = w.Flush()
			if err != nil {
				log.Fatal(err)
			}

			var resp proto.Response
			for i := 0; i < cnt; i++ {
				err = binary.Read(r, binary.BigEndian, &resp)
				if err != nil {
					log.Fatal(err)
				}
			}
			conn.Close()
		}()
	}
	wg.Wait()

	log.Debug(time.Since(start).Seconds())
}
