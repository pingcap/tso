package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/ngaut/log"
	"github.com/ngaut/tso/tso-server/server"
)

var (
	addr = flag.String("addr", ":1234", "server listening address")
)

func main() {
	flag.Parse()

	oracle, err := server.NewTimestampOracle(*addr)
	if err != nil {
		log.Fatal(err)
	}

	go http.ListenAndServe(":5555", nil)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		oracle.Close()
		os.Exit(0)
	}()

	oracle.Run()
}
