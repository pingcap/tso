package client

import (
	"bufio"
	"github.com/ngaut/deadline"
	"net"
	"time"
)

//not thread-safe
type Conn struct {
	addr string
	net.Conn
	closed     bool
	r          *bufio.Reader
	w          *bufio.Writer
	netTimeout time.Duration
}

func NewConnection(addr string, netTimeout time.Duration) (*Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Duration(netTimeout)*time.Second)
	if err != nil {
		return nil, err
	}

	return &Conn{
		addr:       addr,
		Conn:       conn,
		r:          bufio.NewReaderSize(conn, 512*1024),
		w:          bufio.NewWriterSize(deadline.NewDeadlineWriter(conn, netTimeout), 512*1024),
		netTimeout: netTimeout,
	}, nil
}

//require read to use bufio
func (c *Conn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *Conn) Flush() error {
	return c.w.Flush()
}

func (c *Conn) Write(p []byte) (int, error) {
	return c.w.Write(p)
}
