package client

import (
	"container/list"
	"encoding/binary"
	"time"

	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
)

type Client struct {
	requests chan *PipelineRequest

	pending *list.List
	conf    *Conf
}

type Conf struct {
	ServerAddr string
}

type PipelineRequest struct {
	Done  chan error
	Reply *proto.Response
}

func NewClient(conf *Conf) *Client {
	c := &Client{
		requests: make(chan *PipelineRequest, 200000),
		pending:  list.New(),
		conf:     conf,
	}

	go c.workerLoop()

	return c
}

func (c *Client) cleanupPending(err error) {
	log.Warn(err)
	length := c.pending.Len()
	for i := 0; i < length; i++ {
		e := c.pending.Front()
		c.pending.Remove(e)
		e.Value.(*PipelineRequest).Done <- err
	}
}

func (c *Client) notifyOne(reply *proto.Response) {
	e := c.pending.Front()
	c.pending.Remove(e)
	req := e.Value.(*PipelineRequest)
	req.Reply = reply
	req.Done <- nil
}

func (c *Client) writeRequests(session *Conn) error {
	var protoHdr [1]byte
	for i := 0; i < c.pending.Len(); i++ {
		session.Write(protoHdr[:])
	}
	return session.Flush()
}

func (c *Client) handleResponse(session *Conn) error {
	length := c.pending.Len()
	for i := 0; i < length; i++ {
		var resp proto.Response
		err := binary.Read(session, binary.BigEndian, &resp)
		if err != nil {
			return err
		}
		c.notifyOne(&resp)
	}

	return nil
}

func (c *Client) do() error {
	session, err := NewConnection(c.conf.ServerAddr, time.Duration(1*time.Second))
	if err != nil {
		return err
	}
	defer session.Close()
	for {
		select {
		case req := <-c.requests:
			c.pending.PushBack(req)
			length := len(c.requests)
			for i := 0; i < length; i++ {
				req = <-c.requests
				c.pending.PushBack(req)
			}
			err = c.writeRequests(session)
			if err != nil {
				return err
			}
			err = c.handleResponse(session)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) workerLoop() {
	for {
		err := c.do()
		if err != nil {
			log.Warn(err)
		}
		time.Sleep(time.Second)
	}
}

func (c *Client) GetTimestamp() *PipelineRequest {
	pr := &PipelineRequest{
		Done: make(chan error, 1),
	}
	c.requests <- pr
	return pr
}
