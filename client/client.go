package client

import (
	"container/list"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/tso/proto"
)

const (
	maxPipelineRequest = 100000
)

// Client is a timestamp oracle client.
type Client struct {
	requests chan *PipelineRequest

	pending *list.List
	conf    *Conf
}

// Conf is the configuration.
type Conf struct {
	ServerAddr string
}

// PipelineRequest let you get the timestamp with pipeline.
type PipelineRequest struct {
	done  chan error
	reply *proto.Response
}

func newPipelineRequest() *PipelineRequest {
	return &PipelineRequest{
		done: make(chan error, 1),
	}
}

// MarkDone sets the repsone for current request.
func (pr *PipelineRequest) MarkDone(reply *proto.Response, err error) {
	if err != nil {
		pr.reply = nil
	}
	pr.reply = reply
	pr.done <- errors.Trace(err)
}

// GetTS gets the timestamp.
func (pr *PipelineRequest) GetTS() (*proto.Timestamp, error) {
	err := <-pr.done
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pr.reply.Timestamp, nil
}

// NewClient creates a timestamp oracle client.
func NewClient(conf *Conf) *Client {
	c := &Client{
		requests: make(chan *PipelineRequest, maxPipelineRequest),
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
		e.Value.(*PipelineRequest).MarkDone(nil, err)
	}
}

func (c *Client) notifyOne(reply *proto.Response) {
	e := c.pending.Front()
	c.pending.Remove(e)
	req := e.Value.(*PipelineRequest)
	req.MarkDone(reply, nil)
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
		err := resp.Decode(session)
		if err != nil {
			return errors.Trace(err)
		}
		c.notifyOne(&resp)
	}

	return nil
}

func (c *Client) do() error {
	session, err := NewConnection(c.conf.ServerAddr, time.Duration(1*time.Second))
	if err != nil {
		return errors.Trace(err)
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
				return errors.Trace(err)
			}
			err = c.handleResponse(session)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (c *Client) workerLoop() {
	for {
		err := c.do()
		if err != nil {
			c.cleanupPending(err)
		}
		time.Sleep(time.Second)
	}
}

// GoGetTimestamp returns a PipelineRequest so you can get the timestamp later.
func (c *Client) GoGetTimestamp() *PipelineRequest {
	pr := newPipelineRequest()
	c.requests <- pr
	return pr
}
