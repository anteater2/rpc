package rpc

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/anteater2/bitmesh/message"
)

// Caller represents a caller service where remote functions are declared.
// It sends the call to callee over the network
// and captures the correpsponding return value.
type Caller struct {
	port     int
	sender   *message.Sender
	receiver *message.Receiver

	nextID func() uint64

	retChan map[uint64]chan interface{}
	rw      sync.RWMutex
}

// NewCaller creates a new Caller
func NewCaller(port int) (*Caller, error) {
	var c Caller
	var err error
	c.port = port
	c.retChan = make(map[uint64]chan interface{})
	c.nextID = makeIDGenerator()
	c.sender = message.NewSender()
	c.receiver, err = message.NewReceiver(port, func(addr string, v interface{}) {
		reply := v.(reply)
		c.rw.RLock()
		ret, prs := c.retChan[reply.ID]
		c.rw.RUnlock()
		if prs {
			ret <- reply.Ret
		}
	})
	if err != nil {
		return nil, err
	}
	c.receiver.Register(reply{})
	c.sender.Register(call{})
	return &c, nil
}

// RemoteFunc is the type returned by Declare
type RemoteFunc func(addr string, arg interface{}) (interface{}, error)

// Declare registers a return type and makes a RemoteFunc
// which sends a call to the specified address and block until return or timeout.
// This RemoteFunc will check the type of arg and the type of retuen value.
// If the type of arg does not match, it will panic; if the type of return value
// does not match, it will return an error.
// If Caller does not receive any return value when time is out, an error will return.
func (c *Caller) Declare(arg interface{}, ret interface{}, timeout time.Duration) RemoteFunc {
	c.sender.Register(arg)
	c.receiver.Register(ret)
	argType := reflect.TypeOf(arg)
	retType := reflect.TypeOf(ret)
	return func(addr string, arg interface{}) (interface{}, error) {
		if reflect.TypeOf(arg) != argType {
			panic(fmt.Sprintf("rpc.Caller.RemoteFunc: bad argument type: %T (expecting %v)",
				arg, argType))
		}

		id := c.nextID()

		// prepare a channel to receive return value
		ret := make(chan interface{}, 1)
		c.rw.Lock()
		c.retChan[id] = ret
		c.rw.Unlock()
		defer func() {
			c.rw.Lock()
			delete(c.retChan, id)
			c.rw.Unlock()
		}()

		// send the call
		call := call{ID: id, Arg: arg, CallerPort: c.port, IsPassedCall: false}
		err := c.sender.Send(addr, call)
		if err != nil {
			return nil, err
		}

		// wait for return or timeout
		select {
		case val := <-ret:
			if reflect.TypeOf(val) != retType {
				return nil, fmt.Errorf("bad return type: %T (expecting %v)", val, retType)
			}
			return val, nil
		case <-time.After(timeout):
			return nil, errors.New("time out")
		}
	}
}

// Start starts the caller
func (c *Caller) Start() error {
	return c.receiver.Start()
}

// Stop stops the caller
func (c *Caller) Stop() {
	c.receiver.Stop()
}

func makeIDGenerator() func() uint64 {
	var counter uint64
	return func() uint64 {
		ret := counter
		counter++
		return ret
	}
}
