// Package message provides utilities to send go objects over the network
// and process go objects received from the network.
package message

import (
	"encoding/gob"
	"fmt"
	"net"
	"reflect"
	"sync"
)

// Receiver is bound to a local address (or more precisely, port number)
// and contains handlers for a set of types.
type Receiver struct {
	localAddr *net.TCPAddr
	addr      string
	handler   func(string, interface{})
	quit      chan struct{}
	wg        *sync.WaitGroup

	types map[reflect.Type]struct{}
	rw    sync.RWMutex
}

// NewReceiver creates a new instance of Receiver
func NewReceiver(port int, handler func(string, interface{})) (*Receiver, error) {
	laddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	return &Receiver{
		localAddr: laddr,
		handler:   handler,
		types:     make(map[reflect.Type]struct{}),
	}, nil
}

// Register records a type so that then receiver will recognize it later
func (r *Receiver) Register(v interface{}) {
	r.rw.Lock()
	gob.Register(v)
	r.types[reflect.TypeOf(v)] = struct{}{}
	r.rw.Unlock()
}

// Addr returns addresss of the receiver
func (r *Receiver) Addr() string {
	return r.addr
}

// Start starts a go routine that listens to incoming messages
// and dispatches them to their registered handlers.
func (r *Receiver) Start() error {
	listener, err := net.ListenTCP("tcp", r.localAddr)
	if err != nil {
		return err
	}
	r.addr = listener.Addr().String()
	r.quit = make(chan struct{}, 1)
	r.wg = new(sync.WaitGroup)
	r.wg.Add(1)
	// start a go routine to listen to connections
	go func() {
		defer listener.Close()
		defer r.wg.Done()
		newConn := make(chan (net.Conn))
		// start a go routine to put new connections into channel newConn
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				newConn <- conn
			}
		}()
		for {
			select {
			case conn := <-newConn:
				go r.handleConnection(conn)
			case <-r.quit:
				return
			}
		}
	}()
	return nil
}

// Stop signals the Receiver to stop and waits until it actually stops
func (r *Receiver) Stop() {
	if r.quit != nil {
		r.quit <- struct{}{}
		r.quit = nil
		r.wg.Wait()
		r.wg = nil
		r.addr = ""
	}
}

func (r *Receiver) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		dec := gob.NewDecoder(conn)
		var msg interface{}
		err := dec.Decode(&msg)
		if err != nil {
			return
		}
		// handle when the type is registered
		r.rw.RLock()
		if _, prs := r.types[reflect.TypeOf(msg)]; prs {
			go r.handler(conn.RemoteAddr().String(), msg)
		}
		r.rw.RUnlock()
	}
}
