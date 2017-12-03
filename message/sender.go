package message

import (
	"encoding/gob"
	"fmt"
	"net"
	"reflect"
	"sync"
)

// Sender sends data of a particular set of types
type Sender struct {
	types map[reflect.Type]struct{}
	mutex sync.Mutex
}

// NewSender creates a new instance of Sender
func NewSender() *Sender {
	return &Sender{types: make(map[reflect.Type]struct{})}
}

// Register records a type so that Sender can send it
func (s *Sender) Register(v interface{}) {
	s.mutex.Lock()
	gob.Register(v)
	s.types[reflect.TypeOf(v)] = struct{}{}
	s.mutex.Unlock()
}

// Send encodes the message using gob and sends it to the addr
func (s *Sender) Send(addr string, message interface{}) error {
	if _, prs := s.types[reflect.TypeOf(message)]; !prs {
		return fmt.Errorf("message: unregistered type %T", message)
	}
	remoteAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, remoteAddr)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(conn)
	err = enc.Encode(&message)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}
