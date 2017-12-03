package rpc

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"

	"github.com/anteater2/bitmesh/message"
)

type remoteFuncType int

const (
	alwaysRetrun = iota
	mayReturn    = iota
)

// Callee represents a callee service where remote functions are implemented.
type Callee struct {
	sender   *message.Sender
	receiver *message.Receiver

	functions     map[reflect.Type]interface{}
	functionTypes map[reflect.Type]remoteFuncType
	rw            sync.RWMutex
}

// NewCallee creates a new instance of Callee
func NewCallee(port int) (*Callee, error) {
	var c Callee
	var err error
	c.sender = message.NewSender()
	c.receiver, err = message.NewReceiver(port, func(addr string, v interface{}) {
		c.handleCall(addr, v.(call))
	})
	if err != nil {
		return nil, err
	}
	c.receiver.Register(call{})
	c.functions = make(map[reflect.Type]interface{})
	c.functionTypes = make(map[reflect.Type]remoteFuncType)
	c.sender.Register(call{})
	c.sender.Register(reply{})
	return &c, nil
}

// PassFunc is used to pass the call to another callee.
// When it is called, the same call will be passed to addr with new argument arg.
// The type of arg should not changed, otherwise this function will panic.
type PassFunc func(addr string, arg interface{}) error

// Implement specifies a remote function that is avaiable on this callee.
//
// Suppose the argument type of the remote function is T and the return type is V.
// Then, f must be of one of the following types:
//   func(T) V
//   func(T, pass PassFunc) (V, bool)
//
// For the first type, callee always sends back the return value of f.
//
// For the second type, a PassFunc is provided for f so that f could choose to
// send the call to other callees. In this case, the second return value of f
// should be set to false so that the callee will not send back any value.
// On the other hand, if the second return value of f is true, the return value of f
// will be sent back.
func (c *Callee) Implement(f interface{}) {
	if t, v, ok := checkImplTypeAlwaysReturn(f); ok {
		c.receiver.Register(reflect.Zero(t).Interface())
		c.sender.Register(reflect.Zero(t).Interface())
		c.sender.Register(reflect.Zero(v).Interface())
		c.rw.Lock()
		c.functions[t] = f
		c.functionTypes[t] = alwaysRetrun
		c.rw.Unlock()
		return
	}
	if t, v, ok := checkImplTypeMayReturn(f); ok {
		c.receiver.Register(reflect.Zero(t).Interface())
		c.sender.Register(reflect.Zero(t).Interface())
		c.sender.Register(reflect.Zero(v).Interface())
		c.rw.Lock()
		c.functions[t] = f
		c.functionTypes[t] = mayReturn
		c.rw.Unlock()
		return
	}
	panic(fmt.Sprintf("rpc.Callee.Implement: invalid function type %T", f))
}

// Start starts the Callee
func (c *Callee) Start() error {
	return c.receiver.Start()
}

// Stop stops the Callee
func (c *Callee) Stop() {
	c.receiver.Stop()
}

func (c *Callee) handleCall(addr string, call call) error {
	argValue := reflect.ValueOf(call.Arg)
	argType := argValue.Type()
	var callerAddr string
	if call.IsPassedCall {
		callerAddr = call.CallerAddr
	} else {
		callerAddr = changePort(addr, call.CallerPort)
	}
	c.rw.RLock()
	if f, prs := c.functions[argType]; prs {
		fValue := reflect.ValueOf(f)
		remoteFuncType := c.functionTypes[argType]
		c.rw.RUnlock()
		switch remoteFuncType {
		case alwaysRetrun:
			out := fValue.Call([]reflect.Value{argValue})
			reply := reply{ID: call.ID, Ret: out[0].Interface()}
			return c.sender.Send(callerAddr, reply)
		case mayReturn:
			pass := func(addr string, arg interface{}) error {
				if reflect.TypeOf(arg) != argType {
					panic(fmt.Sprintf(
						"rpc.Callee.PassFunc: bad argument type: %T (expecting %v)",
						arg, argType))
				}
				call.Arg = arg
				call.CallerAddr = callerAddr
				call.IsPassedCall = true
				return c.sender.Send(addr, call)
			}
			out := fValue.Call([]reflect.Value{argValue, reflect.ValueOf(pass)})
			if out[1].Bool() == true {
				reply := reply{ID: call.ID, Ret: out[0].Interface()}
				return c.sender.Send(callerAddr, reply)
			}
			return nil
		default:
			panic("rpc.Callee.handleCall: unknown function type")
		}
	} else {
		c.rw.RUnlock()
		return nil
	}
}

// func(T) V
func checkImplTypeAlwaysReturn(f interface{}) (t reflect.Type, v reflect.Type, ok bool) {
	fType := reflect.TypeOf(f)
	if fType.Kind() != reflect.Func {
		return nil, nil, false
	}
	if fType.NumIn() != 1 || fType.NumOut() != 1 {
		return nil, nil, false
	}
	return fType.In(0), fType.Out(0), true
}

// func(T, pass PassFunc) (V, bool)
func checkImplTypeMayReturn(f interface{}) (t reflect.Type, v reflect.Type, ok bool) {
	fType := reflect.TypeOf(f)
	if fType.Kind() != reflect.Func {
		return nil, nil, false
	}
	if fType.NumIn() != 2 || fType.NumOut() != 2 {
		return nil, nil, false
	}
	var pass PassFunc
	if fType.In(1) != reflect.TypeOf(pass) || fType.Out(1).Kind() != reflect.Bool {
		return nil, nil, false
	}
	return fType.In(0), fType.Out(0), true
}

func changePort(addr string, port int) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}
