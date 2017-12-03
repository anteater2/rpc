package message_test

import (
	"fmt"

	"github.com/anteater2/bitmesh/message"
)

type myStruct struct {
	Field1 string
	Field2 int
}

func Example() {
	r1, _ := message.NewReceiver(8888, func(addr string, v interface{}) {
		fmt.Printf("r1 receives %T: %v\n", v, v)
	})
	r1.Register(0)
	r1.Register(myStruct{})

	r2, _ := message.NewReceiver(8889, func(addr string, v interface{}) {
		fmt.Printf("r2 receives %T: %v\n", v, v)
	})
	r2.Register("")

	r1.Start()
	r2.Start()

	s := message.NewSender()

	s.Register("")
	s.Register(0)
	s.Register(myStruct{})

	fmt.Printf("sends r2 string: %v\n", "a string")
	s.Send("localhost:8889", "a string")

	fmt.Printf("sends r1 int: %v\n", 123)
	s.Send("localhost:8888", 123)

	fmt.Printf("sends r1 message_test.myStruct: %v\n", myStruct{"to r1", 2})
	s.Send("localhost:8888", myStruct{"to r1", 2})

	// myStruct is not registered in r2 so it will not be received
	fmt.Printf("sends r2 message_test.myStruct: %v  (won't be received)\n", myStruct{"to r2", 2})
	s.Send("localhost:8889", myStruct{"to r2", 2})

	r2.Stop()
	r1.Stop()
	// Unordered output:
	// sends r2 string: a string
	// sends r1 int: 123
	// r2 receives string: a string
	// sends r1 message_test.myStruct: {to r1 2}
	// r1 receives int: 123
	// sends r2 message_test.myStruct: {to r2 2}  (won't be received)
	// r1 receives message_test.myStruct: {to r1 2}
}
