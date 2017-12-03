package rpc_test

import (
	"fmt"
	"time"

	"github.com/anteater2/bitmesh/rpc"
)

// argument types
type addArg struct {
	X int
	Y int
}

type mulArg struct {
	X int
	Y int
}

type notifyArg struct {
}

func Example() {
	// setup caller
	caller, _ := rpc.NewCaller(2000)
	// caller needs to declare remote functions
	// and specify argument type, return type and timeout
	add := caller.Declare(addArg{}, 0, time.Second)
	mul := caller.Declare(mulArg{}, 0, time.Second)
	// note that remote function always has a return value,
	// empty struct can be used as a void type
	notify := caller.Declare(notifyArg{}, struct{}{}, time.Second)

	// setup callees
	callee1, _ := rpc.NewCallee(2001)
	callee2, _ := rpc.NewCallee(2002)

	// callee needs to implement remote functions
	callee1.Implement(func(arg addArg) int {
		return arg.X + arg.Y
	})
	// callee1 hands over multiplication to callee2
	callee1.Implement(func(arg mulArg, pass rpc.PassFunc) (int, bool) {
		pass("localhost:2002", arg)
		return 0, false
	})
	callee2.Implement(func(arg mulArg) int {
		return arg.X * arg.Y
	})
	callee2.Implement(func(arg notifyArg) struct{} {
		fmt.Println("callee2: someone just called me!")
		return struct{}{}
	})

	// start
	caller.Start()
	callee1.Start()
	callee2.Start()

	// the function declared is called with (address string, arg interface{})
	// this will be handled by callee1
	res, err := add("localhost:2001", addArg{1, 2})
	if err != nil {
		panic(err)
	}
	sum := res.(int)
	fmt.Printf("1 + 2 = %d\n", sum)

	// this will be passed by callee1 and handled by callee2
	res, err = mul("localhost:2001", mulArg{3, 4})
	if err != nil {
		panic(err)
	}
	prod := res.(int)
	fmt.Printf("3 * 4 = %d\n", prod)

	// this will be handled by callee2
	_, err = notify("localhost:2002", notifyArg{})
	if err != nil {
		panic(err)
	}

	// stop
	caller.Stop()
	callee1.Stop()
	callee2.Stop()

	// Output:
	// 1 + 2 = 3
	// 3 * 4 = 12
	// callee2: someone just called me!
}
