package rpc

// Call represents a remote call
type call struct {
	ID           uint64
	Arg          interface{}
	CallerPort   uint16
	CallerAddr   string
	IsPassedCall bool // indicates whether CallerAddr or sender's address should be used
}

// Reply represents a reply to a remote call
type reply struct {
	ID  uint64
	Ret interface{}
}
