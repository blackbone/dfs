package node

import (
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type streamLayer struct {
	net.Listener
}

func (s *streamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout(networkTCP, string(address), timeout)
}
