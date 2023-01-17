package raft

import (
	"fmt"
	"net/rpc"
	"sync"
)

// Server wraps a raft.ConsensusModule along with a rpc.Server that exposes its
// methods as RPC endpoints. It also manages the peers of the Raft server. The
// main goal of this type is to simplify the code of raft.Server for
// presentation purposes. raft.ConsensusModule has a *Server to do its peer
// communication and doesn't have to worry about the specifics of running an
// RPC server.
type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	cm *ConsensusModule

	// Requires mutex to access
	peerClients map[int]*rpc.Client
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		// Return an error if this function is called after shutdown
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}
