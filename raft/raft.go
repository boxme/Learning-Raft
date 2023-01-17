package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

const DebugCM = 1

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

// ConsensusModule is a single node of Raft consensus
type ConsensusModule struct {
	mu sync.Mutex

	id int

	peerIds []int

	server *Server

	// Raft state
	state              CMState
	electionResetEvent time.Time

	// Persistent Raft state
	currentTerm int
	votedFor    int
}

// runElectionTimer implements an election timer. It should be launched whenever
// we want to start a timer towards becoming a candidate in a new election.
//
// This function is blocking and should be launched in a separate goroutine;
// it's designed to work for a single (one-shot) election timer, as it exits
// whenever the CM state changes from follower/candidate or the term changes.
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()

	// This loops until either:
	// - we discover the election timer is no longer needed, or
	// - the election timer expires and this CM becomes a candidate
	// In a follower, this typically keeps running in the background for the
	// duration of the CM's lifetime.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("In election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("In election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// Start an election if nothing is heard from a leader or haven't voted for someone for the duration
		// of the timeout.
		if elapse := time.Since(cm.electionResetEvent); elapse >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// Creates a timeout of value between 150ms to 300ms
func (cm *ConsensusModule) electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}
