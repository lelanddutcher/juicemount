package nfs

import (
	"testing"
)

func TestRPCSemaphoreDefault(t *testing.T) {
	s := &Server{}
	// Before Serve(), semaphore should be nil
	if s.RPCSemaphoreSize() != 0 {
		t.Fatalf("expected 0 before init, got %d", s.RPCSemaphoreSize())
	}

	// Initialize manually (Serve() would do this)
	s.rpcSem = make(chan struct{}, DefaultRPCSemaphoreSize)
	if s.RPCSemaphoreSize() != DefaultRPCSemaphoreSize {
		t.Fatalf("expected %d, got %d", DefaultRPCSemaphoreSize, s.RPCSemaphoreSize())
	}
}

func TestRPCSemaphoreBackpressure(t *testing.T) {
	s := &Server{}
	s.rpcSem = make(chan struct{}, 4) // small for testing

	// Fill the semaphore
	for i := 0; i < 4; i++ {
		s.rpcSem <- struct{}{}
	}

	// 5th acquire should block (non-blocking check)
	select {
	case s.rpcSem <- struct{}{}:
		t.Fatal("semaphore should be full but accepted another")
	default:
		// expected — semaphore is full
	}

	// Release one and verify we can acquire
	<-s.rpcSem
	select {
	case s.rpcSem <- struct{}{}:
		// expected — slot freed
	default:
		t.Fatal("semaphore should have a free slot after release")
	}

	// Drain
	for len(s.rpcSem) > 0 {
		<-s.rpcSem
	}
}

func TestActiveConnectionTracking(t *testing.T) {
	s := &Server{}

	if s.ActiveConnections() != 0 {
		t.Fatalf("expected 0 active connections, got %d", s.ActiveConnections())
	}

	// Simulate connections
	s.activeConns.Add(1)
	s.activeConns.Add(1)
	s.activeConns.Add(1)

	if s.ActiveConnections() != 3 {
		t.Fatalf("expected 3, got %d", s.ActiveConnections())
	}

	// Simulate disconnections
	s.activeConns.Add(-1)
	s.activeConns.Add(-1)

	if s.ActiveConnections() != 1 {
		t.Fatalf("expected 1, got %d", s.ActiveConnections())
	}

	s.activeConns.Add(-1)
	if s.ActiveConnections() != 0 {
		t.Fatalf("expected 0, got %d", s.ActiveConnections())
	}
}

func TestRPCStats(t *testing.T) {
	// Reset counters
	rpcCount.Store(0)
	slowRPCCount.Store(0)
	tcpFlushCount.Store(0)
	tcpBatchedCount.Store(0)

	rpcCount.Add(100)
	slowRPCCount.Add(5)
	tcpFlushCount.Add(80)
	tcpBatchedCount.Add(20)

	total, slow, flushes, batched := RPCStats()
	if total != 100 {
		t.Errorf("expected total=100, got %d", total)
	}
	if slow != 5 {
		t.Errorf("expected slow=5, got %d", slow)
	}
	if flushes != 80 {
		t.Errorf("expected flushes=80, got %d", flushes)
	}
	if batched != 20 {
		t.Errorf("expected batched=20, got %d", batched)
	}
}
