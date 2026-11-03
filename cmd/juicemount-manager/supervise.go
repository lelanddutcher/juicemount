package main

// Restart supervision for the Link components (embedded Headscale and the
// NAS tailnet node). A component that fails to start, or stops after it was
// running, is retried with capped exponential backoff for as long as the
// Manager runs: Link degrades and reports why, but never gives up for good.
// Before this, one failed restart (or one 60 s NAS-node attempt) disabled
// Link until the whole container was restarted.

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

const (
	supervisorMinBackoff = time.Second
	supervisorMaxBackoff = time.Minute
	// A component that stayed healthy this long earns a fresh backoff, so an
	// occasional crash restarts quickly while a crash loop slows to the cap.
	supervisorStableAfter = 2 * time.Minute
)

// errStopped is returned by a run function that ended because the
// supervisor was asked to stop.
var errStopped = errors.New("stopped")

// componentStatus is a mutex-guarded manager.LinkComponentStatus.
type componentStatus struct {
	mu sync.Mutex
	st manager.LinkComponentStatus
}

func newComponentStatus() *componentStatus {
	return &componentStatus{st: manager.LinkComponentStatus{State: "starting", Since: time.Now()}}
}

func (c *componentStatus) set(state, detail string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.st.State != state {
		c.st.Since = time.Now()
	}
	c.st.State = state
	c.st.Detail = detail
	if state == "running" {
		c.st.LastError = ""
		c.st.NextRetry = time.Time{}
	}
}

func (c *componentStatus) fail(err error, retryAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.st.State != "backoff" {
		c.st.Since = time.Now()
	}
	c.st.State = "backoff"
	c.st.LastError = err.Error()
	c.st.Restarts++
	c.st.NextRetry = retryAt
}

func (c *componentStatus) snapshot() *manager.LinkComponentStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.st
	return &st
}

func (c *componentStatus) running() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.st.State == "running"
}

// supervisor runs one component through repeated start attempts.
type supervisor struct {
	name   string
	status *componentStatus
	// run starts the component, calls ready once it is serving, then blocks
	// until it stops (returning why) or stop closes (returning errStopped).
	run func(stop <-chan struct{}, ready func(detail string)) error

	minBackoff, maxBackoff, stableAfter time.Duration
	now                                 func() time.Time

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

func newSupervisor(name string, run func(<-chan struct{}, func(string)) error) *supervisor {
	return &supervisor{
		name:        name,
		status:      newComponentStatus(),
		run:         run,
		minBackoff:  supervisorMinBackoff,
		maxBackoff:  supervisorMaxBackoff,
		stableAfter: supervisorStableAfter,
		now:         time.Now,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
}

func (s *supervisor) Start() { go s.loop() }

// Stop asks the component to stop and waits for its run function to return.
func (s *supervisor) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.done
}

func (s *supervisor) loop() {
	defer close(s.done)
	delay := s.minBackoff
	for {
		s.status.set("starting", "")
		var readyAt atomic.Int64 // unix nanos; zero until ready
		err := s.run(s.stop, func(detail string) {
			readyAt.Store(s.now().UnixNano())
			s.status.set("running", detail)
			log.Printf("JuiceMount Link: %s running: %s", s.name, detail)
		})
		select {
		case <-s.stop:
			s.status.set("stopped", "")
			return
		default:
		}
		if err == nil || errors.Is(err, errStopped) {
			err = errors.New("exited")
		}
		if at := readyAt.Load(); at != 0 && s.now().Sub(time.Unix(0, at)) >= s.stableAfter {
			delay = s.minBackoff
		}
		s.status.fail(err, s.now().Add(delay))
		log.Printf("JuiceMount Link: %s not running (%v); retrying in %s", s.name, err, delay)
		timer := time.NewTimer(delay)
		select {
		case <-s.stop:
			timer.Stop()
			s.status.set("stopped", "")
			return
		case <-timer.C:
		}
		delay *= 2
		if delay > s.maxBackoff {
			delay = s.maxBackoff
		}
	}
}
