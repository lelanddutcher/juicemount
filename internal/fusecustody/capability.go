package fusecustody

import (
	"bufio"
	"errors"
	"io"
	"os/exec"
	"time"
)

type CapabilityProbe struct {
	done               chan struct{}
	event              Event
	readErr, errorWait error
}

func StartCapability(executable string) (*CapabilityProbe, error) {
	if executable == "" {
		return nil, errors.New("missing custody helper")
	}
	cmd := exec.Command(executable, "--fuse-custodian-capability")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &CapabilityProbe{done: make(chan struct{})}
	go func() {
		reader := bufio.NewReaderSize(stdout, maxFrame+1)
		p.readErr = readFrame(reader, &p.event)
		var extra [1]byte
		if n, err := reader.Read(extra[:]); n != 0 || err != io.EOF {
			p.readErr = errors.New("capability emitted trailing output or failed to close")
		}
		p.errorWait = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}
func (p *CapabilityProbe) Settled() bool {
	if p == nil {
		return true
	}
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}
func (p *CapabilityProbe) Wait(timeout time.Duration) error {
	if p == nil {
		return errors.New("missing capability probe")
	}
	if timeout <= 0 {
		return errors.New("capability probe pending")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		if p.readErr != nil || p.errorWait != nil || p.event.Version != protocolVersion || p.event.State != "available" || p.event.Session != "" || p.event.ID != 0 {
			return errors.New("custody capability unsupported or malformed")
		}
		return nil
	case <-timer.C:
		return errors.New("custody capability pending")
	}
}
