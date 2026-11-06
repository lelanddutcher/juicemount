//go:build linux

package farmqueue

import (
	"errors"
	"net"
	"sync"
)

// harnessProxy is a toxiproxy-shaped TCP relay between one harness worker and
// Redis. While it is open it forwards bytes unchanged. A partition stops both
// directions: bytes already read from a socket are held, not lost, and are
// delivered on heal, so a request the worker sent just before the partition
// can reach Redis long after the worker gave up on it (a delayed packet). A
// reset partition instead drops every connection. Close is the worker's
// death: held bytes are discarded and nothing reaches Redis afterwards.
type harnessProxy struct {
	ln   net.Listener
	dial func() (net.Conn, error)

	mu    sync.Mutex
	cond  *sync.Cond
	held  bool
	shut  bool
	pairs map[*harnessProxyPair]struct{}
	// writing counts writes that passed the open check and have not returned,
	// so a partition can wait until nothing more is on its way to Redis.
	writing int
	wg      sync.WaitGroup
}

type harnessProxyPair struct {
	client, upstream net.Conn
}

func newHarnessProxy(dial func() (net.Conn, error)) (*harnessProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &harnessProxy{ln: ln, dial: dial, pairs: map[*harnessProxyPair]struct{}{}}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

func (p *harnessProxy) Addr() string { return p.ln.Addr().String() }

func (p *harnessProxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.serve(client)
	}
}

func (p *harnessProxy) serve(client net.Conn) {
	defer p.wg.Done()
	// A connection accepted during a partition hangs like a blackholed SYN
	// until the partition heals or the worker dies.
	p.mu.Lock()
	for p.held && !p.shut {
		p.cond.Wait()
	}
	shut := p.shut
	p.mu.Unlock()
	if shut {
		_ = client.Close()
		return
	}
	upstream, err := p.dial()
	if err != nil {
		_ = client.Close()
		return
	}
	pair := &harnessProxyPair{client: client, upstream: upstream}
	p.mu.Lock()
	if p.shut {
		p.mu.Unlock()
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	p.pairs[pair] = struct{}{}
	p.mu.Unlock()
	var pumps sync.WaitGroup
	pumps.Add(2)
	go func() { defer pumps.Done(); p.pump(upstream, client) }()
	go func() { defer pumps.Done(); p.pump(client, upstream) }()
	pumps.Wait()
	_ = client.Close()
	_ = upstream.Close()
	p.mu.Lock()
	delete(p.pairs, pair)
	p.mu.Unlock()
}

// pump copies src to dst, holding each chunk while the proxy is partitioned.
// When src ends, dst's write side is closed so the peer still processes what
// was delivered before it sees the end of the stream.
func (p *harnessProxy) pump(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if !p.beginWrite() {
				_ = dst.Close()
				_ = src.Close()
				return
			}
			_, writeErr := dst.Write(buf[:n])
			p.endWrite()
			if writeErr != nil {
				_ = src.Close()
				return
			}
		}
		if readErr != nil {
			if half, ok := dst.(interface{ CloseWrite() error }); ok && !errors.Is(readErr, net.ErrClosed) {
				_ = half.CloseWrite()
			} else {
				_ = dst.Close()
			}
			return
		}
	}
}

func (p *harnessProxy) beginWrite() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.held && !p.shut {
		p.cond.Wait()
	}
	if p.shut {
		return false
	}
	p.writing++
	return true
}

func (p *harnessProxy) endWrite() {
	p.mu.Lock()
	p.writing--
	p.cond.Broadcast()
	p.mu.Unlock()
}

// Partition stops forwarding and returns once no write is still in flight,
// so everything already sent has reached the peer's socket. With reset, every
// live connection is dropped as well. It reports false if the proxy was
// already partitioned or closed.
func (p *harnessProxy) Partition(reset bool) bool {
	p.mu.Lock()
	if p.held || p.shut {
		p.mu.Unlock()
		return false
	}
	p.held = true
	for p.writing > 0 {
		p.cond.Wait()
	}
	var drop []*harnessProxyPair
	if reset {
		for pair := range p.pairs {
			drop = append(drop, pair)
		}
	}
	p.mu.Unlock()
	for _, pair := range drop {
		if tcp, ok := pair.client.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = pair.client.Close()
		_ = pair.upstream.Close()
	}
	return true
}

// Heal resumes forwarding, delivering any held bytes first.
func (p *harnessProxy) Heal() {
	p.mu.Lock()
	p.held = false
	p.cond.Broadcast()
	p.mu.Unlock()
}

// Close kills the relay: held bytes are dropped, every connection is closed
// and no new one is accepted. It waits for every relay goroutine to finish.
func (p *harnessProxy) Close() {
	p.mu.Lock()
	if p.shut {
		p.mu.Unlock()
		p.wg.Wait()
		return
	}
	p.shut = true
	p.cond.Broadcast()
	pairs := make([]*harnessProxyPair, 0, len(p.pairs))
	for pair := range p.pairs {
		pairs = append(pairs, pair)
	}
	p.mu.Unlock()
	_ = p.ln.Close()
	for _, pair := range pairs {
		if tcp, ok := pair.client.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = pair.client.Close()
		_ = pair.upstream.Close()
	}
	p.wg.Wait()
}
