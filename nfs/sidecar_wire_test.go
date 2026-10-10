//go:build latency_acceptance

package nfs

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// Real TCP/XDR READs exercise onRead's size clamp. There is no kernel mount,
// real network latency or Finder timing claim in this fixture.
func TestStrongSidecarOversizedWireRead(t *testing.T) {
	for _, scenario := range []string{"nil-reader", "disabled-reader", "invalid-before-reply", "invalid-before-size", "stale-low", "failed-open", "cold-small", "cold-large", "cold-exact", "cold-invalid-before-reply", "warm-exact", "warm-prefix", "warm-tail", "warm-at-end", "warm-past-end", "warm-zero-at-end", "warm-large-offset", "warm-small-large-offset"} {
		t.Run(scenario, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			h := jfs.handler
			const rel = "._movie.mov"
			body := bytes.Repeat([]byte("sidecar-body-"), 700)
			seedStrongSidecar(t, jfs, store, root, rel, body)
			if scenario == "disabled-reader" {
				h.SetCacheReader(cache.NewReader(t.TempDir(), cache.DefaultBlockSize, nil))
				old := cacheReaderServeEnabled
				cacheReaderServeEnabled = false
				t.Cleanup(func() { cacheReaderServeEnabled = old })
			}
			cold := strings.HasPrefix(scenario, "cold-")
			strong := scenario == "nil-reader" || scenario == "disabled-reader" || scenario == "invalid-before-reply" || scenario == "invalid-before-size" || strings.HasPrefix(scenario, "warm-")
			if strong {
				if !h.warmSidecar(rel, filepath.Join(root, rel)) || h.strongSidecarRead(rel) == nil {
					t.Fatal("warm strong body")
				}
				// Any attempted fallback open/fstat/read now fails. Only a pinned body
				// can produce the exact successful response below.
				if err := os.Remove(filepath.Join(root, rel)); err != nil {
					t.Fatal(err)
				}
			} else if !cold {
				e := store.LookupByPath(rel)
				e.Size = 4
				if err := store.Insert(e); err != nil {
					t.Fatal(err)
				}
				// Force the existing direct-reader lazy-open lane without providing a hit.
				h.SetCacheReader(cache.NewReader(t.TempDir(), cache.DefaultBlockSize, nil))
				if scenario == "failed-open" {
					if err := os.Remove(filepath.Join(root, rel)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if scenario == "invalid-before-reply" || scenario == "invalid-before-size" || scenario == "cold-invalid-before-reply" {
				calls := 0
				h.testReadCachedInfo = func(name string) {
					if name != rel {
						return
					}
					calls++
					// First call is sizing, second is post-op attrs after the body read.
					if ((scenario == "invalid-before-reply" || scenario == "cold-invalid-before-reply") && calls == 2) || (scenario == "invalid-before-size" && calls == 1) {
						h.incActiveWriter(rel)
						h.decActiveWriter(rel)
					}
				}
			}
			server := nfslib.NewManagedServer(h)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
			if err != nil {
				listener.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				conn.Close()
				listener.Close()
				if !server.Quiesce(3 * time.Second) {
					t.Error("server did not quiesce")
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("Serve did not stop")
				}
			})
			client := newLatencyClient(conn)
			iterations := 1
			if cold {
				iterations = 2
			}
			for iteration := 0; iteration < iterations; iteration++ {
				requestCount := uint32(1 << 20)
				var offset uint64
				want, wantEOF := body, uint32(1)
				if iteration == 0 && scenario == "cold-small" {
					requestCount = 16 << 10
				}
				if iteration == 0 && scenario == "cold-exact" {
					requestCount = uint32(len(body))
				}
				switch scenario {
				case "warm-exact":
					requestCount = uint32(len(body))
				case "warm-prefix":
					requestCount, want, wantEOF = 4, body[:4], 0
				case "warm-tail":
					offset, requestCount, want = uint64(len(body)-4), 4, body[len(body)-4:]
				case "warm-at-end", "warm-zero-at-end":
					offset, want = uint64(len(body)), nil
					if scenario == "warm-zero-at-end" {
						requestCount = 0
					}
				case "warm-past-end":
					offset, want = uint64(len(body)+4096), nil
				case "warm-large-offset", "warm-small-large-offset":
					offset, want = math.MaxUint64, nil
					if scenario == "warm-small-large-offset" {
						requestCount = 1
					}
				}
				pending, err := client.send(100003, 6, latencyReadArgs{Handle: h.ToHandle(nil, []string{rel}), Offset: offset, Count: requestCount})
				if err != nil {
					t.Fatal(err)
				}
				reply, err := pending.await(time.Now().Add(3 * time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if len(reply.body) < 4 {
					t.Fatal("short reply")
				}
				status := binary.BigEndian.Uint32(reply.body[:4])
				if scenario == "invalid-before-reply" || scenario == "invalid-before-size" || scenario == "cold-invalid-before-reply" {
					if status != uint32(nfslib.NFSStatusJukebox) {
						t.Fatalf("status=%d want retry", status)
					}
					return
				}
				if scenario == "failed-open" {
					if status == 0 {
						t.Fatal("missing backend returned success with stale-low clamp")
					}
					return
				}
				reader, err := latencyOK(reply)
				if err != nil {
					t.Fatal(err)
				}
				if err := latencyPostAttr(reader); err != nil {
					t.Fatal(err)
				}
				count, err := xdr.ReadUint32(reader)
				if err != nil {
					t.Fatal(err)
				}
				eof, err := xdr.ReadUint32(reader)
				if err != nil {
					t.Fatal(err)
				}
				data, err := latencyOpaque(reader)
				if err != nil || int(count) != len(want) || eof != wantEOF || !bytes.Equal(data, want) || reader.Len() != 0 {
					t.Fatalf("READ count=%d want=%d eof=%d want=%d err=%v trailing=%d", count, len(want), eof, wantEOF, err, reader.Len())
				}
				t.Logf("actual READ request=%d response=%d source=%s iteration=%d", requestCount, count, scenario, iteration)
				if cold && iteration == 0 {
					if h.strongSidecarRead(rel) == nil {
						t.Fatal("cold READ did not establish strong proof")
					}
					h.fdPool.Invalidate(filepath.Join(root, rel))
					if err := os.Remove(filepath.Join(root, rel)); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestSpoolWireReadDoesNotInventTerminalEOF(t *testing.T) {
	jfs, spool, _, _ := newSpoolWiredHandlerNoDrain(t)
	const rel = "arriving.mov"
	w, err := jfs.Create(rel)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	const size = 1 << 20
	if err := w.Truncate(size); err != nil {
		t.Fatal(err)
	}
	prefix := bytes.Repeat([]byte{'x'}, 4096)
	if _, err := w.(io.WriterAt).WriteAt(prefix, 0); err != nil {
		t.Fatal(err)
	}
	e, ok := spool.LookupActive(rel)
	if !ok || e.ContiguousEnd() != int64(len(prefix)) || e.WrittenEnd() != size {
		t.Fatal("fixture lacks active spool hole")
	}
	c, err := net.DialTimeout("tcp", bindingRPCAddress(t, jfs.handler), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	client := newLatencyClient(c)
	for _, tc := range []struct {
		name   string
		offset uint64
		want   []byte
		status uint32
		count  uint32
		eof    uint32
	}{
		{"readable-prefix", 0, prefix, 0, 2 << 20, 0},
		{"inflight-hole", 4096, nil, uint32(nfslib.NFSStatusJukebox), 2 << 20, 0},
		{"written-high-water-wide", size, nil, 0, 2 << 20, 1},
		{"written-high-water-small", size, nil, 0, 4, 1},
		{"past-high-water-wide", size + 4096, nil, 0, 2 << 20, 1},
		{"past-high-water-small", size + 4096, nil, 0, 4, 1},
		{"zero-count-is-not-eof-proof", size, nil, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pending, err := client.send(100003, 6, latencyReadArgs{Handle: jfs.handler.ToHandle(jfs, []string{rel}), Offset: tc.offset, Count: tc.count})
			if err != nil {
				t.Fatal(err)
			}
			reply, err := pending.await(time.Now().Add(3 * time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if len(reply.body) < 4 {
				t.Fatal("short reply")
			}
			if status := binary.BigEndian.Uint32(reply.body[:4]); status != tc.status {
				t.Fatalf("status=%d want=%d", status, tc.status)
			}
			if tc.status != 0 {
				return
			}
			r, err := latencyOK(reply)
			if err != nil {
				t.Fatal(err)
			}
			if err := latencyPostAttr(r); err != nil {
				t.Fatal(err)
			}
			count, err := xdr.ReadUint32(r)
			if err != nil {
				t.Fatal(err)
			}
			eof, err := xdr.ReadUint32(r)
			if err != nil {
				t.Fatal(err)
			}
			data, err := latencyOpaque(r)
			if err != nil || count != uint32(len(tc.want)) || eof != tc.eof || !bytes.Equal(data, tc.want) || r.Len() != 0 {
				t.Fatalf("READ count=%d EOF=%d bytes=%d err=%v; want count=%d EOF=%d", count, eof, len(data), err, len(tc.want), tc.eof)
			}
		})
	}
}
