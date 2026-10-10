package respproxy

import (
	"context"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestPerfDirectVsProxy measures direct Redis vs the proxy back to back on
// the same machine state, n=6 alternating runs per op. Set
// JM_RESPPROXY_PERF=1 to run (it takes about a minute).
func TestPerfDirectVsProxy(t *testing.T) {
	if os.Getenv("JM_RESPPROXY_PERF") == "" {
		t.Skip("JM_RESPPROXY_PERF not set")
	}
	addr := redisAddr(t, true)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce)
	ctx := context.Background()
	mk := func(a string, auth bool, pool int) *redis.Client {
		o := &redis.Options{Addr: a, DB: testDB, Protocol: 3, PoolSize: pool}
		if auth {
			o.Username, o.Password = "mac1", "mac1-pass"
		}
		c := redis.NewClient(o)
		t.Cleanup(func() { c.Close() })
		return c
	}
	ops := map[string]func(c *redis.Client) error{
		"GET": func(c *redis.Client) error { return c.Get(ctx, "i1").Err() },
		"SET": func(c *redis.Client) error { return c.Set(ctx, "i1", "v", 0).Err() },
		"PIPE100": func(c *redis.Client) error {
			p := c.Pipeline()
			for i := 0; i < 100; i++ {
				p.Get(ctx, "i1")
			}
			_, err := p.Exec(ctx)
			return err
		},
		"MULTI3": func(c *redis.Client) error {
			_, err := c.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, "i1", "v", 0)
				p.HSet(ctx, "d1", "a", "b")
				p.IncrBy(ctx, "nextinode", 1)
				return nil
			})
			return err
		},
	}
	dc, pc := mk(addr, false, 4), mk(tp.addr, true, 4)
	dc.Set(ctx, "i1", "v", 0)
	run := func(c *redis.Client, f func(*redis.Client) error, n int) time.Duration {
		st := time.Now()
		for i := 0; i < n; i++ {
			if err := f(c); err != nil && err != redis.Nil {
				t.Fatal(err)
			}
		}
		return time.Since(st) / time.Duration(n)
	}
	for _, nm := range []string{"GET", "SET", "PIPE100", "MULTI3"} {
		var d, p, diff []time.Duration
		for r := 0; r < 6; r++ {
			a, b := run(dc, ops[nm], 3000), run(pc, ops[nm], 3000)
			d, p, diff = append(d, a), append(p, b), append(diff, b-a)
		}
		srt := func(x []time.Duration) { sort.Slice(x, func(i, j int) bool { return x[i] < x[j] }) }
		srt(d)
		srt(p)
		srt(diff)
		t.Logf("%-8s direct med %v [%v..%v]  proxy med %v [%v..%v]  paired added med %v [%v..%v]",
			nm, d[3], d[0], d[5], p[3], p[0], p[5], diff[3], diff[0], diff[5])
	}
	for _, cfg := range []struct {
		name string
		c    *redis.Client
	}{{"direct", mk(addr, false, 200)}, {"proxy", mk(tp.addr, true, 200)}} {
		var tot []float64
		for r := 0; r < 6; r++ {
			var wg sync.WaitGroup
			st := time.Now()
			for g := 0; g < 200; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 500; i++ {
						cfg.c.Get(ctx, "i1")
					}
				}()
			}
			wg.Wait()
			tot = append(tot, float64(200*500)/time.Since(st).Seconds())
		}
		sort.Float64s(tot)
		t.Logf("200 conns GET %-6s: median %.0f ops/s [%.0f..%.0f]", cfg.name, tot[3], tot[0], tot[5])
	}
}
