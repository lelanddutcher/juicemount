//go:build redisoptionsacceptance

package respproxy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestReplacementCapabilitiesAuthenticatedSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name          string
		birth, engine bool
	}{
		{"off", false, true}, {"on", true, true}, {"no lease policy", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, direct := birthActivationRedis(t)
			p, err := New(Config{Upstream: addr, Principals: principalsForTest(t), Mode: ModeEnforce,
				VolumeDB: testDB, AllowedDBs: []int{0, testDB}, Singleton: true, StageBirth: tc.birth,
				SingletonRenew: time.Hour, ModePoll: time.Hour, Logf: t.Logf})
			if err != nil {
				t.Fatal(err)
			}
			var e *LeaseEngine
			if tc.engine {
				e = NewLeaseEngine(LeaseConfig{Side: p.Side(), VolumeDB: testDB, NoScan: true,
					SweepEvery: time.Hour, FlushEvery: time.Hour, Logf: t.Logf})
				if err := e.Start(); err != nil {
					t.Fatal(err)
				}
				p.SetPolicy(e)
			}
			var served chan error
			t.Cleanup(func() {
				if e != nil {
					e.Close()
				}
				_ = p.Close()
				if served != nil {
					select {
					case err := <-served:
						if err != nil {
							t.Error(err)
						}
					case <-time.After(3 * time.Second):
						t.Error("proxy did not join")
					}
				}
			})
			if err := p.Start(); err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			served = make(chan error, 1)
			go func() { served <- p.Serve(ln) }()
			client := func(principal string, db int) *redis.Client {
				r := redis.NewClient(&redis.Options{Addr: ln.Addr().String(), Username: principal,
					Password: passOf(principal), DB: db, Protocol: 2, DisableIdentity: true, MaxRetries: -1})
				t.Cleanup(func() { _ = r.Close() })
				return r
			}
			ctx := context.Background()
			mac := client("mac1", testDB)
			ownerBefore, err := direct.Get(ctx, SingletonKey).Result()
			if err != nil || ownerBefore != p.Incarnation() {
				t.Fatalf("singleton before snapshot: %q %v", ownerBefore, err)
			}
			ttlBefore, err := direct.PTTL(ctx, SingletonKey).Result()
			if err != nil || ttlBefore <= 0 {
				t.Fatalf("singleton TTL before snapshot: %v %v", ttlBefore, err)
			}
			v, err := mac.Do(ctx, "JM.LEASE", "REPLACEMENT_CAPABILITIES").Result()
			if err != nil {
				t.Fatal(err)
			}
			a, ok := v.([]any)
			if !ok || len(a) != 5 || a[0] != int64(1) || a[1] != int64(testDB) || a[2] != "mac1" || a[3] != p.Incarnation() || a[4] != int64(0) && a[4] != int64(1) {
				t.Fatalf("invalid typed snapshot: %#v", v)
			}
			if (a[4] == int64(1)) != (tc.birth && tc.engine) {
				t.Fatalf("readiness=%v birth=%t lease-policy=%t", a[4], tc.birth, tc.engine)
			}
			ownerAfter, err := direct.Get(ctx, SingletonKey).Result()
			ttlAfter, ttlErr := direct.PTTL(ctx, SingletonKey).Result()
			if err != nil || ttlErr != nil || ownerAfter != ownerBefore || ttlAfter <= 0 || ttlAfter > ttlBefore {
				t.Fatalf("snapshot mutated singleton: owner=%q ttl=%v errors=%v/%v", ownerAfter, ttlAfter, err, ttlErr)
			}
			for _, tc := range []struct {
				name string
				r    *redis.Client
				args []any
			}{
				{"wrong principal", client("admin1", testDB), []any{"JM.LEASE", "REPLACEMENT_CAPABILITIES"}},
				{"wrong DB", client("mac1", 0), []any{"JM.LEASE", "REPLACEMENT_CAPABILITIES"}},
				{"extra argument", mac, []any{"JM.LEASE", "REPLACEMENT_CAPABILITIES", "unexpected"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if _, err := tc.r.Do(ctx, tc.args...).Result(); err == nil {
						t.Fatal("invalid capability request accepted")
					}
				})
			}
			if tc.birth && tc.engine {
				p.setMode(ModeObserve, "capability test")
				v, err := mac.Do(ctx, "JM.LEASE", "REPLACEMENT_CAPABILITIES").Result()
				if err != nil {
					t.Fatal(err)
				}
				if off, ok := v.([]any); !ok || len(off) != 5 || off[4] != int64(0) {
					t.Fatalf("observe advertised birth: %#v", v)
				}
				p.setMode(ModeEnforce, "capability test")
				if err := direct.Del(ctx, SingletonKey).Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := mac.Do(ctx, "JM.LEASE", "REPLACEMENT_CAPABILITIES").Result(); err == nil {
					t.Fatal("missing singleton advertised stage birth")
				}
			}
		})
	}
}
