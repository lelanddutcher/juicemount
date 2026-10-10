//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type replacementTrackedIO struct {
	replacementPublishIO
	mu             sync.Mutex
	opened, closed int
	closeErrors    []error
}

func (b *replacementTrackedIO) track(f replacementPublishFile, err error) (replacementPublishFile, error) {
	if err != nil {
		return f, err
	}
	b.mu.Lock()
	b.opened++
	b.mu.Unlock()
	return &replacementTrackedFile{replacementPublishFile: f, owner: b}, nil
}
func (b *replacementTrackedIO) Open(n string, create bool) (replacementPublishFile, error) {
	return b.track(b.replacementPublishIO.Open(n, create))
}
func (b *replacementTrackedIO) Create(n string, mode os.FileMode) (replacementPublishFile, error) {
	return b.track(b.replacementPublishIO.Create(n, mode))
}
func (b *replacementTrackedIO) OpenOwned(n string, expected replaceguard.File) (replacementPublishFile, error) {
	return b.track(b.replacementPublishIO.OpenOwned(n, expected))
}
func (b *replacementTrackedIO) assertClosed(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.opened == 0 || b.opened != b.closed || len(b.closeErrors) != 0 {
		t.Fatalf("SDK descriptor custody: opened=%d closed=%d errors=%v", b.opened, b.closed, b.closeErrors)
	}
}

type replacementTrackedFile struct {
	replacementPublishFile
	owner *replacementTrackedIO
}

func (f *replacementTrackedFile) Close() error {
	err := f.replacementPublishFile.Close()
	f.owner.mu.Lock()
	f.owner.closed++
	if err != nil {
		f.owner.closeErrors = append(f.owner.closeErrors, err)
	}
	f.owner.mu.Unlock()
	return err
}

func newReplacementFreshFixture(t *testing.T, body []byte) (*replacementResumeFixture, *replacementRuntime) {
	t.Helper()
	x, spool, d, row, w, _ := replacementNestedFixtureBodyVolume(t, body, "12345678-1234-abcd-5678-123456789abc")
	rt := newReplacementRuntime(t, x, t.TempDir(), false)
	client, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: rt.url, Spool: spool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Stop)
	d.SetProjectLeases(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := x.session.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	return &replacementResumeFixture{x: x, spool: spool, d: d, row: row, proxyURL: rt.url, backend: &replacementSDKIO{fs: rt.fs, m: rt.m, w: *w, ctx: jfsmeta.Background(), parent: 4}}, rt
}
func replacementRedisRunID(t *testing.T, x *boundCacheFixture) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	info, err := x.rdb.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "run_id:"))
		}
	}
	t.Fatal("Redis run_id absent")
	return ""
}
func replaceReplacementRuntime(t *testing.T, f *replacementResumeFixture, old *replacementRuntime) *replacementRuntime {
	t.Helper()
	runID := replacementRedisRunID(t, f.x)
	objectDir, err := os.Stat(old.objectRoot)
	if err != nil {
		t.Fatal(err)
	}
	w := f.backend.w
	f.d.leasesPtr.Load().Stop()
	old.stop(t)
	next := newReplacementRuntime(t, f.x, old.objectRoot, true)
	if next.fs == old.fs || next.m == old.m || next.store == old.store || next.objects == old.objects || next.proxy.Incarnation() == old.proxy.Incarnation() {
		t.Fatal("runtime generation reused")
	}
	now, err := os.Stat(next.objectRoot)
	if err != nil || !os.SameFile(objectDir, now) || runID != replacementRedisRunID(t, f.x) {
		t.Fatal("backing generation changed")
	}
	f.proxyURL = next.url
	f.reopen(t, true)
	f.backend = &replacementSDKIO{fs: next.fs, m: next.m, w: w, ctx: jfsmeta.Background(), parent: 4}
	t.Logf("new SDK/meta/chunk/object clients; joined old proxy; incarnation %s -> %s; Redis run_id unchanged", old.proxy.Incarnation(), next.proxy.Incarnation())
	return next
}

func TestReplacementBornFreshRuntime(t *testing.T) { testReplacementBornUploadReopen(t, true) }

func TestReplacementBornRuntimeLeaseChanges(t *testing.T) {
	for _, which := range []string{"same-owner", "peer-held", "foreign-history", "revoked-session"} {
		t.Run(which, func(t *testing.T) {
			body := bytes.Repeat([]byte("fresh-owner-save-"), 220003)
			f, rt := newReplacementFreshFixture(t, body)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tracked := &replacementTrackedIO{replacementPublishIO: f.backend}
			fault := &replacementInterruptedUploadIO{replacementPublishIO: tracked, at: "after-first-chunk", cancel: cancel}
			factory := func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				return fault, func() {}, nil
			}
			if err := f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, factory, f.binding); !errors.Is(err, context.Canceled) {
				t.Fatalf("initial interruption: %v", err)
			}
			tracked.assertClosed(t)
			before, err := f.spool.meta.ReplacementPublication(f.row)
			if err != nil || before == nil || before.Phase != "intent" {
				t.Fatalf("intent: %+v %v", before, err)
			}
			stage, err := f.backend.Stat(path.Base(before.StagePath))
			if err != nil || stage.Length != 1<<20 {
				t.Fatalf("partial stage: %+v %v", stage, err)
			}
			opKey := replaceguard.BirthOperationKey(f.backend.w.OperationID)
			birth, err := f.x.rdb.Get(context.Background(), opKey).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			born, err := replaceguard.DecodeBirth(birth)
			if err != nil || born.Epoch != 1 {
				t.Fatalf("birth: %+v %v", born, err)
			}
			used, _ := f.spool.Capacity()
			keys := []string{"i3", "c3_0", "d4", "i" + fmt.Sprint(stage.Inode), "c" + fmt.Sprint(stage.Inode) + "_0", opKey, replaceguard.BirthInodeKey(stage.Inode), replaceguard.Key(stage.Inode)}
			snapshot := func() []string {
				t.Helper()
				out := make([]string, len(keys))
				for i, k := range keys {
					v, e := f.x.rdb.Dump(context.Background(), k).Result()
					if e != nil && e != redis.Nil {
						t.Fatal(e)
					}
					out[i] = v
				}
				return out
			}
			remote := snapshot()
			if which == "revoked-session" {
				revokeReplacementRuntimeOwner(t, rt)
			} else {
				released, err := f.d.leasesPtr.Load().releaseNow(context.Background(), "media/project")
				if err != nil || released != "released" {
					t.Fatalf("release owner: %s %v", released, err)
				}
				if which != "same-owner" {
					u, err := url.Parse(rt.url)
					if err != nil {
						t.Fatal(err)
					}
					u.User = url.UserPassword("mac-peer", "mac-secret")
					peer, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: u.String()})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(peer.Stop)
					grant, err := peer.EnsureForDrain("media/project")
					if err != nil || !grant.held || grant.epoch != 3 {
						t.Fatalf("peer tenure: %+v %v", grant, err)
					}
					if which == "foreign-history" {
						release, err := peer.releaseNow(context.Background(), "media/project")
						if err != nil || release != "released" {
							t.Fatalf("peer release: %s %v", release, err)
						}
					}
					peer.Stop()
				}
				rt = replaceReplacementRuntime(t, f, rt)
			}
			resumedCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			err = f.run(resumedCtx)
			if which == "same-owner" {
				if err != nil {
					t.Fatalf("same-owner resume: %v", err)
				}
				destination, err := f.backend.Stat("clip.mov")
				if err != nil || destination.Inode != stage.Inode || f.backend.renames != 1 || cleanupPhase(t, f.spool, f.row.ID) != metadata.ReplacementPublicationCleaned {
					t.Fatalf("publication: %+v %v", destination, err)
				}
				final, err := f.d.leasesPtr.Load().replacementRead(context.Background(), stage.Inode)
				if err != nil || final == nil || final.Epoch != 3 || final.Proxy == born.Proxy {
					t.Fatalf("new authority: %+v %v", final, err)
				}
				actual, err := f.x.rdb.Get(context.Background(), opKey).Bytes()
				if err != nil || !bytes.Equal(birth, actual) {
					t.Fatal("birth refreshed")
				}
				assertReplacementOfflineNFS(t, f.x, f.spool, f.row, body, used-f.row.Size, false)
				return
			}
			if err == nil {
				t.Fatal("forbidden resume accepted")
			}
			if !reflect.DeepEqual(remote, snapshot()) {
				t.Fatal("forbidden recovery changed remote custody")
			}
			after, err := f.spool.meta.ReplacementPublication(f.row)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("journal changed: %+v %v", after, err)
			}
			data, err := os.ReadFile(f.row.SpoolFile)
			if err != nil || !bytes.Equal(data, body) {
				t.Fatalf("private bytes changed: %v", err)
			}
			if got, _ := f.spool.Capacity(); got != used || f.backend.renames != 0 {
				t.Fatal("refusal released bytes or published")
			}
			state, err := f.x.rdb.HGetAll(context.Background(), respproxy.LeaseKeyPrefix+"4").Result()
			if err != nil {
				t.Fatal(err)
			}
			if which == "peer-held" && (state["holder"] != "mac-peer" || state["epoch"] != "3") {
				t.Fatalf("peer lease disturbed: %v", state)
			}
			if which == "foreign-history" {
				f.d.leasesPtr.Load().mu.Lock()
				decision := f.d.leasesPtr.Load().projects["media/project"]
				epoch, foreign := decision.epoch, decision.foreignBefore
				f.d.leasesPtr.Load().mu.Unlock()
				if epoch != 5 || foreign != 5 {
					t.Fatalf("fresh client missed peer history: epoch=%d foreign=%d", epoch, foreign)
				}
			}
			assertReplacementOfflineNFSRoute(t, f.x, f.spool, f.row, body, used, true, true)
		})
	}
}

func revokeReplacementRuntimeOwner(t *testing.T, rt *replacementRuntime) {
	t.Helper()
	u, err := url.Parse(rt.url)
	if err != nil {
		t.Fatal(err)
	}
	dial := func(user string) (net.Conn, *bufio.Reader, *bufio.Writer) {
		t.Helper()
		c, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		r, w := bufio.NewReader(c), bufio.NewWriter(c)
		if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := respproxy.WriteCommand(w, [][]byte{[]byte("AUTH"), []byte(user), []byte("mac-secret")}); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		v, err := respproxy.ReadValue(r)
		if err != nil || v.Err() != nil || string(v.Str) != "OK" {
			t.Fatalf("AUTH: %+v %v", v, err)
		}
		return c, r, w
	}
	target, tr, tw := dial("mac-test")
	peer, pr, pw := dial("mac-peer")
	next, err := respproxy.ParsePrincipals([]byte(fmt.Sprintf(`{"principals":[{"name":"mac-peer","kind":"mac","host":"peer-host","argon2id":%q}]}`, rt.authHash)))
	if err != nil {
		t.Fatal(err)
	}
	rt.proxy.Auth.Swap(next)
	ping := func(c net.Conn, r *bufio.Reader, w *bufio.Writer) (respproxy.Value, error) {
		if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			return respproxy.Value{}, err
		}
		err := respproxy.WriteCommand(w, [][]byte{[]byte("PING")})
		if err == nil {
			err = w.Flush()
		}
		if err != nil {
			return respproxy.Value{}, err
		}
		return respproxy.ReadValue(r)
	}
	_, err = ping(target, tr, tw)
	if err == nil {
		t.Fatal("revoked existing session survived")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("revoked session timed out, closure unproven: %v", err)
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("revocation transport: %v", err)
	}
	v, err := ping(peer, pr, pw)
	if err != nil || v.Err() != nil || string(v.Str) != "PONG" {
		t.Fatalf("unrelated peer session: %+v %v", v, err)
	}
}
