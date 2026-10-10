package respproxy

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/redis/go-redis/v9"
)

func refreshReplacement(t *testing.T, tp *testProxy, request, expected replaceguard.Record) replaceguard.Record {
	t.Helper()
	raw, err := replaceguard.Encode(request)
	if err != nil {
		t.Fatal(err)
	}
	mac := tp.raw(t)
	mac.login("mac1")
	prior, err := replaceguard.Encode(expected)
	if err != nil {
		t.Fatal(err)
	}
	v := mac.do("JM.LEASE", "ARM_REPLACE", string(raw), string(prior))
	if v.Type != '$' || v.Null {
		t.Fatalf("refresh not acknowledged: %+v", v)
	}
	got, err := replaceguard.Decode(v.Str)
	if err != nil {
		t.Fatal(err)
	}
	if got.Owner != "mac1" || got.Proxy != tp.Incarnation() {
		t.Fatalf("untrusted authority in refresh: %+v", got)
	}
	return got
}

func refuseRefresh(t *testing.T, tp *testProxy, request replaceguard.Record) {
	t.Helper()
	raw, err := replaceguard.Encode(request)
	if err != nil {
		t.Fatal(err)
	}
	mac := tp.raw(t)
	mac.login("mac1")
	if v := mac.do("JM.LEASE", "ARM_REPLACE", string(raw)); v.Type != '-' {
		t.Fatalf("invalid refresh accepted: %+v", v)
	}
}

func replaceLeaseEpoch(t *testing.T, r *replacementRig) uint64 {
	t.Helper()
	mac := r.conn(t, "mac1")
	if v := mac.do("JM.LEASE", "RELEASE", "ino:3", "1"); v.Type != '*' ||
		len(v.Elems) < 2 || string(v.Elems[0].Str) != "RELEASED" || v.Elems[1].Int != 2 {
		t.Fatalf("release original tenure: %+v", v)
	}
	if v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' ||
		len(v.Elems) < 2 || string(v.Elems[0].Str) != "GRANTED" || v.Elems[1].Int != 3 {
		t.Fatalf("same owner new tenure: %+v", v)
	}
	return 3
}

func assertReplacementStored(t *testing.T, r *replacementRig, want replaceguard.Record) {
	t.Helper()
	raw, err := r.db.Get(context.Background(), replaceguard.Key(r.rec.Stage.Inode)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	got, err := replaceguard.Decode(raw)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("stored replacement changed: got=%+v want=%+v err=%v", got, want, err)
	}
}

func renameRefreshedReplacement(t *testing.T, tp *testProxy, r *replacementRig, want replaceguard.Record) {
	t.Helper()
	conf := meta.DefaultConf()
	conf.Retries = 0
	conf.MaxDeletes = 0
	m := meta.NewClient("redis://mac1:mac1-pass@"+tp.addr+"/15", conf)
	t.Cleanup(func() { _ = m.Shutdown() })
	if _, err := m.Load(false); err != nil {
		t.Fatal(err)
	}
	var inode meta.Ino
	var attr meta.Attr
	if st := m.Rename(meta.Background(), 3, r.rec.Stage.Name, 3, r.rec.Destination.Name, 0, &inode, &attr); st != 0 || inode != 21 {
		t.Fatalf("actual metadata rename after refresh: %v inode=%d", st, inode)
	}
	want.Phase = "committed"
	assertReplacementStored(t, r, want)
	if r.db.HExists(context.Background(), "d3", r.rec.Stage.Name).Val() ||
		r.db.HGet(context.Background(), "d3", r.rec.Destination.Name).Val() != mkEntry(typeFile, 21) {
		t.Fatal("refresh rename did not publish the stage")
	}
}

func TestReplacementRefreshSameOwnerNewEpochThenRename(t *testing.T) {
	r := startReplacementRig(t)
	r.arm(t)
	original := r.rec
	newEpoch := replaceLeaseEpoch(t, r)
	request := original
	request.Epoch = newEpoch
	got := refreshReplacement(t, r.tp, request, original)
	want := original
	want.Epoch = newEpoch
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refresh changed original evidence: got=%+v want=%+v", got, want)
	}
	assertReplacementStored(t, r, want)
	r.unchanged(t)
	renameRefreshedReplacement(t, r.tp, r, want)
}

// This uses the same owned Redis DB and persisted lease; it deliberately does
// not rebuild the tree or call the setup helper that flushes the test DB.
func restartReplacementProxy(t *testing.T, r *replacementRig) *testProxy {
	t.Helper()
	r.eng.Close()
	singletonCloseJoined(t, r.tp.Proxy)
	sink := &logSink{t: t}
	p, err := New(Config{Upstream: r.tp.cfg.Upstream, Principals: principalsForTest(t), Mode: ModeEnforce,
		VolumeDB: testDB, Singleton: true, ModePoll: time.Hour, Logf: sink.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { singletonCloseJoined(t, p) })
	e := NewLeaseEngine(LeaseConfig{Side: p.Side(), VolumeDB: testDB, Now: r.clk.Now, Logf: sink.Logf,
		NoScan: true, SweepEvery: time.Hour, FlushEvery: time.Hour})
	t.Cleanup(e.Close)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	p.SetPolicy(e)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go p.Serve(ln)
	return &testProxy{Proxy: p, addr: ln.Addr().String(), logs: sink}
}

func TestReplacementRefreshNewProxySameRedisThenRename(t *testing.T) {
	r := startReplacementRig(t)
	r.arm(t)
	original := r.rec
	newProxy := restartReplacementProxy(t, r)
	if newProxy.Incarnation() == original.Proxy {
		t.Fatal("restarted proxy reused singleton incarnation")
	}
	got := refreshReplacement(t, newProxy, original, original)
	want := original
	want.Proxy = newProxy.Incarnation()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restart refresh changed original evidence: got=%+v want=%+v", got, want)
	}
	assertReplacementStored(t, r, want)
	r.unchanged(t)
	renameRefreshedReplacement(t, newProxy, r, want)
}

func TestReplacementRefreshRejectsImmutableEvidenceChanges(t *testing.T) {
	cases := map[string]func(*replaceguard.Record){
		"accepted_epoch":    func(r *replaceguard.Record) { r.AcceptedEpoch = 0 },
		"image":             func(r *replaceguard.Record) { r.ImageSHA256 = strings.Repeat("d", 64) },
		"old_graph":         func(r *replaceguard.Record) { r.GraphDigest = strings.Repeat("d", 64) },
		"stage":             func(r *replaceguard.Record) { r.Stage.Length++ },
		"destination":       func(r *replaceguard.Record) { r.Destination.Length++ },
		"stage_ctime":       func(r *replaceguard.Record) { r.Stage.CtimeNS++ },
		"destination_ctime": func(r *replaceguard.Record) { r.Destination.CtimeNS++ },
		"backend_uuid":      func(r *replaceguard.Record) { r.BackendUUID = "other-volume" },
		"backend_digest":    func(r *replaceguard.Record) { r.BackendDigest = strings.Repeat("d", 64) },
		"parents":           func(r *replaceguard.Record) { r.Parents[1].Name = "Other" },
		"operation":         func(r *replaceguard.Record) { r.OperationID = strings.Repeat("d", 32) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			r := startReplacementRig(t)
			r.arm(t)
			original := r.rec
			newEpoch := replaceLeaseEpoch(t, r)
			request := original
			request.Epoch = newEpoch
			request.Parents = append([]replaceguard.Parent(nil), original.Parents...)
			change(&request)
			refuseRefresh(t, r.tp, request)
			assertReplacementStored(t, r, original)
			r.unchanged(t)
		})
	}
}

func TestReplacementRefreshRejectsForeignTenureSinceOriginalAcceptance(t *testing.T) {
	r := startReplacementRig(t)
	r.arm(t)
	original := r.rec
	mac1, mac2 := r.conn(t, "mac1"), r.conn(t, "mac2")
	if v := mac1.do("JM.LEASE", "RELEASE", "ino:3", "1"); v.Type != '*' || v.Elems[1].Int != 2 {
		t.Fatalf("release: %+v", v)
	}
	if v := mac2.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' || v.Elems[1].Int != 3 {
		t.Fatalf("foreign grant: %+v", v)
	}
	if v := mac2.do("JM.LEASE", "RELEASE", "ino:3", "3"); v.Type != '*' || v.Elems[1].Int != 4 {
		t.Fatalf("foreign release: %+v", v)
	}
	if v := mac1.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' || v.Elems[1].Int != 5 {
		t.Fatalf("owner regained: %+v", v)
	}
	request := original
	request.Epoch = 5
	refuseRefresh(t, r.tp, request)
	assertReplacementStored(t, r, original)
	r.unchanged(t)
}

func TestReplacementRefreshRejectsUnusableRecordsAndPartialStage(t *testing.T) {
	for _, fault := range []string{"malformed", "committed", "regressed-epoch", "foreign-owner", "partial-stage"} {
		t.Run(fault, func(t *testing.T) {
			r := startReplacementRig(t)
			r.arm(t)
			original := r.rec
			newEpoch := replaceLeaseEpoch(t, r)
			ctx := context.Background()
			key := replaceguard.Key(original.Stage.Inode)
			switch fault {
			case "malformed":
				if err := r.db.Set(ctx, key, "{bad", 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "committed", "regressed-epoch", "foreign-owner":
				changed := original
				switch fault {
				case "committed":
					changed.Phase = "committed"
				case "regressed-epoch":
					changed.Epoch = newEpoch + 1
				case "foreign-owner":
					changed.Owner = "mac2"
				}
				raw, err := replaceguard.Encode(changed)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.db.Set(ctx, key, raw, 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "partial-stage":
				if err := r.db.HDel(ctx, "d3", original.Stage.Name).Err(); err != nil {
					t.Fatal(err)
				}
				if err := r.db.HSet(ctx, "d3", original.Destination.Name, mkEntry(typeFile, original.Stage.Inode)).Err(); err != nil {
					t.Fatal(err)
				}
			}
			before := r.db.Get(ctx, key).Val()
			request := original
			request.Epoch = newEpoch
			refuseRefresh(t, r.tp, request)
			if got := r.db.Get(ctx, key).Val(); got != before {
				t.Fatalf("failed refresh modified retained record: %q != %q", got, before)
			}
			if fault == "partial-stage" {
				if r.db.Exists(ctx, key).Val() != 1 || r.db.HExists(ctx, "d3", original.Stage.Name).Val() {
					t.Fatal("partial rename discarded prepared record or restored missing stage")
				}
			} else {
				r.unchanged(t)
			}
		})
	}
}

func TestReplacementRefreshInvalidatesOldWatch(t *testing.T) {
	r := startReplacementRig(t)
	r.arm(t)
	original := r.rec
	request := original
	request.Epoch = replaceLeaseEpoch(t, r)
	ctx := context.Background()
	key := replaceguard.Key(original.Stage.Inode)
	var refreshed replaceguard.Record
	err := r.db.Watch(ctx, func(tx *redis.Tx) error {
		refreshed = refreshReplacement(t, r.tp, request, original)
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "replacement-refresh-watch-probe", "must-abort", 0)
			return nil
		})
		return err
	}, key)
	if !errors.Is(err, redis.TxFailedErr) || r.db.Exists(ctx, "replacement-refresh-watch-probe").Val() != 0 {
		t.Fatalf("old WATCH survived refresh: err=%v", err)
	}
	want := original
	want.Epoch = request.Epoch
	if !reflect.DeepEqual(refreshed, want) {
		t.Fatalf("unexpected refreshed record: %+v", refreshed)
	}
	assertReplacementStored(t, r, want)
}
