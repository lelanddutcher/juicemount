package main

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestVolumeOwnerRoutineProbeDoesNotConsumeOperationTable(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config, o.logical, o.link = "selected", "redis://remote:6379", &jmnfs.LinkNode{}
	o.probeWork = func(_ *jmnfs.LinkNode, _ string, _ time.Duration, startup bool) (time.Duration, float64, float64, error) {
		if startup {
			return 0, 0, 0, errors.New("unexpected bulk benchmark")
		}
		return 450 * time.Microsecond, 0, 0, nil
	}
	request := servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
		Method: "probe", Strings: []string{o.instance, o.config, o.root}, Ints: []int{1000}}
	for i := 0; i < 130; i++ {
		resp := o.dispatch(request)
		if resp.Error != "" || len(o.ops) != 0 || !containsProbeNanos(resp.Text, 450000) {
			t.Fatalf("routine probe %d lost remote RTT or exhausted operation table: %s %s", i, resp.Error, resp.Text)
		}
	}
	request.Strings[1] = "foreign"
	if resp := o.dispatch(request); resp.Error == "" {
		t.Fatal("foreign probe accepted")
	}
	request.Strings[1] = o.config
	request.Strings[2] = "foreign-root"
	if resp := o.dispatch(request); resp.Error == "" {
		t.Fatal("foreign isolation accepted")
	}
	o.probeWork = func(*jmnfs.LinkNode, string, time.Duration, bool) (time.Duration, float64, float64, error) {
		return 0, 0, 0, errors.New("remote Redis refused")
	}
	request.Strings[2] = o.root
	if resp := o.dispatch(request); resp.Error == "" {
		t.Fatal("remote probe failure claimed backend up")
	}
}

func containsProbeNanos(raw string, nanos int64) bool {
	var r volumeOwnerReceipt
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return false
	}
	return r.BackendUp && r.ProbeRTTNanos == nanos
}

func TestVolumeOwnerHeldProbeRetainsLinkThroughStop(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config, o.logical, o.link = "selected", "redis://remote:6379", &jmnfs.LinkNode{}
	m := &heldOwnerManager{stopState: health.FUSEStopped}
	o.manager = m
	var released atomic.Int32
	o.releaseLink = func() { released.Add(1) }
	entered, release := make(chan struct{}), make(chan struct{})
	o.probeWork = func(*jmnfs.LinkNode, string, time.Duration, bool) (time.Duration, float64, float64, error) {
		close(entered)
		<-release
		return time.Millisecond, 0, 0, nil
	}
	done := make(chan struct{})
	result := make(chan volumeOwnerReceipt, 1)
	go func() { r, _ := o.probe(time.Second, false); result <- r; close(done) }()
	var once sync.Once
	joined := false
	defer func() {
		once.Do(func() { close(release) })
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("original probe not joined")
			}
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("remote probe did not enter")
	}
	if _, err := o.stop(); err == nil || released.Load() != 0 || o.link == nil {
		t.Fatalf("Stop released Link while real probe was held: %v", err)
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
		joined = true
	case <-time.After(time.Second):
		t.Fatal("probe not joined")
	}
	if r := <-result; r.BackendUp {
		t.Fatal("held probe promoted stale backend after Stop admission")
	}
	if _, err := o.stop(); err != nil || released.Load() != 1 {
		t.Fatalf("joined probe prevented final Stop: %v", err)
	}
}

func TestVolumeOwnerStartupProbeRetainsOneOriginalID(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config, o.logical, o.link = "selected", "redis://remote:6379", &jmnfs.LinkNode{}
	entered, release := make(chan struct{}), make(chan struct{})
	var count atomic.Int32
	var enteredOnce sync.Once
	o.probeWork = func(_ *jmnfs.LinkNode, _ string, _ time.Duration, startup bool) (time.Duration, float64, float64, error) {
		if !startup {
			return 0, 0, 0, errors.New("wrong probe")
		}
		count.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		return time.Millisecond, 1, 2, nil
	}
	const id = "abababababababababababababababab"
	request := func(id string) servicecontrol.Request {
		return servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
			Method: "probe-startup", Strings: []string{o.instance, id, o.config}}
	}
	resp := o.dispatch(request(id))
	if resp.Error != "" {
		t.Fatal(resp.Error)
	}
	var once sync.Once
	defer func() {
		once.Do(func() { close(release) })
		o.mu.Lock()
		var originals []*volumeOwnerOperation
		for _, op := range o.ops {
			if op.method == "probe-startup" {
				originals = append(originals, op)
			}
		}
		o.mu.Unlock()
		for _, op := range originals {
			select {
			case <-op.done:
			case <-time.After(time.Second):
				t.Error("original startup probe not joined")
			}
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("original startup probe did not enter")
	}
	if same := o.dispatch(request(id)); same.Error != "" {
		t.Fatal("same original ID was refused")
	}
	if other := o.dispatch(request("cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")); other.Error == "" {
		t.Fatal("second startup benchmark admitted")
	}
	if count.Load() != 1 || len(o.ops) != 1 {
		t.Fatal("more than one physical startup probe ran")
	}
	once.Do(func() { close(release) })
	o.mu.Lock()
	op := o.ops[id]
	o.mu.Unlock()
	select {
	case <-op.done:
	case <-time.After(time.Second):
		t.Fatal("original probe not joined")
	}
	if replay := o.dispatch(request(id)); replay.Error != "" || !containsProbeNanos(replay.Text, int64(time.Millisecond)) {
		t.Fatal("completed startup proof was not queryable under original ID")
	}
}
