//go:build latency_acceptance

package nfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// This is the ordinary active-spool WRITE opener under a finite wire burst.
// It does not hold SQL, stall an opener, alter the production waiter gate, or require FUSE.
func TestContinuationWireBudgetNormalOpen(t *testing.T) {
	if budget := nfslib.DefaultRPCSemaphoreSize - nfslib.WriteRPCReserve; budget != 96 {
		t.Fatalf("managed WRITE budget changed: %d", budget)
	}
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_DRAIN_BATCH_INSERT", "0")
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "") // use the normal selected deadline
	tmpRoot := os.Getenv("TMPDIR")
	if !filepath.IsAbs(tmpRoot) {
		t.Fatal("isolated absolute TMPDIR required")
	}
	base, err := os.MkdirTemp(tmpRoot, "jm-wire-budget-")
	if err != nil {
		t.Fatal(err)
	}
	fullCleanupRegistered := false
	t.Cleanup(func() {
		if !fullCleanupRegistered { // setup failed before handler ownership existed
			if err := os.RemoveAll(base); err != nil {
				t.Errorf("remove unused fixture: %v", err)
			}
		}
	})
	backing := filepath.Join(base, "backing")
	if err := os.Mkdir(backing, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(base, "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, backing)
	type rpcCall struct {
		done         chan struct{}
		conn         net.Conn
		slot         int
		body         []byte
		status       uint32
		err          error
		began, ended time.Time
	}
	var calls []*rpcCall
	var sockets []net.Conn
	var listener net.Listener
	var server *nfslib.Server
	var serveDone chan error
	var spool *SpoolStore
	spoolOwnedByHandler := false
	// Registered before any client launch. Unknown original work keeps DB/handler
	// alive; a failed assertion never grants permission to close live backing.
	t.Cleanup(func() {
		retained := true
		defer func() {
			if retained {
				t.Logf("retained unresolved wire fixture: %s", base)
			}
		}()
		for _, c := range sockets {
			_ = c.Close()
		}
		joined := true
		joinDeadline := time.Now().Add(8 * time.Second)
		for _, c := range calls {
			remaining := time.Until(joinDeadline)
			if remaining < 0 {
				remaining = 0
			}
			select {
			case <-c.done:
			case <-time.After(remaining):
				t.Error("original wire WRITE client not joined")
				joined = false
			}
		}
		if listener != nil {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("listener close: %v", err)
			}
		}
		if server != nil {
			if !server.Quiesce(8 * time.Second) {
				t.Error("original RPC handlers not joined")
				joined = false
			}
			select {
			case err := <-serveDone:
				if err != nil && !errors.Is(err, net.ErrClosed) {
					t.Errorf("Serve: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Error("original Serve not joined")
				joined = false
			}
		}
		if !joined {
			return
		}
		if !h.StopHandler() {
			t.Error("handler ownership not joined")
			return
		}
		if spool != nil && !spoolOwnedByHandler {
			spool.Stop()
		}
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
			return
		}
		if t.Failed() { // keep the exact image and DB for any diagnostic RED
			return
		}
		if err := os.RemoveAll(base); err != nil {
			t.Errorf("remove joined fixture: %v", err)
			return
		}
		retained = false
	})
	fullCleanupRegistered = true
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err = NewSpoolStore(filepath.Join(base, "spool"), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: backing, Workers: 1, SmallWorkers: -1})
	if err != nil {
		t.Fatal(err)
	}
	h.SetSpool(spool, d) // ordinary active spool; never dispatch a drain
	spoolOwnedByHandler = true
	h.spoolLifecycleMu.Lock()
	stopSweep := h.spoolSweeperStop
	h.spoolSweeperStop = nil
	h.spoolLifecycleMu.Unlock()
	if stopSweep == nil {
		t.Fatal("no sweeper join handle")
	}
	stopSweep() // keep this same cohort writing during all three finite phases
	jfs := &juiceFS{handler: h}
	f, err := jfs.Create("clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	entry, ok := spool.LookupActive("clip.mov")
	if !ok {
		t.Fatal("Create did not leave active spool")
	}
	rowID, inode := entry.ID(), entry.Inode()
	before, err := store.CaptureMirror("clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	handle := h.ToHandle(jfs, []string{"clip.mov"})
	rawInode, err := h.export.inode(handle)
	if err != nil {
		t.Fatal(err)
	}
	qualify := func() {
		t.Helper()
		fs, parts, err := h.FromHandle(handle)
		cohort, ok := fs.(*continuationFS)
		if err != nil || !ok || strings.Join(parts, "/") != "clip.mov" ||
			cohort.spool != spool || cohort.entry != entry || cohort.rowID != rowID ||
			cohort.inode != inode || rawInode != inode || cohort.incarnation != before.Incarnation ||
			before.Incarnation == "" || before.Revision <= 0 || before.StoreID != "" {
			t.Fatalf("ordinary FromHandle continuation cohort lost: fs=%T parts=%v err=%v", fs, parts, err)
		}
	}
	qualify()
	listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server = nfslib.NewManagedServer(h)
	serveDone = make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	address := listener.Addr().String()
	all := make([]*rpcCall, 0, 129)
	const chunk = 4096
	for phase, count := range []int{1, 32, 96} {
		qualify()
		if !wireBudgetWaitConnections(server, 0, 3*time.Second) {
			t.Fatal("prior client reader not joined")
		}
		phaseCalls := make([]*rpcCall, 0, count)
		frames := make([][]byte, 0, count)
		for i := 0; i < count; i++ {
			slot := len(all) + i
			body := make([]byte, chunk)
			for j := range body {
				body[j] = byte((slot*31+j*17)%251 + 1)
			}
			frame, err := wireBudgetFrame(handle, slot, body)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := net.DialTimeout("tcp", address, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			sockets = append(sockets, conn) // owned even if later setup fails
			c := &rpcCall{done: make(chan struct{}), conn: conn, slot: slot, body: body}
			phaseCalls = append(phaseCalls, c)
			frames = append(frames, frame)
		}
		if !wireBudgetWaitConnections(server, int64(count), 4*time.Second) {
			t.Fatalf("preconnected readers=%d, want %d", server.ActiveConnections(), count)
		}
		gate := make(chan struct{})
		var release sync.Once
		releaseGate := func() { release.Do(func() { close(gate) }) }
		t.Cleanup(releaseGate)
		ready := make(chan struct{}, count)
		for i, c := range phaseCalls {
			calls = append(calls, c) // original handle retained before goroutine starts
			frame := frames[i]
			go func(c *rpcCall) {
				defer close(c.done)
				defer c.conn.Close()
				ready <- struct{}{}
				<-gate
				c.began = time.Now()
				c.status, c.err = wireBudgetStatus(c.conn, frame, uint32(7+c.slot))
				c.ended = time.Now()
			}(c)
		}
		for i := 0; i < count; i++ {
			select {
			case <-ready:
			case <-time.After(4 * time.Second):
				t.Fatal("original client failed to reach send barrier")
			}
		}
		beforeCounters := metrics.Default().AdmissionRefusals()
		wallStart := time.Now()
		releaseGate() // no SQL, namespace, spool, or opener lock held
		phaseDeadline := time.Now().Add(6 * time.Second)
		for _, c := range phaseCalls {
			remaining := time.Until(phaseDeadline)
			if remaining < 0 {
				remaining = 0
			}
			select {
			case <-c.done:
			case <-time.After(remaining):
				t.Fatal("original wire WRITE exceeded bounded client completion")
			}
		}
		wall := time.Since(wallStart)
		if !wireBudgetWaitConnections(server, 0, 4*time.Second) {
			t.Fatalf("phase %d client readers not joined", phase)
		}
		all = append(all, phaseCalls...)
		okCount, jukebox, transport, unexpected := 0, 0, 0, 0
		firstSend, lastSend := phaseCalls[0].began, phaseCalls[0].began
		outcomes := make([]string, 0, count)
		for _, c := range phaseCalls {
			if c.began.Before(firstSend) {
				firstSend = c.began
			}
			if c.began.After(lastSend) {
				lastSend = c.began
			}
			switch {
			case c.err != nil:
				transport++
			case c.status == uint32(nfslib.NFSStatusOk):
				okCount++
			case c.status == uint32(nfslib.NFSStatusJukebox):
				jukebox++
			default:
				unexpected++
			}
			outcomes = append(outcomes, fmt.Sprintf("slot=%d/status=%d/error=%v/elapsed=%s", c.slot, c.status, c.err, c.ended.Sub(c.began)))
		}
		afterCounters := metrics.Default().AdmissionRefusals()
		deltas, err := wireBudgetAdmissionDelta(beforeCounters, afterCounters)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("normal_wire_budget phase=%d clients=%d OK=%d JUKEBOX=%d transport=%d unexpected=%d wall=%s send_skew=%s cap_delta=%d all_reason_deltas=%v", phase, count, okCount, jukebox, transport, unexpected, wall, lastSend.Sub(firstSend), deltas["continuation_waiter_cap"], deltas)
		t.Logf("normal_wire_budget phase=%d original_rpc_outcomes=%s", phase, strings.Join(outcomes, ","))
		if transport != 0 || unexpected != 0 {
			t.Fatalf("phase %d has uncertain/unexpected wire replies; exact bytes not attributed", phase)
		}
		if okCount == 0 {
			t.Fatalf("phase %d has no positive WRITE; budget observation invalid", phase)
		}
		qualify()
		current, active := spool.LookupActive("clip.mov")
		if !active || current != entry || current.ID() != rowID {
			t.Fatal("original active spool cohort changed")
		}
		afterMirror, err := store.CaptureMirror("clip.mov")
		if err != nil || afterMirror.Incarnation != before.Incarnation {
			t.Fatalf("mirror identity changed: %+v %v", afterMirror, err)
		}
		image, err := os.ReadFile(entry.SpoolFilePath())
		if err != nil {
			t.Fatal(err)
		}
		if entry.WrittenEnd() != int64(len(image)) {
			t.Fatalf("spool written end=%d image bytes=%d", entry.WrittenEnd(), len(image))
		}
		if len(image) > len(all)*chunk {
			t.Fatalf("spool image escaped attempted range: %d > %d", len(image), len(all)*chunk)
		}
		mutatedNonOK := 0
		for _, c := range all {
			segment := make([]byte, chunk)
			offset := c.slot * chunk
			if offset < len(image) {
				copy(segment, image[offset:min(offset+chunk, len(image))])
			}
			match := bytes.Equal(segment, c.body)
			zero := bytes.Equal(segment, make([]byte, chunk))
			if c.status == uint32(nfslib.NFSStatusOk) && !match {
				t.Fatalf("acknowledged WRITE slot %d bytes differ", c.slot)
			}
			if c.status != uint32(nfslib.NFSStatusOk) {
				if match {
					mutatedNonOK++
				} else if !zero {
					t.Fatalf("non-OK WRITE slot %d contains partial/foreign bytes", c.slot)
				}
			}
		}
		t.Logf("normal_wire_budget phase=%d image_bytes=%d acknowledged=%d nonOK_mutated=%d cohort_row=%d", phase, len(image), okCount, mutatedNonOK, rowID)
		if okCount != count || jukebox != 0 || mutatedNonOK != 0 {
			t.Fatalf("ordinary WRITE burst refused: phase=%d OK=%d/%d JUKEBOX=%d nonOK_mutated=%d", phase, okCount, count, jukebox, mutatedNonOK)
		}
		if len(image) != len(all)*chunk {
			t.Fatalf("exact full spool image bytes=%d, want %d", len(image), len(all)*chunk)
		}
		for reason, n := range deltas {
			if n != 0 {
				t.Fatalf("ordinary WRITE burst refused: phase=%d admission %s=%d", phase, reason, n)
			}
		}
	}
}

func wireBudgetFrame(handle []byte, slot int, body []byte) ([]byte, error) {
	var call bytes.Buffer
	if err := binary.Write(&call, binary.BigEndian, []uint32{uint32(7 + slot), 0, 2, 100003, 3, uint32(nfslib.NFSProcedureWrite), 0, 0, 0, 0}); err != nil {
		return nil, err
	}
	if err := xdr.Write(&call, bindingWriteArgs{Handle: handle, Offset: uint64(slot * len(body)), Count: uint32(len(body)), Data: body}); err != nil {
		return nil, err
	}
	var record bytes.Buffer
	if err := binary.Write(&record, binary.BigEndian, uint32(call.Len())|0x80000000); err != nil {
		return nil, err
	}
	_, err := record.Write(call.Bytes())
	return record.Bytes(), err
}

func wireBudgetStatus(conn net.Conn, record []byte, xid uint32) (uint32, error) {
	if err := conn.SetDeadline(time.Now().Add(4 * time.Second)); err != nil {
		return 0, err
	}
	if _, err := io.Copy(conn, bytes.NewReader(record)); err != nil {
		return 0, err
	}
	var marker uint32
	if err := binary.Read(conn, binary.BigEndian, &marker); err != nil {
		return 0, err
	}
	if marker&0x80000000 == 0 || marker&0x7fffffff > 1<<20 {
		return 0, fmt.Errorf("unexpected RPC record %#x", marker)
	}
	reply := make([]byte, marker&0x7fffffff)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return 0, err
	}
	if len(reply) < 28 || binary.BigEndian.Uint32(reply[0:]) != xid || binary.BigEndian.Uint32(reply[4:]) != 1 ||
		binary.BigEndian.Uint32(reply[8:]) != 0 || binary.BigEndian.Uint32(reply[16:]) != 0 || binary.BigEndian.Uint32(reply[20:]) != 0 {
		return 0, fmt.Errorf("unexpected RPC envelope for XID %d", xid)
	}
	return binary.BigEndian.Uint32(reply[24:]), nil
}

func wireBudgetWaitConnections(server *nfslib.Server, count int64, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if server.ActiveConnections() == count {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return server.ActiveConnections() == count
}

func wireBudgetAdmissionDelta(before, after metrics.AdmissionSnapshot) (map[string]uint64, error) {
	bv, av := reflect.ValueOf(before), reflect.ValueOf(after)
	typ := reflect.TypeOf(before)
	out := make(map[string]uint64, bv.NumField())
	for i := 0; i < bv.NumField(); i++ {
		old, next := bv.Field(i).Uint(), av.Field(i).Uint()
		if next < old {
			return nil, fmt.Errorf("admission counter reset: %s", typ.Field(i).Name)
		}
		out[typ.Field(i).Tag.Get("json")] = next - old
	}
	return out, nil
}
