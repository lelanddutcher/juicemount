package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// GUI ↔ service contract for the busy-Quit panel. The GUI side is the real
// Swift EjectNowRunner + EjectNowHTTP client, built from the app sources into
// Tests/EjectNowContractProbe.swift; the service side is the real
// handleEjectNowHTTP over TCP, backed by the real helper core
// (unmountd.Service) and a model kernel. Only the time constants shrink.
//
// The probe is a Swift binary, so this test runs only when
// JM_GUI_EJECT_PROBE names it (see the probe's header for the build line).

// guiContractKernel is e2eKernel plus a gate that can hold a FORCED unmount
// in the kernel (a force under way), and a record that one was issued.
type guiContractKernel struct {
	*e2eKernel
	forceGate chan struct{} // nil: forces return at once
	forced    atomic.Bool
}

func (k *guiContractKernel) UnmountByFSID(fsid unmountd.FSID, flags int) error {
	if flags&unmountd.FlagForce != 0 {
		k.forced.Store(true)
		if k.forceGate != nil {
			<-k.forceGate
		}
	}
	return k.e2eKernel.UnmountByFSID(fsid, flags)
}

type guiContractServer struct {
	addr   string
	kernel *guiContractKernel
	mu     sync.Mutex
	log    []string    // mutations only: "POST start 202", "POST escalate 200", "DELETE 409", …
	at     []time.Time // arrival of each log entry's request
	// dropStarts: this many start POSTs are lost: the connection closes
	// before the handler sees them, so the GUI gets no HTTP answer.
	dropStarts atomic.Int32
}

func (s *guiContractServer) count(method string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, l := range s.log {
		if strings.HasPrefix(l, method+" ") || l == method {
			n++
		}
	}
	return n
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) { r.code = code; r.ResponseWriter.WriteHeader(code) }

func startGUIContractServer(t *testing.T, holdForce bool) *guiContractServer {
	t.Helper()
	useFakeServiceHelper(t, &fakeServiceHelper{ping: client.PingInfo{Protocol: client.ProtocolVersion, Role: "service", UID: e2eUID}})
	oldFloor, oldOrd, oldWait := ejectNowFloor, ejectNowOrdinaryWait, ejectNowWait
	ejectNowFloor = 2 * time.Second
	ejectNowOrdinaryWait = 200 * time.Millisecond
	ejectNowWait = 1500 * time.Millisecond
	t.Cleanup(func() { ejectNowFloor, ejectNowOrdinaryWait, ejectNowWait = oldFloor, oldOrd, oldWait })

	k := &guiContractKernel{e2eKernel: &e2eKernel{busy: true, there: true}}
	if holdForce {
		k.forceGate = make(chan struct{})
	}
	st, err := unmountd.OpenStore(filepath.Join(t.TempDir(), "unmountd"), 0)
	if err != nil {
		t.Fatal(err)
	}
	pol := unmountd.DefaultPolicy()
	pol.BackoffMin, pol.BackoffMax = 5*time.Millisecond, 20*time.Millisecond
	pol.SnapshotWatch, pol.Watch, pol.InKernelPoll = time.Second, time.Second, 20*time.Millisecond
	svc, err := unmountd.NewService(unmountd.Config{Store: st, Kernel: k, Policy: pol})
	if err != nil {
		t.Fatal(err)
	}
	ctl := &coreBackedNFS{fakeNFSCustody: fakeNFSCustody{path: "/Volumes/zpool"}, svc: svc, op: "nfs-" + e2eToken}
	if _, err := svc.Register(ctl.caller(), unmountd.RegisterRequest{OpID: ctl.op, Type: unmountd.TypeNFS, FSID: e2eFSID,
		Token: e2eToken, MntOnName: "/Volumes/zpool", MntFromName: e2eSource}); err != nil {
		t.Fatal(err)
	}
	publishNFSController(t, ctl)

	s := &guiContractServer{kernel: k}
	mux := http.NewServeMux()
	mux.HandleFunc("/eject-now", func(w http.ResponseWriter, r *http.Request) {
		// Record the phase the GUI sent ("-" when none) without consuming
		// the body the handler reads.
		method := r.Method
		if r.Method == http.MethodPost {
			raw, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var req struct {
				Phase string `json:"phase"`
			}
			_ = json.Unmarshal(raw, &req)
			if req.Phase == "" {
				req.Phase = "-"
			}
			method += " " + req.Phase
		}
		arrived := time.Now()
		if method == "POST start" && s.dropStarts.Add(-1) >= 0 {
			s.mu.Lock()
			s.log = append(s.log, "POST start dropped")
			s.at = append(s.at, arrived)
			s.mu.Unlock()
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			conn.Close()
			return
		}
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		handleEjectNowHTTP(rec, r)
		if r.Method == http.MethodGet {
			return // the capability read is not part of the sequence
		}
		s.mu.Lock()
		s.log = append(s.log, fmt.Sprintf("%s %d", method, rec.code))
		s.at = append(s.at, arrived)
		s.mu.Unlock()
	})
	// Test-only side channel: has a forced unmount reached the kernel?
	mux.HandleFunc("/test/forced", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, k.forced.Load())
	})
	srv := httptest.NewServer(mux)
	// svc.Close waits for its jobs; release a held force first (cleanups
	// run last-registered first).
	t.Cleanup(svc.Close)
	t.Cleanup(srv.Close)
	if holdForce {
		t.Cleanup(func() {
			select {
			case <-k.forceGate:
			default:
				close(k.forceGate)
			}
		})
	}
	s.addr = strings.TrimPrefix(srv.URL, "http://")
	return s
}

// runGUIProbe runs one scenario and returns the probe's KEY=VALUE lines.
func runGUIProbe(t *testing.T, bin, scenario, addr string) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, scenario, addr)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	kv := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(out.Bytes()))
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), "="); ok {
			kv[k] = v
		}
	}
	if err != nil {
		t.Fatalf("probe %s: %v\n%s", scenario, err, out.String())
	}
	t.Logf("probe %s:\n%s", scenario, out.String())
	return kv
}

func probeInt(t *testing.T, kv map[string]string, key string) int {
	t.Helper()
	n, err := strconv.Atoi(kv[key])
	if err != nil {
		t.Fatalf("probe output has no %s: %v", key, kv)
	}
	return n
}

func TestEjectNowGUIContract(t *testing.T) {
	bin := os.Getenv("JM_GUI_EJECT_PROBE")
	if bin == "" {
		t.Skip("JM_GUI_EJECT_PROBE unset (build app/JuiceMount/Tests/EjectNowContractProbe.swift)")
	}

	// "Eject now" on a busy volume: the GUI keeps POSTing through 202s,
	// honouring retry_after, until the service's floor allows the force.
	t.Run("eject", func(t *testing.T) {
		s := startGUIContractServer(t, false)
		kv := runGUIProbe(t, bin, "eject", s.addr)
		if kv["OUTCOME"] != "retired" {
			t.Fatalf("outcome %q, want retired", kv["OUTCOME"])
		}
		if !s.kernel.forced.Load() {
			t.Fatal("RETIRED without a forced unmount on a busy volume")
		}
		if _, present := s.kernel.state(); present {
			t.Fatal("volume still present after RETIRED")
		}
		if s.count("DELETE") != 0 || s.count("POST") < 3 || probeInt(t, kv, "POSTS") != s.count("POST") {
			t.Fatalf("requests %v, probe POSTS=%s", s.log, kv["POSTS"])
		}
		// Round 7: the countdown's POST is a start, every later one an
		// escalate, and only an escalate is answered with the force.
		s.mu.Lock()
		log := append([]string(nil), s.log...)
		s.mu.Unlock()
		if log[0] != "POST start 202" {
			t.Fatalf("first request %q, want POST start 202 (log %v)", log[0], log)
		}
		for _, l := range log[1:] {
			if !strings.HasPrefix(l, "POST escalate ") {
				t.Fatalf("request after the start is %q, want an escalate POST (log %v)", l, log)
			}
		}
		if want := "start," + strings.Repeat("escalate,", len(log)-2) + "escalate"; kv["PHASES"] != want {
			t.Fatalf("probe phases %q, want %q", kv["PHASES"], want)
		}
	})

	// Round 7, the measured carry-over: an earlier panel left a busy window
	// nobody withdrew, older than the floor. The new panel's start POST must
	// not force; Cancel Quit then withdraws, and the volume stays mounted.
	t.Run("carry-over-start-never-forces", func(t *testing.T) {
		s := startGUIContractServer(t, false)
		req, _ := http.NewRequest(http.MethodPost, "http://"+s.addr+"/eject-now", strings.NewReader(`{"phase":"escalate"}`))
		req.Header.Set(localControlHeader, "1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("earlier panel's POST: %d", resp.StatusCode)
		}
		time.Sleep(ejectNowFloor + 500*time.Millisecond) // the old window now meets the floor
		kv := runGUIProbe(t, bin, "cancel-after-post", s.addr)
		if s.kernel.forced.Load() {
			t.Fatalf("CARRY-OVER: the new panel's start POST forced the volume (log %v)", s.log)
		}
		if kv["OUTCOME"] != "abandoned:withdrawn" {
			t.Fatalf("outcome %q, want abandoned:withdrawn (log %v)", kv["OUTCOME"], s.log)
		}
		if got := fmt.Sprint(s.log); got != "[POST escalate 202 POST start 202 DELETE 200]" {
			t.Fatalf("requests %s, want [POST escalate 202 POST start 202 DELETE 200]", got)
		}
		if _, present := s.kernel.state(); !present {
			t.Fatal("volume gone after the new panel's start POST and Cancel Quit")
		}
	})

	// Review #7, finding 1 (lost start): an earlier panel left a busy window
	// older than the floor, and the new panel's start POST is lost (no HTTP
	// answer, so the service never saw it). "Eject now" right after must not
	// force on the old window: the GUI sends the start again, and the force
	// comes no sooner than the floor after that start's arrival.
	t.Run("lost-start-eject-now-waits-full-floor", func(t *testing.T) {
		s := startGUIContractServer(t, false)
		req, _ := http.NewRequest(http.MethodPost, "http://"+s.addr+"/eject-now", strings.NewReader(`{"phase":"escalate"}`))
		req.Header.Set(localControlHeader, "1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("earlier panel's POST: %d", resp.StatusCode)
		}
		time.Sleep(ejectNowFloor + 500*time.Millisecond) // the old window now meets the floor
		s.dropStarts.Store(1)
		kv := runGUIProbe(t, bin, "eject", s.addr)
		s.mu.Lock()
		log := append([]string(nil), s.log...)
		at := append([]time.Time(nil), s.at...)
		s.mu.Unlock()
		// Validity: the start really was lost, and the GUI saw no answer.
		if len(log) < 3 || log[1] != "POST start dropped" || !strings.HasPrefix(kv["PHASES"], "start,") {
			t.Fatalf("scenario did not run: log %v, probe phases %q", log, kv["PHASES"])
		}
		if log[2] != "POST start 202" {
			t.Fatalf("LOST START: the POST after the lost start is %q, want POST start 202 (log %v)", log[2], log)
		}
		if kv["OUTCOME"] != "retired" || !s.kernel.forced.Load() {
			t.Fatalf("outcome %q forced=%v, want retired with the force (log %v)", kv["OUTCOME"], s.kernel.forced.Load(), log)
		}
		last := len(log) - 1
		if log[last] != "POST escalate 200" {
			t.Fatalf("last request %q, want POST escalate 200 (log %v)", log[last], log)
		}
		for _, l := range log[3:last] {
			if l != "POST escalate 202" {
				t.Fatalf("request %q between the accepted start and the force (log %v)", l, log)
			}
		}
		// The service reads its clock after the escalate's ordinary pass
		// (up to ejectNowOrdinaryWait), so the force POST may ARRIVE that
		// much before the floor is met.
		if waited := at[last].Sub(at[2]); waited < ejectNowFloor-ejectNowOrdinaryWait-50*time.Millisecond {
			t.Fatalf("force POST %v after the accepted start, want about the %v floor (log %v)", waited, ejectNowFloor, log)
		}
		if want := "start,start," + strings.Repeat("escalate,", last-3) + "escalate"; kv["PHASES"] != want {
			t.Fatalf("probe phases %q, want %q", kv["PHASES"], want)
		}
	})

	// Cancel Quit after the start POST: exactly one DELETE, no further POST,
	// and the volume stays mounted after the apps let go.
	t.Run("cancel-after-post", func(t *testing.T) {
		s := startGUIContractServer(t, false)
		kv := runGUIProbe(t, bin, "cancel-after-post", s.addr)
		if kv["OUTCOME"] != "abandoned:withdrawn" {
			t.Fatalf("outcome %q, want abandoned:withdrawn", kv["OUTCOME"])
		}
		if got := fmt.Sprint(s.log); got != "[POST start 202 DELETE 200]" {
			t.Fatalf("requests %s, want [POST start 202 DELETE 200]", got)
		}
		after, _ := s.kernel.state()
		s.kernel.mu.Lock()
		s.kernel.busy = false
		s.kernel.mu.Unlock()
		time.Sleep(300 * time.Millisecond)
		if n, present := s.kernel.state(); n != after || !present {
			t.Fatalf("unmount after the GUI's DELETE: calls %d -> %d, present=%v", after, n, present)
		}
	})

	// Cancel before any POST: nothing reaches the service.
	t.Run("cancel-before-post", func(t *testing.T) {
		s := startGUIContractServer(t, false)
		kv := runGUIProbe(t, bin, "cancel-before-post", s.addr)
		if kv["OUTCOME"] != "abandoned:none" || len(s.log) != 0 {
			t.Fatalf("outcome %q requests %v", kv["OUTCOME"], s.log)
		}
	})

	// Cancel while the force is in the kernel: the DELETE answers 409 and
	// the GUI reports force-in-progress, not "cancelled".
	t.Run("cancel-during-force", func(t *testing.T) {
		s := startGUIContractServer(t, true)
		kv := runGUIProbe(t, bin, "cancel-during-force", s.addr)
		if kv["OUTCOME"] != "abandoned:force_in_progress" {
			t.Fatalf("outcome %q, want abandoned:force_in_progress", kv["OUTCOME"])
		}
		if !strings.Contains(kv["PANEL"], "can no longer be cancelled") {
			t.Fatalf("panel text %q", kv["PANEL"])
		}
		if s.count("DELETE") != 1 || probeInt(t, kv, "POSTS_AT_CANCEL") != probeInt(t, kv, "POSTS") {
			t.Fatalf("requests %v; POSTs at cancel %s, at end %s", s.log, kv["POSTS_AT_CANCEL"], kv["POSTS"])
		}
		s.mu.Lock()
		last := s.log[len(s.log)-1]
		s.mu.Unlock()
		if last != "DELETE 409" {
			t.Fatalf("last request %q, want DELETE 409 (log %v)", last, s.log)
		}
	})
}
