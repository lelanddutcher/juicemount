//go:build gateway_acceptance

package mutationgateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/continuity/delivery"
)

// The separately built standalone Manager serves this gate on its unprefixed
// route. Its original listener address is read from its retained log scanner;
// no test process probes, reserves, releases, or rebinds the port. The fixture
// has a real Gateway and schema-8 catalog. This is loopback executable evidence,
// not a native helper, Mac copy, Finder, NAS, or packaged-app acceptance claim.
type catalogManagerOriginal struct {
	t        *testing.T
	cmd      *exec.Cmd
	done     chan error
	scanned  chan catalogManagerScan
	ready    chan string
	waited   bool
	joined   bool
	signaled bool
	waitErr  error
	scan     catalogManagerScan
	stopErr  error
}

type catalogManagerScan struct {
	err       error
	listening int
}

func catalogManagerVerifyBinary(t *testing.T) (string, string) {
	t.Helper()
	path, want := os.Getenv("JM_MANAGER_BINARY"), os.Getenv("JM_MANAGER_BINARY_SHA256")
	if !filepath.IsAbs(path) || !validHash(want) {
		t.Fatal("absolute JM_MANAGER_BINARY and lowercase exact JM_MANAGER_BINARY_SHA256 are required")
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0111 == 0 {
		t.Fatal("Manager candidate must be a regular executable, not a symlink")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	opened, statErr := file.Stat()
	h := sha256.New()
	_, readErr := io.Copy(h, file)
	closeErr := file.Close()
	after, afterErr := os.Lstat(path)
	if statErr != nil || readErr != nil || closeErr != nil || afterErr != nil || !os.SameFile(before, opened) || !os.SameFile(opened, after) || hex.EncodeToString(h.Sum(nil)) != want {
		t.Fatal("Manager executable identity/hash changed")
	}
	return path, want
}

func catalogManagerStart(t *testing.T, f *catalogExecutableFixture) (*catalogManagerOriginal, string) {
	t.Helper()
	binary, digest := catalogManagerVerifyBinary(t)
	// The existing Manager requires a deployment admin key even though this
	// test never calls admin routes. It is a fresh private fixture value. The
	// delivery bearer remains only in memory and Authorization headers.
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	admin := hex.EncodeToString(key)
	clear(key)
	cmd := exec.Command(binary,
		"--listen", "127.0.0.1:0",
		"--source-roots", f.dir,
		"--juicefs", filepath.Join(f.dir, "never-execute-juicefs"),
		"--meta", "redis://127.0.0.1:1/0",
		"--vol-name", "catalog_fixture",
		"--continuity-catalog", f.database,
		"--continuity-library", string(catalogExecutableLibrary),
		"--continuity-delivery-registry", f.registry,
		"--continuity-delivery-mode", "loopback_http")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "JM_ADMIN_KEY=" + admin}
	if tmp := os.Getenv("TMPDIR"); tmp != "" {
		cmd.Env = append(cmd.Env, "TMPDIR="+tmp)
	}
	admin = ""
	output, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = writer, writer
	p := &catalogManagerOriginal{t: t, cmd: cmd, done: make(chan error, 1), scanned: make(chan catalogManagerScan, 1), ready: make(chan string, 1)}
	if err := cmd.Start(); err != nil {
		_ = output.Close()
		_ = writer.Close()
		t.Fatal("Manager executable did not start")
	}
	t.Cleanup(func() { p.stopNormally() })
	go func() { p.done <- cmd.Wait() }()
	go func() {
		var result catalogManagerScan
		scanner := bufio.NewScanner(output)
		scanner.Buffer(make([]byte, 1024), 65536)
		for scanner.Scan() {
			line := scanner.Text()
			if index := strings.Index(line, "manager HTTP listening on "); index >= 0 {
				result.listening++
				addr := line[index+len("manager HTTP listening on "):]
				select {
				case p.ready <- addr:
				default:
				}
			}
		}
		result.err = errors.Join(scanner.Err(), output.Close())
		p.scanned <- result
	}()
	imageExecutableRecord(t, map[string]any{"kind": "manager_original", "event": "start", "pid": cmd.Process.Pid, "binary_sha256": digest})
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case addr := <-p.ready:
		host, port, splitErr := net.SplitHostPort(addr)
		number, numberErr := strconv.Atoi(port)
		if splitErr != nil || host != "127.0.0.1" || numberErr != nil || number < 1 || number > 65535 {
			t.Fatal("Manager readiness was not its owned numeric loopback listener")
		}
		imageExecutableRecord(t, map[string]any{"kind": "manager_original", "event": "ready", "pid": cmd.Process.Pid, "address": addr})
		return p, "http://" + addr
	case err := <-p.done:
		p.recordWait(err)
		t.Fatal("Manager original exited before readiness")
	case <-timer.C:
		imageExecutableRecord(t, map[string]any{"kind": "manager_original", "event": "startup_observation_deadline", "pid": cmd.Process.Pid})
		t.Fatal("Manager readiness deadline; original retained for normal join")
	}
	return nil, ""
}

func (p *catalogManagerOriginal) recordWait(err error) {
	p.waited, p.waitErr = true, err
	code, signaled := -1, false
	if p.cmd.ProcessState != nil {
		code = p.cmd.ProcessState.ExitCode()
		if status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
			signaled = status.Signaled()
		}
	}
	imageExecutableRecord(p.t, map[string]any{"kind": "manager_original", "event": "wait", "pid": p.cmd.Process.Pid, "wait_completed": true, "exit_code": code, "signaled": signaled, "wait_error": err != nil})
}

func (p *catalogManagerOriginal) stopNormally() {
	if !p.waited {
		select {
		case err := <-p.done:
			p.recordWait(err)
		default:
			p.stopErr = p.cmd.Process.Signal(syscall.SIGTERM)
			p.signaled = p.stopErr == nil
			imageExecutableRecord(p.t, map[string]any{"kind": "manager_original", "event": "signal", "pid": p.cmd.Process.Pid, "signal": "SIGTERM", "signal_error": p.stopErr != nil})
			p.recordWait(<-p.done)
		}
	}
	if !p.joined {
		p.scan = <-p.scanned
		p.joined = true
		imageExecutableRecord(p.t, map[string]any{"kind": "manager_original", "event": "scanner_join", "pid": p.cmd.Process.Pid, "scanner_error": p.scan.err != nil, "listening_lines": p.scan.listening})
	}
	if !p.signaled || p.stopErr != nil || p.waitErr != nil || p.cmd.ProcessState == nil || p.cmd.ProcessState.ExitCode() != 0 || p.scan.err != nil || p.scan.listening != 1 {
		p.t.Error("Manager original did not stop with one joined listener and normal exit")
	}
}

type catalogManagerHTTP struct {
	t      *testing.T
	base   string
	bearer []byte
	client *http.Client
}

func (h *catalogManagerHTTP) post(route string, value any, bearer []byte) *http.Response {
	h.t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, h.base+"/api/continuity/v1/delivery/"+route, bytes.NewReader(catalogExecutableJSON(h.t, value)))
	if err != nil {
		h.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if bearer != nil {
		request.Header.Set("Authorization", "Bearer "+string(bearer))
	}
	response, err := h.client.Do(request)
	if err != nil {
		h.t.Fatal("Manager HTTP request did not complete", err)
	}
	return response
}

func (h *catalogManagerHTTP) json(route string, value any, bearer []byte, target any) []byte {
	h.t.Helper()
	response := h.post(route, value, bearer)
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 2049))
	closeErr := response.Body.Close()
	imageExecutableRecord(h.t, map[string]any{"kind": "manager_http", "route": route, "status": response.StatusCode, "body_bytes": len(body), "read_complete": readErr == nil, "body_closed": closeErr == nil})
	if response.StatusCode != http.StatusOK || readErr != nil || closeErr != nil || len(body) == 0 || len(body) > 2048 || response.Header.Get("Cache-Control") != "no-store" {
		h.t.Fatal("Manager JSON response incomplete or unexpected")
	}
	catalogExecutableDecode(h.t, body, target)
	if !bytes.Equal(body, catalogExecutableJSON(h.t, target)) {
		h.t.Fatal("Manager JSON response was not canonical")
	}
	return body
}

func (h *catalogManagerHTTP) expectError(route string, value any, bearer []byte, status int, code string) {
	h.t.Helper()
	response := h.post(route, value, bearer)
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 2049))
	closeErr := response.Body.Close()
	var failure struct {
		Version int    `json:"version"`
		Error   string `json:"error"`
	}
	if readErr != nil || closeErr != nil || len(body) > 2048 || response.StatusCode != status || response.Header.Get("X-JM-Delivery") != "" || response.Header.Get("Cache-Control") != "no-store" {
		h.t.Fatal("Manager refusal framing/status changed")
	}
	catalogExecutableDecode(h.t, body, &failure)
	if failure.Version != 1 || failure.Error != code || !bytes.Equal(body, catalogExecutableJSON(h.t, failure)) {
		h.t.Fatal("Manager refusal code changed")
	}
	imageExecutableRecord(h.t, map[string]any{"kind": "manager_http", "route": route, "status": status, "error": code, "body_closed": true})
}

func (h *catalogManagerHTTP) selectDelivery(request catalog.DeliverySelectRequest) catalog.DeliverySelection {
	h.t.Helper()
	var selected catalog.DeliverySelection
	h.json("select", request, h.bearer, &selected)
	return selected
}

func (h *catalogManagerHTTP) transfer(selection catalog.DeliverySelection, operation string, expected []byte, offset int64) delivery.TransferHeader {
	h.t.Helper()
	if offset < 0 || offset > int64(len(expected)) {
		h.t.Fatal("bad fixture offset")
	}
	prefix := sha256.Sum256(expected[:offset])
	request := delivery.TransferRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: selection.SelectionID, OperationID: operation, Offset: offset, PrefixSHA256: hex.EncodeToString(prefix[:])}
	response := h.post("transfer", request, h.bearer)
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 2049))
		_ = response.Body.Close()
		h.t.Fatal("Manager refused expected transfer")
	}
	values := response.Header.Values("X-JM-Delivery")
	if len(values) != 1 || response.Header.Get("Content-Type") != "application/octet-stream" || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("X-Content-Type-Options") != "nosniff" || response.ContentLength != -1 || response.Header.Get("Content-Encoding") != "" {
		_ = response.Body.Close()
		h.t.Fatal("Manager transfer headers changed")
	}
	rawHeader, err := base64.RawURLEncoding.DecodeString(values[0])
	if err != nil || base64.RawURLEncoding.EncodeToString(rawHeader) != values[0] {
		_ = response.Body.Close()
		h.t.Fatal("Manager transfer header was not canonical base64url")
	}
	header, err := delivery.DecodeTransferHeader(rawHeader)
	if err != nil || delivery.ValidateTransferResponse(request, selection, header, nil) != nil {
		_ = response.Body.Close()
		h.t.Fatal("Manager transfer header lost pin or request binding")
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, int64(len(expected))-offset+1))
	endValues := response.Trailer.Values("X-JM-Transfer-End")
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(endValues) != 1 || int64(len(body)) != int64(len(expected))-offset || !bytes.Equal(body, expected[offset:]) {
		h.t.Fatal("Manager transfer bytes, EOF, trailer, or body close incomplete")
	}
	rawEnd, err := base64.RawURLEncoding.DecodeString(endValues[0])
	if err != nil || base64.RawURLEncoding.EncodeToString(rawEnd) != endValues[0] {
		h.t.Fatal("Manager transfer end was not canonical base64url")
	}
	end, err := delivery.DecodeTransferEnd(rawEnd)
	if err != nil || delivery.ValidateTransferResponse(request, selection, header, &end) != nil {
		h.t.Fatal("Manager transfer end lost attempt or cursor binding")
	}
	joined := append(append([]byte(nil), expected[:offset]...), body...)
	sum := sha256.Sum256(joined)
	if int64(len(joined)) != selection.Content.Size || hex.EncodeToString(sum[:]) != selection.Content.SHA256 || !bytes.Equal(joined, expected) {
		h.t.Fatal("Manager transfer failed whole-file reconstruction")
	}
	imageExecutableRecord(h.t, map[string]any{"kind": "manager_http", "route": "transfer", "status": 200, "selection_id": selection.SelectionID, "operation_id": operation, "offset": offset, "body_bytes": len(body), "attempt_id": header.Attempt.ID, "header_valid": true, "end_valid": true, "eof": true, "body_closed": true, "whole_sha256": selection.Content.SHA256})
	return header
}

func TestCatalogDeliveryManagerExecutableBytes(t *testing.T) {
	f := newCatalogExecutableFixture(t)
	f.ok(nil, "upgrade-delivery-lifecycle") // Manager requires existing schema 8.
	f.byteGrant()
	partsA := [][]byte{bytes.Repeat([]byte{0, 1, 2, 255}, 160*1024), bytes.Repeat([]byte("manager-A-middle"), 32*1024), bytes.Repeat([]byte("manager-A-tail"), 16*1024)}
	sealA, dataA := f.seal(81, partsA)
	revisionA := f.revision(catalogExecutableAsset, dataA, "", "manager-actual-revision-A")
	f.bind("manager-actual-bind-A", catalogExecutableAsset, revisionA, sealA, nil)
	manager, endpoint := catalogManagerStart(t, f)
	transport := &http.Transport{Proxy: nil, DisableCompression: true}
	h := &catalogManagerHTTP{t: t, base: endpoint, bearer: f.bearer, client: &http.Client{Timeout: 45 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	t.Cleanup(transport.CloseIdleConnections)
	requestA := f.request("manager-actual-select-A", catalogExecutableAsset, "", int64(len(dataA)))
	selectionA := h.selectDelivery(requestA)
	if selectionA.RevisionID != revisionA || selectionA.Content.SHA256 != sealA.Content.SHA256 {
		t.Fatal("Manager latest select did not pin A")
	}
	var inspection catalog.DeliveryInspection
	h.json("inspect", catalog.DeliveryInspectRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: selectionA.SelectionID}, h.bearer, &inspection)
	if inspection.OriginalSelection != selectionA || !inspection.RequesterDeliveryEligibility.Eligible {
		t.Fatal("Manager inspect lost original selection or eligibility")
	}
	catalogExecutableCheckpoint(t, "manager-original-selected-and-inspected-A")
	partsB := [][]byte{bytes.Repeat([]byte("manager-B-first"), 32*1024), bytes.Repeat([]byte{9, 8, 7, 254}, 160*1024), bytes.Repeat([]byte("manager-B-tail"), 16*1024)}
	sealB, dataB := f.seal(82, partsB)
	if len(dataA) == len(dataB) && bytes.Equal(dataA, dataB) {
		t.Fatal("A and B fixture bytes must differ")
	}
	revisionB := f.revision(catalogExecutableAsset, dataB, revisionA, "manager-actual-revision-B")
	f.bind("manager-actual-bind-B", catalogExecutableAsset, revisionB, sealB, nil)
	selectionB := h.selectDelivery(f.request("manager-actual-select-B", catalogExecutableAsset, "", int64(len(dataB))))
	if selectionB.RevisionID != revisionB || selectionB.SelectionID == selectionA.SelectionID {
		t.Fatal("Manager latest after B did not advance")
	}
	retryA := h.selectDelivery(requestA)
	if retryA != selectionA {
		t.Fatal("Manager exact select retry changed A after B")
	}
	first := h.transfer(selectionA, "manager-actual-transfer-A", dataA, 0)
	second := h.transfer(selectionB, "manager-actual-transfer-B", dataB, 0)
	if first.Attempt.ID == second.Attempt.ID {
		t.Fatal("A and B reused an attempt")
	}
	resumeAt := int64(len(dataA) / 2)
	resumed := h.transfer(selectionA, "manager-actual-transfer-A", dataA, resumeAt)
	if resumed.Attempt.ID != first.Attempt.ID || resumed.Length != int64(len(dataA))-resumeAt {
		t.Fatal("resume changed the completed attempt or retransmitted the prefix")
	}
	finished := h.transfer(selectionA, "manager-actual-transfer-A", dataA, int64(len(dataA)))
	if finished.Attempt.ID != first.Attempt.ID || finished.Length != 0 {
		t.Fatal("zero-byte recovery changed the completed attempt")
	}
	// Independently read the immutable source images after Manager delivery.
	// This prevents a completed HTTP stream from hiding changed Gateway bytes.
	checkSource := func(seal ImageSeal, expected []byte) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		view, err := f.reader.ImageStatus(ctx, imageRef(seal))
		if err != nil || view.Content != seal.Content {
			t.Fatal("Gateway source status changed after Manager transfer")
		}
		imageExecutableFullRead(t, ctx, f.reader, view, expected)
	}
	checkSource(sealA, dataA)
	checkSource(sealB, dataB)
	catalogExecutableCheckpoint(t, "manager-original-full-A-B-tail-resume-and-zero-byte-recovery")
	emptySeal, empty := f.seal(83, [][]byte{})
	emptyRevision := f.revision(catalogExecutableEmptyAsset, empty, "", "manager-actual-empty-revision")
	f.bind("manager-actual-bind-empty", catalogExecutableEmptyAsset, emptyRevision, emptySeal, nil)
	emptySelection := h.selectDelivery(f.request("manager-actual-select-empty", catalogExecutableEmptyAsset, "", 0))
	h.transfer(emptySelection, "manager-actual-transfer-empty", empty, 0)
	catalogExecutableCheckpoint(t, "manager-original-authenticated-empty-end")
	h.expectError("inspect", catalog.DeliveryInspectRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: selectionA.SelectionID}, nil, http.StatusUnauthorized, "unauthenticated")
	emptySHA := sha256.Sum256(nil)
	foreign := delivery.TransferRequest{Version: 1, LibraryID: continuity.LibraryID("lib_99999999999999999999999999999999"), SelectionID: selectionA.SelectionID, OperationID: "manager-actual-foreign", Offset: 0, PrefixSHA256: hex.EncodeToString(emptySHA[:])}
	h.expectError("transfer", foreign, h.bearer, http.StatusNotFound, "unavailable")
	cancelSelection := h.selectDelivery(f.request("manager-actual-select-cancel", catalogExecutableAsset, revisionA, int64(len(dataA))))
	var canceled catalog.DeliveryCancelResult
	h.json("cancel", catalog.DeliveryCancelRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: "manager-actual-cancel", SelectionID: cancelSelection.SelectionID}, h.bearer, &canceled)
	if !canceled.CurrentStatus.RecordState.Canceled || canceled.CurrentStatus.OriginalSelection != cancelSelection {
		t.Fatal("Manager cancel did not durably cut the selected pin")
	}
	denied := delivery.TransferRequest{Version: 1, LibraryID: catalogExecutableLibrary, SelectionID: cancelSelection.SelectionID, OperationID: "manager-actual-after-cancel", Offset: 0, PrefixSHA256: hex.EncodeToString(emptySHA[:])}
	h.expectError("transfer", denied, h.bearer, http.StatusNotFound, "unavailable")
	catalogExecutableCheckpoint(t, "manager-original-scoped-and-canceled-refusal")
	transport.CloseIdleConnections()
	manager.stopNormally()
	f.producer.Close()
	f.reader.Close()
	f.gateway.stopNormally()
	imageExecutableRecord(t, map[string]any{"kind": "manager_delivery_scope", "manager_original_joined": true, "gateway_original_joined": true, "whole_A_bytes": len(dataA), "whole_B_bytes": len(dataB), "native_copy_or_finder_qualified": false})
}
