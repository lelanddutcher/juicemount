//go:build gateway_acceptance

package mutationgateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

type nativeHelperOriginals struct {
	t               *testing.T
	bearer          []byte
	started, joined int
}

type nativeHelperRunResult struct {
	stdout, stderr []byte
	exitCode       int
}

type nativeHelperDrain struct {
	data     []byte
	overflow bool
	err      error
}

func nativeHelperVerifyBinary(t *testing.T) (string, string) {
	t.Helper()
	path, want := os.Getenv("JM_NATIVE_HELPER_BINARY"), os.Getenv("JM_NATIVE_HELPER_BINARY_SHA256")
	if !filepath.IsAbs(path) || !validHash(want) {
		t.Fatal("absolute JM_NATIVE_HELPER_BINARY and lowercase exact JM_NATIVE_HELPER_BINARY_SHA256 are required")
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0111 == 0 {
		t.Fatal("native helper candidate must be a regular executable, not a symlink")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("open native helper binary:", err)
	}
	opened, statErr := file.Stat()
	h := sha256.New()
	_, readErr := io.Copy(h, file)
	closeErr := file.Close()
	after, afterErr := os.Lstat(path)
	if statErr != nil || readErr != nil || closeErr != nil || afterErr != nil || !os.SameFile(before, opened) || !os.SameFile(opened, after) || hex.EncodeToString(h.Sum(nil)) != want {
		t.Fatal("native helper executable identity/hash changed")
	}
	return path, want
}

func nativeHelperDrainPipe(pipe *os.File) nativeHelperDrain {
	var capture catalogExecutableBuffer
	_, readErr := io.Copy(&capture, pipe)
	return nativeHelperDrain{data: append([]byte(nil), capture.data.Bytes()...), overflow: capture.overflow, err: errors.Join(readErr, pipe.Close())}
}

// Every invocation is a newly built catalog-helper original. The parent keeps
// its exact Cmd, private input writer and both output drainers until Wait and
// all three pipe joins complete. Bearer bytes are only written to owned stdin.
func (h *nativeHelperOriginals) run(label string, input []byte, args ...string) nativeHelperRunResult {
	h.t.Helper()
	binary, digest := nativeHelperVerifyBinary(h.t)
	for _, arg := range args {
		if bytes.Contains([]byte(arg), h.bearer) {
			h.t.Fatal("native helper argv contained private bearer bytes")
		}
	}
	cmd := exec.Command(binary, args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2"}
	if tmp := os.Getenv("TMPDIR"); tmp != "" {
		cmd.Env = append(cmd.Env, "TMPDIR="+tmp)
	}
	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		h.t.Fatal("helper private stdin pipe:", err)
	}
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		_ = stdinRead.Close()
		_ = stdinWrite.Close()
		h.t.Fatal("helper private stdout pipe:", err)
	}
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		_ = stdinRead.Close()
		_ = stdinWrite.Close()
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		h.t.Fatal("helper private stderr pipe:", err)
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdinRead, stdoutWrite, stderrWrite
	if err := cmd.Start(); err != nil {
		for _, pipe := range []*os.File{stdinRead, stdinWrite, stdoutRead, stdoutWrite, stderrRead, stderrWrite} {
			_ = pipe.Close()
		}
		h.t.Fatal("native helper executable did not start")
	}
	h.started++
	imageExecutableRecord(h.t, map[string]any{"kind": "native_helper_original", "event": "start", "ordinal": h.started, "label": label, "pid": cmd.Process.Pid, "binary_sha256": digest})
	parentCloseErr := errors.Join(stdinRead.Close(), stdoutWrite.Close(), stderrWrite.Close())
	inputDone := make(chan error, 1)
	outputDone := make(chan nativeHelperDrain, 1)
	diagnosticDone := make(chan nativeHelperDrain, 1)
	go func() {
		_, copyErr := io.Copy(stdinWrite, bytes.NewReader(input))
		inputDone <- errors.Join(copyErr, stdinWrite.Close())
	}()
	go func() { outputDone <- nativeHelperDrainPipe(stdoutRead) }()
	go func() { diagnosticDone <- nativeHelperDrainPipe(stderrRead) }()
	waitErr := cmd.Wait()
	inputErr, output, diagnostic := <-inputDone, <-outputDone, <-diagnosticDone
	h.joined++
	code, signaled := -1, false
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
			signaled = status.Signaled()
		}
	}
	imageExecutableRecord(h.t, map[string]any{"kind": "native_helper_original", "event": "wait_and_stdio_join", "ordinal": h.joined, "label": label, "pid": cmd.Process.Pid, "wait_completed": true, "input_joined": true, "output_joined": true, "diagnostic_joined": true, "exit_code": code, "signaled": signaled, "stdout_bytes": len(output.data), "stderr_bytes": len(diagnostic.data), "input_error": inputErr != nil, "output_error": output.err != nil, "diagnostic_error": diagnostic.err != nil})
	if parentCloseErr != nil || output.err != nil || diagnostic.err != nil || output.overflow || diagnostic.overflow || signaled || code < 0 {
		h.t.Fatal("native helper original did not produce a bounded joined terminal result")
	}
	if waitErr != nil {
		var exit *exec.ExitError
		if !errors.As(waitErr, &exit) {
			h.t.Fatal("native helper original Wait failed")
		}
	}
	if code == 0 && inputErr != nil {
		h.t.Fatal("successful native helper original did not consume private stdin")
	}
	return nativeHelperRunResult{stdout: output.data, stderr: diagnostic.data, exitCode: code}
}

func (h *nativeHelperOriginals) success(result nativeHelperRunResult) []byte {
	h.t.Helper()
	if result.exitCode != 0 || len(result.stderr) != 0 || len(result.stdout) == 0 || result.stdout[len(result.stdout)-1] != '\n' || bytes.Count(result.stdout, []byte{'\n'}) != 1 {
		h.t.Fatal("native helper did not return one clean complete receipt")
	}
	return result.stdout
}

func (h *nativeHelperOriginals) refused(result nativeHelperRunResult) {
	h.t.Helper()
	var diagnostic struct {
		Error             string `json:"error"`
		OperationID       string `json:"operation_id,omitempty"`
		Selector          string `json:"selector,omitempty"`
		OutputCloseFailed bool   `json:"output_close_failed,omitempty"`
		PartialOutput     bool   `json:"partial_output,omitempty"`
		Code              string `json:"code,omitempty"`
		HTTPStatus        int    `json:"http_status,omitempty"`
		OutcomeUncertain  bool   `json:"outcome_uncertain,omitempty"`
	}
	if result.exitCode != 3 || len(result.stdout) != 0 || bytes.Contains(result.stderr, h.bearer) {
		h.t.Fatal("native helper refusal disclosed output or did not exit normally")
	}
	catalogExecutableDecode(h.t, result.stderr, &diagnostic)
	if !strings.HasPrefix(diagnostic.Error, "native delivery ") || diagnostic.Code == "" || diagnostic.HTTPStatus != 0 || diagnostic.OutputCloseFailed || diagnostic.PartialOutput {
		h.t.Fatal("native helper refusal was not fixed and undisclosed")
	}
}

type nativeHelperEnvelope struct {
	Version int             `json:"version"`
	Bearer  string          `json:"bearer"`
	Request json.RawMessage `json:"request"`
}

type nativeHelperSelectResult struct {
	Version   int                       `json:"version"`
	CopyID    string                    `json:"copy_id"`
	Selection catalog.DeliverySelection `json:"selection"`
}

type nativeHelperLocalStatus struct {
	CopyID               string `json:"copy_id"`
	Basename             string `json:"basename"`
	Size                 int64  `json:"size"`
	SHA256               string `json:"sha256"`
	Path                 string `json:"path"`
	PreviouslyDownloaded bool   `json:"previously_downloaded"`
}

func nativeHelperPNG(t *testing.T, seed uint8) ([]byte, [][]byte) {
	t.Helper()
	picture := image.NewRGBA(image.Rect(0, 0, 256, 256))
	for y := 0; y < 256; y++ {
		for x := 0; x < 256; x++ {
			picture.SetRGBA(x, y, color.RGBA{R: uint8(x) + seed, G: uint8(y) + seed*3, B: uint8(x*y) + seed, A: 255})
		}
	}
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, picture); err != nil {
		t.Fatal("encode actual fixture PNG:", err)
	}
	media := append([]byte(nil), encoded.Bytes()...)
	if len(media) < 3 {
		t.Fatal("actual PNG cannot form three nonempty Gateway chunks")
	}
	one := len(media) / 3
	return media, [][]byte{media[:one], media[one : 2*one], media[2*one:]}
}

// The standalone Manager is unprefixed. This top tests the newly built native
// helper as separate processes; it does not claim /manager, TLS, or Finder use.
func TestCatalogDeliveryNativeHelperExecutable(t *testing.T) {
	f := newCatalogExecutableFixture(t)
	f.ok(nil, "upgrade-delivery-lifecycle")
	f.byteGrant()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pngA, partsA := nativeHelperPNG(t, 17)
	sealA, dataA := f.seal(201, partsA)
	if !bytes.Equal(dataA, pngA) {
		t.Fatal("Gateway changed PNG A source bytes")
	}
	revisionA := f.revision(catalogExecutableAsset, dataA, "", "native-helper-actual-revision-A")
	f.bind("native-helper-actual-bind-A", catalogExecutableAsset, revisionA, sealA, nil)
	// A second discoverable asset forces a real opaque next-page token while
	// leaving the deliberate A byte source unchanged.
	secondRevision := f.revision(catalogExecutableEmptyAsset, dataA, "", "native-helper-actual-discovery-second")
	manager, endpoint := catalogManagerStart(t, f)
	clientRoot := filepath.Join(t.TempDir(), "native-helper-profile")
	if clientRoot == f.dir || strings.HasPrefix(clientRoot, f.dir+string(os.PathSeparator)) || strings.HasPrefix(f.dir, clientRoot+string(os.PathSeparator)) {
		t.Fatal("native helper profile overlapped server fixture root")
	}
	originals := &nativeHelperOriginals{t: t, bearer: f.bearer}
	t.Cleanup(func() {
		if originals.started != originals.joined {
			t.Error("native helper original process and pipe joins do not balance")
		}
		imageExecutableRecord(t, map[string]any{"kind": "native_helper_original_counts", "started": originals.started, "joined": originals.joined})
	})
	invoke := func(label, configuredEndpoint string, input []byte, extra []string, words ...string) nativeHelperRunResult {
		t.Helper()
		args := []string{"--native-root", clientRoot, "--endpoint", configuredEndpoint, "--library", string(catalogExecutableLibrary), "--allow-loopback-http", "--timeout", "120s"}
		args = append(args, extra...)
		args = append(args, words...)
		return originals.run(label, input, args...)
	}
	envelope := func(request any) []byte {
		t.Helper()
		var raw json.RawMessage
		switch value := request.(type) {
		case json.RawMessage:
			raw = value
		default:
			raw = catalogExecutableJSON(t, value)
		}
		return catalogExecutableJSON(t, nativeHelperEnvelope{Version: 1, Bearer: string(f.bearer), Request: raw})
	}
	type profileResult struct {
		Version int  `json:"version"`
		Created bool `json:"created"`
	}
	var profile profileResult
	catalogExecutableDecode(t, originals.success(invoke("profile_create", endpoint, nil, nil, "native", "profile", "create")), &profile)
	if profile.Version != 1 || !profile.Created {
		t.Fatal("native helper did not create the exact private profile")
	}
	searchRequest := struct {
		Version         int                  `json:"version"`
		LibraryID       continuity.LibraryID `json:"library_id"`
		Text            string               `json:"text"`
		ContextAssetIDs []continuity.AssetID `json:"context_asset_ids"`
		PageSize        int                  `json:"page_size"`
	}{2, catalogExecutableLibrary, "synthetic executable asset", []continuity.AssetID{}, 1}
	var first, second catalog.DiscoverySearchPage
	catalogExecutableDecode(t, originals.success(invoke("search", endpoint, envelope(searchRequest), nil, "native", "search")), &first)
	if first.Version != 2 || first.Kind != "search" || len(first.Items) != 1 || first.MatchedCount != 2 || first.Next == nil || *first.Next == "" {
		t.Fatal("native helper search did not return a bounded typed first page")
	}
	pageRequest := struct {
		Version   int                  `json:"version"`
		LibraryID continuity.LibraryID `json:"library_id"`
		Token     string               `json:"token"`
	}{2, catalogExecutableLibrary, *first.Next}
	catalogExecutableDecode(t, originals.success(invoke("page", endpoint, envelope(pageRequest), nil, "native", "page")), &second)
	if second.Version != 2 || second.Kind != "search" || len(second.Items) != 1 || second.MatchedCount != 2 || second.Next != nil {
		t.Fatal("native helper page did not finish the original typed search window")
	}
	found := make(map[continuity.AssetID]continuity.RevisionID)
	for _, page := range []catalog.DiscoverySearchPage{first, second} {
		for _, item := range page.Items {
			if item.Head == nil || item.Label != "synthetic executable asset" || len(item.Reasons) == 0 {
				t.Fatal("native helper discovery disclosed an incomplete typed asset")
			}
			if _, exists := found[item.AssetID]; exists {
				t.Fatal("native helper repeated an asset across pages")
			}
			found[item.AssetID] = *item.Head
		}
	}
	if len(found) != 2 || found[catalogExecutableAsset] != revisionA || found[catalogExecutableEmptyAsset] != secondRevision {
		t.Fatal("native helper search/page did not disclose the two stable asset IDs")
	}
	selectedAsset := catalogExecutableAsset
	catalogExecutableCheckpoint(t, "native-helper-actual-search-page-typed-asset-IDs")

	const copyA = "copy_11111111111111111111111111111111"
	const copyB = "copy_22222222222222222222222222222222"
	const invalidCopy = "copy_33333333333333333333333333333333"
	const basename = "approved.png"
	requestA := f.request("native-helper-actual-select-A", selectedAsset, "", int64(len(dataA)))
	invalid := invoke("invalid_select_refusal", endpoint, envelope(json.RawMessage("{}")), []string{"--copy-id", invalidCopy, "--basename", "invalid.png"}, "native", "select")
	originals.refused(invalid)
	if _, err := os.Lstat(filepath.Join(clientRoot, "staging", invalidCopy)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid native select created a private copy directory:", err)
	}
	selectA := func(label, name string) nativeHelperRunResult {
		t.Helper()
		return invoke(label, endpoint, envelope(requestA), []string{"--copy-id", copyA, "--basename", name}, "native", "select")
	}
	var selectedA nativeHelperSelectResult
	catalogExecutableDecode(t, originals.success(selectA("select_A", basename)), &selectedA)
	if selectedA.Version != 1 || selectedA.CopyID != copyA || selectedA.Selection.AssetID != selectedAsset || selectedA.Selection.RevisionID != revisionA || selectedA.Selection.OperationID != requestA.ID || selectedA.Selection.Content.Size != int64(len(dataA)) || selectedA.Selection.Content.SHA256 != sealA.Content.SHA256 {
		t.Fatal("native helper did not persist and return original PNG A selection")
	}
	originals.refused(selectA("changed_name_refusal", "changed.png"))
	var retriedA nativeHelperSelectResult
	catalogExecutableDecode(t, originals.success(selectA("select_A_retry", basename)), &retriedA)
	if retriedA != selectedA {
		t.Fatal("exact native helper select retry changed original A pin")
	}

	pngB, partsB := nativeHelperPNG(t, 63)
	sealB, dataB := f.seal(202, partsB)
	if !bytes.Equal(dataB, pngB) || bytes.Equal(dataA, dataB) {
		t.Fatal("Gateway PNG B source did not differ from A")
	}
	revisionB := f.revision(selectedAsset, dataB, revisionA, "native-helper-actual-revision-B")
	f.bind("native-helper-actual-bind-B", selectedAsset, revisionB, sealB, nil)
	requestB := f.request("native-helper-actual-select-B", selectedAsset, "", int64(len(dataB)))
	var selectedB nativeHelperSelectResult
	catalogExecutableDecode(t, originals.success(invoke("select_B", endpoint, envelope(requestB), []string{"--copy-id", copyB, "--basename", "approved-b.png"}, "native", "select")), &selectedB)
	if selectedB.CopyID != copyB || selectedB.Selection.RevisionID != revisionB || selectedB.Selection.SelectionID == selectedA.Selection.SelectionID {
		t.Fatal("native helper latest selection did not advance to B")
	}
	catalogExecutableDecode(t, originals.success(selectA("select_A_after_B", basename)), &retriedA)
	if retriedA != selectedA {
		t.Fatal("native helper original A pin changed after latest advanced to B")
	}
	catalogExecutableCheckpoint(t, "native-helper-actual-A-pinned-after-B")

	receiveInput := append(append([]byte(nil), f.bearer...), '\n')
	var received nativeHelperLocalStatus
	catalogExecutableDecode(t, originals.success(invoke("receive_A", endpoint, receiveInput, []string{"--copy-id", copyA}, "native", "receive")), &received)
	clear(receiveInput)
	expectedPath := filepath.Join(clientRoot, "copies", copyA, basename)
	if received.CopyID != copyA || received.Basename != basename || received.Path != expectedPath || received.Size != int64(len(dataA)) || received.SHA256 != sealA.Content.SHA256 || !received.PreviouslyDownloaded {
		t.Fatal("native helper receive did not return exact named verified PNG A status")
	}
	info, err := os.Lstat(expectedPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatal("native helper named output is not a private regular file:", err)
	}
	actual, err := os.ReadFile(expectedPath)
	digest := sha256.Sum256(actual)
	if err != nil || !bytes.Equal(actual, dataA) || int64(len(actual)) != received.Size || hex.EncodeToString(digest[:]) != received.SHA256 {
		t.Fatal("native helper named PNG failed independent byte and whole-hash check:", err)
	}
	decoded, err := png.Decode(bytes.NewReader(actual))
	if err != nil || decoded.Bounds() != image.Rect(0, 0, 256, 256) {
		t.Fatal("native helper named media is not a complete PNG:", err)
	}
	for _, source := range []struct {
		seal  ImageSeal
		bytes []byte
	}{{sealA, dataA}, {sealB, dataB}} {
		view, err := f.reader.ImageStatus(ctx, imageRef(source.seal))
		if err != nil || view.Content != source.seal.Content {
			t.Fatal("server Gateway source status changed after native helper receive:", err)
		}
		imageExecutableFullRead(t, ctx, f.reader, view, source.bytes)
	}
	imageExecutableRecord(t, map[string]any{"kind": "native_helper_actual", "event": "named_PNG_verified", "copy_id": copyA, "basename": basename, "selection_id": selectedA.Selection.SelectionID, "revision_id": revisionA, "size": len(actual), "sha256": hex.EncodeToString(digest[:]), "server_source_unchanged": true})
	manager.stopNormally()
	if !manager.waited || !manager.joined || !manager.signaled || manager.stopErr != nil || manager.waitErr != nil || manager.scan.err != nil || manager.scan.listening != 1 {
		t.Fatal("Manager original wait, scanner and normal stop did not complete")
	}
	f.producer.Close()
	f.reader.Close()
	f.gateway.stopNormally()
	catalogExecutableCheckpoint(t, "native-helper-actual-server-originals-joined")

	// A new helper process must re-open the retained local profile with empty
	// stdin after both network peers have joined. It cannot use a live bearer.
	var offline nativeHelperLocalStatus
	catalogExecutableDecode(t, originals.success(invoke("copy_status_offline", endpoint, nil, []string{"--copy-id", copyA}, "native", "copy-status")), &offline)
	if offline != received {
		t.Fatal("fresh offline native helper changed verified PNG status")
	}
	originals.refused(invoke("profile_mismatch_refusal", "http://127.0.0.1:1", nil, []string{"--copy-id", copyA}, "native", "copy-status"))
	imageExecutableRecord(t, map[string]any{"kind": "native_helper_actual", "event": "offline_status", "copy_id": copyA, "basename": basename, "size": offline.Size, "sha256": offline.SHA256, "previously_downloaded": offline.PreviouslyDownloaded, "helper_originals_started": originals.started, "helper_originals_joined": originals.joined, "standalone_unprefixed": true, "finder_qualified": false})
	catalogExecutableCheckpoint(t, "native-helper-actual-offline-named-PNG-status")
}
