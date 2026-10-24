//go:build gateway_acceptance

package mutationgateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
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

// This is a catalog-to-managed-file executable gate, not native/NAS acceptance.
// Build owners supply both original executables and exact hashes. The Gateway
// process, CA/producer/reader setup and shutdown reuse image executable helpers.
// Each catalog invocation is a fresh actual CLI process, never an in-process
// main/handler. Run with -timeout=0 under the retained qualification owner.
//
// Deliberately absent: a deterministic mid-transfer/publication gate. Neither
// product CLI exposes one. Successful sequential revocation checks below must
// not be reported as a concurrent revocation or final-writer-fence qualification.
const catalogExecutableLibrary continuity.LibraryID = "lib_11111111111111111111111111111111"
const catalogExecutableAsset continuity.AssetID = "asset_11111111111111111111111111111111"
const catalogExecutableEmptyAsset continuity.AssetID = "asset_22222222222222222222222222222222"
const catalogExecutableOwner catalog.SubjectID = "sub_11111111111111111111111111111111"
const catalogExecutableReader catalog.SubjectID = "sub_22222222222222222222222222222222"

type catalogExecutableFixture struct {
	t                                             *testing.T
	dir, database, registry, managed, incarnation string
	cfg                                           Config
	certs                                         certificates
	replacement                                   tls.Certificate
	gateway                                       *imageExecutableOriginal
	endpoint                                      string
	producer, reader                              *Client
	producerLease                                 Lease
	bearer                                        []byte
	selector                                      string
	cliStarted, cliJoined                         int
}

type catalogExecutableRoute struct {
	ID              string `json:"id"`
	Endpoint        string `json:"endpoint"`
	CertificateFile string `json:"certificate_file"`
	KeyFile         string `json:"key_file"`
	RootsFile       string `json:"roots_file"`
	StoreID         string `json:"store_id"`
}
type catalogExecutableRegistry struct {
	Version             int                      `json:"version"`
	LibraryID           continuity.LibraryID     `json:"library_id"`
	MaterializationRoot string                   `json:"materialization_root"`
	Routes              []catalogExecutableRoute `json:"routes"`
}
type catalogExecutableResult struct {
	Path    string                         `json:"path"`
	Attempt catalog.MaterializationAttempt `json:"attempt"`
}

func catalogExecutableJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func catalogExecutableDecode(t *testing.T, b []byte, v any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		t.Fatal("invalid executable receipt")
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		t.Fatal("trailing executable receipt")
	}
}
func catalogExecutableCheckpoint(t *testing.T, name string) {
	t.Helper()
	imageExecutableRecord(t, map[string]any{"kind": "catalog_delivery_check", "name": name, "pass": true})
}
func catalogExecutableDirectory(t *testing.T, path string) string {
	t.Helper()
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
func catalogExecutableVerifyBinary(t *testing.T) (string, string) {
	t.Helper()
	path, want := os.Getenv("JM_CATALOG_BINARY"), os.Getenv("JM_CATALOG_BINARY_SHA256")
	if !filepath.IsAbs(path) || !validHash(want) {
		t.Fatal("absolute JM_CATALOG_BINARY and lowercase exact JM_CATALOG_BINARY_SHA256 are required")
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !before.Mode().IsRegular() || before.Mode().Perm()&0111 == 0 {
		t.Fatal("catalog candidate must be a regular executable, not a symlink")
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
	if statErr != nil || readErr != nil || closeErr != nil || afterErr != nil {
		t.Fatal("catalog binary hashing failed")
	}
	if !os.SameFile(before, opened) || !os.SameFile(opened, after) || hex.EncodeToString(h.Sum(nil)) != want {
		t.Fatal("catalog binary identity/hash changed")
	}
	return path, want
}

// bounded output continues draining after the limit so a broken CLI cannot
// deadlock Wait. Contents stay private, including one-time issuance stdout.
// exec.Cmd retains and joins its original stdout/stderr copying goroutines.
// Input uses an explicit private pipe; no bearer/secret is placed in argv/env.
type catalogExecutableBuffer struct {
	data     bytes.Buffer
	overflow bool
}

func (b *catalogExecutableBuffer) Write(p []byte) (int, error) {
	n := len(p)
	available := 65536 - b.data.Len()
	if len(p) > available {
		b.overflow = true
		p = p[:available]
	}
	_, _ = b.data.Write(p)
	return n, nil
}
func (f *catalogExecutableFixture) cli(database string, input []byte, extra ...string) (stdout, stderr []byte, code int) {
	f.t.Helper()
	binary, digest := catalogExecutableVerifyBinary(f.t)
	args := []string{"--catalog", database, "--library", string(catalogExecutableLibrary), "--timeout", "20s"}
	args = append(args, extra...)
	cmd := exec.Command(binary, args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2"}
	if tmp := os.Getenv("TMPDIR"); tmp != "" {
		cmd.Env = append(cmd.Env, "TMPDIR="+tmp)
	}
	read, write, err := os.Pipe()
	if err != nil {
		f.t.Fatal(err)
	}
	var out, diagnostic catalogExecutableBuffer
	cmd.Stdin, cmd.Stdout, cmd.Stderr = read, &out, &diagnostic
	if err = cmd.Start(); err != nil {
		_ = read.Close()
		_ = write.Close()
		f.t.Fatal("catalog executable did not start")
	}
	f.cliStarted++
	imageExecutableRecord(f.t, map[string]any{"kind": "catalog_original", "event": "start", "ordinal": f.cliStarted, "pid": cmd.Process.Pid, "binary_sha256": digest})
	// No fatal assertion is made between successful Start and exact Wait/input
	// join. Close our read descriptor so early CLI exit also unblocks input.
	readCloseErr := read.Close()
	inputDone := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(write, bytes.NewReader(input))
		inputDone <- errors.Join(copyErr, write.Close())
	}()
	waitErr := cmd.Wait()
	// The input goroutine is the sole writer closer. Child exit releases its
	// kernel reader; our parent reader was already closed immediately after Start.
	inputErr := <-inputDone
	f.cliJoined++
	code = -1
	signaled := false
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
			signaled = status.Signaled()
		}
	}
	imageExecutableRecord(f.t, map[string]any{"kind": "catalog_original", "event": "wait_and_pipe_join", "ordinal": f.cliJoined, "pid": cmd.Process.Pid, "exit_code": code, "signaled": signaled, "wait_completed": true, "input_joined": true, "input_error": inputErr != nil})
	if readCloseErr != nil || out.overflow || diagnostic.overflow || signaled || code < 0 {
		f.t.Fatal("catalog original did not produce a bounded normal terminal result")
	}
	if waitErr != nil {
		var exit *exec.ExitError
		if !errors.As(waitErr, &exit) {
			f.t.Fatal("catalog original Wait failed")
		}
	}
	if code == 0 && inputErr != nil {
		f.t.Fatal("successful catalog original did not consume private input")
	}
	return out.data.Bytes(), diagnostic.data.Bytes(), code
}
func (f *catalogExecutableFixture) ok(input []byte, extra ...string) []byte {
	f.t.Helper()
	out, diagnostic, code := f.cli(f.database, input, extra...)
	if code != 0 || len(diagnostic) != 0 {
		f.t.Fatal("catalog command did not succeed cleanly; private output withheld")
	}
	return out
}
func (f *catalogExecutableFixture) denied(input []byte, extra ...string) {
	f.t.Helper()
	before := f.readyNames()
	out, diagnostic, code := f.cli(f.database, input, extra...)
	var failure struct {
		Error             string `json:"error"`
		OperationID       string `json:"operation_id,omitempty"`
		Selector          string `json:"selector,omitempty"`
		OutputCloseFailed bool   `json:"output_close_failed,omitempty"`
		PartialOutput     bool   `json:"partial_output,omitempty"`
	}
	catalogExecutableDecode(f.t, diagnostic, &failure)
	if code != 3 || len(out) != 0 || failure.Error != "catalog operation failed" || failure.OutputCloseFailed || failure.PartialOutput {
		f.t.Fatal("expected clean catalog refusal without a handoff receipt")
	}
	if !bytes.Equal(before, f.readyNames()) {
		f.t.Fatal("refused CLI operation changed managed ready entries")
	}
}
func (f *catalogExecutableFixture) readyNames() []byte {
	f.t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.managed, "ready"))
	if err != nil {
		f.t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return catalogExecutableJSON(f.t, names)
}
func (f *catalogExecutableFixture) admin(id, kind string, value any) catalog.AdminRequest {
	return catalog.AdminRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: id, ExpectedIncarnation: f.incarnation, Kind: kind, Value: catalogExecutableJSON(f.t, value)}
}
func (f *catalogExecutableFixture) credentials(serial int, actions []string) ([]byte, string) {
	f.t.Helper()
	id := fmt.Sprintf("catalog-executable-issue-%d-%d", serial, len(actions))
	value := catalog.IssueCredentialRequest{SubjectID: catalogExecutableReader, Scope: catalog.Scope{Kind: "library"}, Actions: actions, ExpiresUnix: time.Now().Add(30 * time.Minute).Unix()}
	var request any = f.admin(id, "issue_credential", value)
	command := "issue"
	if serial == 2 {
		request = catalog.AdminRequestV2{Version: 2, LibraryID: catalogExecutableLibrary, ID: id, ExpectedIncarnation: f.incarnation, Kind: "issue_credential", Value: catalogExecutableJSON(f.t, value)}
		command = "issue-v2"
	}
	if serial == 3 {
		request = catalog.AdminRequestV3{Version: 3, LibraryID: catalogExecutableLibrary, ID: id, ExpectedIncarnation: f.incarnation, Kind: "issue_credential", Value: catalogExecutableJSON(f.t, value)}
		command = "issue-v3"
	}
	out, diagnostic, code := f.cli(f.database, catalogExecutableJSON(f.t, request), "--secret-out", "-", "credential", command)
	if code != 0 || len(out) != 103 || out[102] != '\n' || !bytes.HasPrefix(out, []byte("jmc1_")) {
		f.t.Fatal("expected one private credential secret")
	}
	// All codecs share these public receipt fields; the version stays explicit.
	var receipt struct {
		Receipt catalog.AdminReceipt `json:"receipt"`
		Status  string               `json:"status"`
	}
	catalogExecutableDecode(f.t, diagnostic, &receipt)
	if receipt.Receipt.Version != serial || receipt.Receipt.Result.Selector == "" {
		f.t.Fatal("wrong credential codec receipt")
	}
	bearer := append([]byte(nil), out[:102]...)
	clear(out)
	f.t.Cleanup(func() { clear(bearer) })
	return bearer, receipt.Receipt.Result.Selector
}
func (f *catalogExecutableFixture) configureReader(cert tls.Certificate) {
	f.t.Helper()
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		f.t.Fatal(err)
	}
	imageExecutableWrite(f.t, filepath.Join(f.dir, "reader.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}))
	imageExecutableWrite(f.t, filepath.Join(f.dir, "reader-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	f.writeRegistry(f.registry, f.managed)
}
func (f *catalogExecutableFixture) writeRegistry(path, managed string) {
	imageExecutableWrite(f.t, path, catalogExecutableJSON(f.t, catalogExecutableRegistry{1, catalogExecutableLibrary, managed, []catalogExecutableRoute{{"gateway", f.endpoint, filepath.Join(f.dir, "reader.pem"), filepath.Join(f.dir, "reader-key.pem"), filepath.Join(f.dir, "ca.pem"), f.cfg.Images.StoreID}}}))
}
func (f *catalogExecutableFixture) gatewayArgs(listen string) []string {
	return []string{"--state", f.cfg.Directory, "--volume", f.cfg.Volume, "--listen", listen, "--tls-cert", filepath.Join(f.dir, "server.pem"), "--tls-key", filepath.Join(f.dir, "server-key.pem"), "--client-ca", filepath.Join(f.dir, "ca.pem"), "--images-config", filepath.Join(f.dir, "images.json")}
}
func newCatalogExecutableFixture(t *testing.T) *catalogExecutableFixture {
	t.Helper()
	_, _ = imageExecutableVerifyBinary(t)
	_, _ = catalogExecutableVerifyBinary(t)
	f := &catalogExecutableFixture{t: t, dir: t.TempDir()}
	t.Cleanup(func() {
		if f.cliStarted != f.cliJoined {
			t.Error("catalog original handle count does not balance")
		}
		imageExecutableRecord(t, map[string]any{"kind": "catalog_original_counts", "started": f.cliStarted, "joined": f.cliJoined})
	})
	if err := os.Chmod(f.dir, 0700); err != nil {
		t.Fatal(err)
	}
	f.database, f.registry = filepath.Join(f.dir, "catalog.sqlite"), filepath.Join(f.dir, "delivery.json")
	f.managed = catalogExecutableDirectory(t, filepath.Join(f.dir, "managed"))
	catalogExecutableDirectory(t, filepath.Join(f.managed, "attempts"))
	catalogExecutableDirectory(t, filepath.Join(f.managed, "ready"))
	f.cfg = Config{Directory: catalogExecutableDirectory(t, filepath.Join(f.dir, "gateway")), Volume: "catalog_executable_volume", LeaseTTL: time.Minute, Images: &ImageConfig{Version: 1, StoreID: "ims_11111111111111111111111111111111", Initialize: true, MaxImages: 8, MaxLogicalBytes: 1 << 30, MaxMetadataBytes: 1 << 20}}
	f.certs, f.replacement = imageExecutableCertificates(t)
	key, err := x509.MarshalPKCS8PrivateKey(f.certs.server.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	imageExecutableWrite(t, filepath.Join(f.dir, "server.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.certs.server.Certificate[0]}))
	imageExecutableWrite(t, filepath.Join(f.dir, "server-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	imageExecutableWrite(t, filepath.Join(f.dir, "ca.pem"), f.certs.caPEM)
	imageExecutableWrite(t, filepath.Join(f.dir, "images.json"), imageJSON(f.cfg.Images))
	f.gateway = imageExecutableStart(t, "catalog-initial", f.gatewayArgs("127.0.0.1:0"))
	f.endpoint = f.gateway.endpoint()
	f.producer = imageExecutableClient(t, f.endpoint, f.certs.a, f.certs.roots)
	f.reader = imageExecutableClient(t, f.endpoint, f.certs.b, f.certs.roots)
	f.configureReader(f.certs.b)
	f.ok(nil, "init-delivery")
	f.ok(nil, "upgrade-delivery") // exact schema-7 retry, not an implicit Open upgrade.
	label := "synthetic executable owner"
	var boot catalog.AdminReceipt
	catalogExecutableDecode(t, f.ok(catalogExecutableJSON(t, f.admin("catalog-executable-bootstrap", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: catalogExecutableOwner, CreateLabel: &label})), "owner", "bootstrap"), &boot)
	f.incarnation = boot.Result.Incarnation
	if f.incarnation == "" {
		t.Fatal("missing authority incarnation")
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: catalogExecutableLibrary, ID: "catalog-executable-metadata-policy", Administration: "local_owner", OperatorSubjectID: catalogExecutableOwner, Changes: []catalog.PolicyChange{
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: catalogExecutableReader, Value: catalog.SubjectValue{Kind: "person", Label: "synthetic executable reader", Enabled: true}}),
		catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "catalog-executable-metadata", Value: catalog.GrantValue{SubjectID: catalogExecutableReader, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"assets.discover", "revisions.read"}}}),
	}}
	f.ok(catalogExecutableJSON(t, f.admin(policy.ID, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy})), "policy", "apply")
	f.bearer, f.selector = f.credentials(3, []string{"assets.discover", "bytes.read", "revisions.read"})
	return f
}
func (f *catalogExecutableFixture) byteGrant() {
	value := catalog.ByteGrantValue{SubjectID: catalogExecutableReader, Scope: catalog.Scope{Kind: "library"}, Action: "bytes.read"}
	request := catalog.BytePolicyRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: "catalog-executable-byte-policy", ExpectedIncarnation: f.incarnation, Changes: []catalog.ByteGrantChange{{Kind: "put", ID: "bgr_11111111111111111111111111111111", Value: &value}}}
	first := f.ok(catalogExecutableJSON(f.t, request), "delivery", "policy", "apply")
	second := f.ok(nil, "--operation-id", request.ID, "delivery", "policy", "status")
	if !bytes.Equal(first, second) {
		f.t.Fatal("byte policy durable receipt changed across CLI processes")
	}
}
func (f *catalogExecutableFixture) revision(asset continuity.AssetID, data []byte, parent continuity.RevisionID, id string) continuity.RevisionID {
	f.t.Helper()
	sum := sha256.Sum256(data)
	manifest := continuity.Manifest{Version: 1, LibraryID: catalogExecutableLibrary, AssetID: asset, Content: continuity.Content{Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}}
	if parent != "" {
		manifest.Parents = []continuity.RevisionID{parent}
	}
	revision, err := manifest.RevisionID()
	if err != nil {
		f.t.Fatal(err)
	}
	changes := []catalog.Change{}
	if parent == "" {
		changes = append(changes, catalog.Change{CreateAsset: &catalog.CreateAsset{AssetID: asset, Label: "synthetic executable asset"}})
	}
	changes = append(changes, catalog.Change{RecordRevision: &manifest}, catalog.Change{AdvanceHead: &catalog.AdvanceHead{AssetID: asset, ExpectedRevision: parent, Revision: revision}})
	// The product has no general Apply CLI. This is the explicit production
	// fixture caller required by the contract; no catalog SQL is edited.
	store, err := catalog.OpenContext(context.Background(), f.database)
	if err != nil {
		f.t.Fatal(err)
	}
	_, applyErr := store.Apply(context.Background(), catalog.Operation{Version: 1, LibraryID: catalogExecutableLibrary, ID: id, Actor: "catalog-executable-fixture", Changes: changes})
	closeErr := store.Close()
	if applyErr != nil || closeErr != nil {
		f.t.Fatal("production Apply/CAS fixture failed", applyErr, closeErr)
	}
	catalogExecutableCheckpoint(f.t, "production-apply-"+id)
	return revision
}
func (f *catalogExecutableFixture) seal(number int, parts [][]byte) (ImageSeal, []byte) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	lease := f.producerLease
	var err error
	if lease.Session == "" {
		lease, err = f.producer.Claim(ctx)
	} else {
		// Claim creates a fresh nonce/session. Keep the original producer
		// admission and renew its exact live epoch for the next image.
		lease, err = f.producer.Renew(ctx, lease)
		if err == nil && (lease.Volume != f.producerLease.Volume || lease.Session != f.producerLease.Session || lease.Epoch != f.producerLease.Epoch) {
			f.t.Fatal("producer renewal changed the owned lease identity")
		}
	}
	if err != nil {
		f.t.Fatal(err)
	}
	f.producerLease = lease
	complete := []byte{}
	receipts := make([]ImageCustodyReceipt, 0, len(parts))
	cohort := fmt.Sprintf("catalog-executable-cohort-%d", number)
	for index, part := range parts {
		sum := sha256.Sum256(part)
		upload := Upload{Chunk{lease.Session, lease.Epoch, fmt.Sprintf("catalog-executable-%d-%d", number, index), cohort, int64(len(complete)), int64(len(part)), hex.EncodeToString(sum[:])}, part}
		receipt, err := f.producer.Upload(ctx, upload)
		if err != nil {
			f.t.Fatal(err)
		}
		receipts = append(receipts, ImageCustody(receipt))
		complete = append(complete, part...)
	}
	sum := sha256.Sum256(complete)
	seal := ImageSeal{1, f.cfg.Images.StoreID, f.cfg.Volume, string(catalogExecutableLibrary), fmt.Sprintf("img_%032x", number), fmt.Sprintf("iop_%032x", number), cohort, hash(f.certs.a.Certificate[0]), hash(f.certs.b.Certificate[0]), ImageContent{int64(len(complete)), hex.EncodeToString(sum[:])}, receipts}
	if _, err := f.producer.SealImage(ctx, seal); err != nil {
		f.t.Fatal(err)
	}
	view, err := f.reader.ImageStatus(ctx, imageRef(seal))
	if err != nil || len(view.Chunks) != len(parts) || view.Content != seal.Content {
		f.t.Fatal("actual producer seal/dedicated reader view failed")
	}
	return seal, complete
}
func (f *catalogExecutableFixture) bind(id string, asset continuity.AssetID, revision continuity.RevisionID, seal ImageSeal, prior *catalog.SourceReceipt) catalog.SourceReceipt {
	f.t.Helper()
	request := catalog.SourceBindRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: id, ExpectedIncarnation: f.incarnation, AssetID: asset, RevisionID: revision, Image: imageRef(seal)}
	if prior != nil {
		request.SourceID, request.ExpectedSourceVersion = prior.SourceID, prior.SourceVersion
	}
	var receipt catalog.SourceReceipt
	catalogExecutableDecode(f.t, f.ok(catalogExecutableJSON(f.t, request), "--delivery-config", f.registry, "--route-id", "gateway", "source", "bind"), &receipt)
	if receipt.RevisionID != revision || receipt.AssetID != asset || !receipt.Enabled || receipt.Content.Size != seal.Content.Size || receipt.Content.SHA256 != seal.Content.SHA256 {
		f.t.Fatal("CLI source binding disagrees with immutable content")
	}
	return receipt
}
func (f *catalogExecutableFixture) request(id string, asset continuity.AssetID, exact continuity.RevisionID, size int64) catalog.DeliverySelectRequest {
	mode := "latest"
	if exact != "" {
		mode = "exact"
	}
	return catalog.DeliverySelectRequest{Version: 1, LibraryID: catalogExecutableLibrary, ID: id, AssetID: asset, Mode: mode, RevisionID: exact, ExpiresUnix: time.Now().Add(10 * time.Minute).Unix(), MaxBytes: size}
}
func (f *catalogExecutableFixture) envelope(request catalog.DeliverySelectRequest, bearer []byte) []byte {
	return catalogExecutableJSON(f.t, struct {
		Version int             `json:"version"`
		Bearer  string          `json:"bearer"`
		Request json.RawMessage `json:"request"`
	}{1, string(bearer), catalogExecutableJSON(f.t, request)})
}
func (f *catalogExecutableFixture) selectDelivery(request catalog.DeliverySelectRequest) catalog.DeliverySelection {
	f.t.Helper()
	var selection catalog.DeliverySelection
	catalogExecutableDecode(f.t, f.ok(f.envelope(request, f.bearer), "--delivery-config", f.registry, "delivery", "select"), &selection)
	return selection
}
func (f *catalogExecutableFixture) assertBytes(path string, expected []byte) {
	f.t.Helper()
	if filepath.Dir(path) != filepath.Join(f.managed, "ready") {
		f.t.Fatal("output escaped the private managed ready directory")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		f.t.Fatal("output is not an owned private regular file")
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		f.t.Fatal(err)
	}
	actualHash, expectedHash := sha256.Sum256(actual), sha256.Sum256(expected)
	if len(actual) != len(expected) || actualHash != expectedHash || !bytes.Equal(actual, expected) {
		f.t.Fatal("independent materialized whole-content bytes/hash mismatch")
	}
}
func (f *catalogExecutableFixture) materialize(selection catalog.DeliverySelection, id string, expected []byte) catalogExecutableResult {
	f.t.Helper()
	var result catalogExecutableResult
	catalogExecutableDecode(f.t, f.ok(f.bearer, "--delivery-config", f.registry, "--selection-id", selection.SelectionID, "--operation-id", id, "delivery", "materialize"), &result)
	if result.Attempt.State != "COMPLETED" || result.Attempt.SelectionID != selection.SelectionID || result.Attempt.Content != selection.Content || result.Attempt.ID == "" || result.Path != filepath.Join(f.managed, "ready", result.Attempt.ID+".payload") {
		f.t.Fatal("CLI did not return a bound completed attempt")
	}
	f.assertBytes(result.Path, expected)
	return result
}

func TestCatalogDeliveryExecutableBytes(t *testing.T) {
	f := newCatalogExecutableFixture(t)
	partsA := [][]byte{bytes.Repeat([]byte("A-start\x00"), 4096), bytes.Repeat([]byte{0, 1, 2, 255}, 64*1024), bytes.Repeat([]byte("A-middle-and-end"), 8192)}
	sealA, dataA := f.seal(1, partsA)
	revisionA := f.revision(catalogExecutableAsset, dataA, "", "catalog-executable-A")
	sourceA := f.bind("catalog-executable-bind-A", catalogExecutableAsset, revisionA, sealA, nil)
	requestA := f.request("catalog-executable-select-A", catalogExecutableAsset, "", int64(len(dataA)))
	// Causal byte-policy oracle: valid codec3 ceiling and metadata grant, but no
	// current byte policy. A full positive select follows the explicit grant.
	f.denied(f.envelope(requestA, f.bearer), "--delivery-config", f.registry, "delivery", "select")
	catalogExecutableCheckpoint(t, "codec3-no-current-byte-grant-denied")
	f.byteGrant()
	selectionA := f.selectDelivery(requestA)
	if selectionA.RevisionID != revisionA || selectionA.Content.SHA256 != sealA.Content.SHA256 {
		t.Fatal("latest A selected different bytes")
	}
	catalogExecutableCheckpoint(t, "latest-selected-A-after-explicit-byte-grant")
	for _, codec := range []int{1, 2, 3} {
		bearer, _ := f.credentials(codec, []string{"assets.discover", "revisions.read"})
		if out := f.ok(bearer, "--action", "revisions.read", "--asset", string(catalogExecutableAsset), "auth", "check"); string(out) != "allowed\n" {
			t.Fatal("metadata credential positive baseline failed")
		}
		request := f.request(fmt.Sprintf("catalog-executable-no-byte-ceiling-%d", codec), catalogExecutableAsset, "", int64(len(dataA)))
		f.denied(f.envelope(request, bearer), "--delivery-config", f.registry, "delivery", "select")
		catalogExecutableCheckpoint(t, fmt.Sprintf("codec%d-metadata-valid-bytes-denied", codec))
	}
	partsB := [][]byte{bytes.Repeat([]byte("B-start\x00"), 4096), bytes.Repeat([]byte{9, 8, 7, 254}, 64*1024), bytes.Repeat([]byte("B-middle-and-end"), 8192)}
	sealB, dataB := f.seal(2, partsB)
	if len(dataA) != len(dataB) || bytes.Equal(dataA, dataB) {
		t.Fatal("pinning fixture must distinguish equal-size A and B")
	}
	revisionB := f.revision(catalogExecutableAsset, dataB, revisionA, "catalog-executable-B")
	f.bind("catalog-executable-bind-B", catalogExecutableAsset, revisionB, sealB, nil)
	selectionB := f.selectDelivery(f.request("catalog-executable-select-B", catalogExecutableAsset, "", int64(len(dataB))))
	if selectionB.RevisionID != revisionB || revisionA == revisionB {
		t.Fatal("fresh latest select did not pin B")
	}
	retryA := f.selectDelivery(requestA) // exact original input, fresh CLI after CAS.
	if !bytes.Equal(catalogExecutableJSON(t, selectionA), catalogExecutableJSON(t, retryA)) {
		t.Fatal("exact latest retry changed A after head advanced to B")
	}
	changed := requestA
	changed.MaxBytes--
	f.denied(f.envelope(changed, f.bearer), "--delivery-config", f.registry, "delivery", "select")
	catalogExecutableCheckpoint(t, "fresh-process-exact-retry-pins-A-after-B-CAS")
	// Restart the real Gateway at the exact same configured route. Existing
	// selections do not acquire an endpoint/config digest from a new registry.
	f.producer.Close()
	f.reader.Close()
	f.gateway.stopNormally()
	// Reopening the journal freezes prior process admission; never renew it.
	f.producerLease = Lease{}
	f.cfg.Images.Initialize = false
	imageExecutableWrite(t, filepath.Join(f.dir, "images.json"), imageJSON(f.cfg.Images))
	f.gateway = imageExecutableStart(t, "catalog-restart", f.gatewayArgs(strings.TrimPrefix(f.endpoint, "https://")))
	if f.gateway.endpoint() != f.endpoint {
		t.Fatal("restart changed the configured route")
	}
	f.producer = imageExecutableClient(t, f.endpoint, f.certs.a, f.certs.roots)
	f.reader = imageExecutableClient(t, f.endpoint, f.certs.b, f.certs.roots)
	var status catalog.DeliverySelection
	catalogExecutableDecode(t, f.ok(f.bearer, "--delivery-config", f.registry, "--selection-id", selectionA.SelectionID, "delivery", "status"), &status)
	if !bytes.Equal(catalogExecutableJSON(t, status), catalogExecutableJSON(t, selectionA)) {
		t.Fatal("fresh CLI status lost pinned A")
	}
	outputA := f.materialize(selectionA, "catalog-executable-materialize-A", dataA)
	outputB := f.materialize(selectionB, "catalog-executable-materialize-B", dataB)
	f.assertBytes(outputA.Path, dataA)
	f.assertBytes(outputB.Path, dataB)
	if outputA.Path == outputB.Path {
		t.Fatal("A/B materialization reused one file")
	}
	retryOutput := f.materialize(selectionA, "catalog-executable-materialize-A", dataA)
	if retryOutput != outputA {
		t.Fatal("fresh CLI materialize retry changed its original completed file")
	}
	catalogExecutableCheckpoint(t, "independent-whole-A-and-B-bytes-after-real-restart")
	// Same inode/size tampering distinguishes whole-content recovery from
	// metadata, path or size-only checks. This is a recovery-hash oracle, not a
	// mid-write corruption oracle for RecordPreparedMaterialization.
	tampered := append([]byte(nil), dataB...)
	tampered[0] ^= 0x20
	beforeTamper, err := os.Stat(outputB.Path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(outputB.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt(tampered, 0)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		t.Fatal("managed-file tamper fixture failed")
	}
	afterTamper, err := os.Stat(outputB.Path)
	if err != nil || !os.SameFile(beforeTamper, afterTamper) || beforeTamper.Size() != afterTamper.Size() {
		t.Fatal("tamper fixture changed the original inode or size")
	}
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", selectionB.SelectionID, "--operation-id", "catalog-executable-materialize-B", "delivery", "reconcile")
	catalogExecutableCheckpoint(t, "same-size-ready-content-tamper-reconcile-denied")
	// Empty content still travels through authenticated status, source bind,
	// selection, typed preparation/publication and an independently read file.
	emptySeal, empty := f.seal(3, [][]byte{})
	emptyRevision := f.revision(catalogExecutableEmptyAsset, empty, "", "catalog-executable-empty")
	f.bind("catalog-executable-bind-empty", catalogExecutableEmptyAsset, emptyRevision, emptySeal, nil)
	emptySelection := f.selectDelivery(f.request("catalog-executable-select-empty", catalogExecutableEmptyAsset, "", 0))
	f.materialize(emptySelection, "catalog-executable-materialize-empty", empty)
	catalogExecutableCheckpoint(t, "authenticated-empty-image-materialized-and-independently-read")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := f.producer.RebindImageReader(ctx, ImageReaderChange{1, sealA.StoreID, sealA.ImageID, "irg_11111111111111111111111111111111", 1, nil}); err != nil {
		t.Fatal(err)
	}
	_, err = f.reader.ImageStatus(ctx, imageRef(sealA))
	requireHTTP(t, err, "403")
	revoked := f.selectDelivery(f.request("catalog-executable-select-revoked-reader", catalogExecutableAsset, revisionA, int64(len(dataA))))
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", revoked.SelectionID, "--operation-id", "catalog-executable-materialize-revoked-reader", "delivery", "materialize")
	f.assertBytes(outputA.Path, dataA) // delivered bytes are retained, not retracted.
	catalogExecutableCheckpoint(t, "gateway-reader-revocation-refuses-new-output-preserves-completed-A")
	leaf := hash(f.replacement.Certificate[0])
	if _, err := f.producer.RebindImageReader(ctx, ImageReaderChange{1, sealA.StoreID, sealA.ImageID, "irg_22222222222222222222222222222222", 2, &leaf}); err != nil {
		t.Fatal(err)
	}
	_, err = f.reader.ImageStatus(ctx, imageRef(sealA))
	requireHTTP(t, err, "403")
	fresh := imageExecutableClient(t, f.endpoint, f.replacement, f.certs.roots)
	view, err := fresh.ImageStatus(ctx, imageRef(sealA))
	if err != nil || view.Generation != 3 {
		t.Fatal("replacement reader positive baseline failed")
	}
	imageExecutableFullRead(t, ctx, fresh, view, dataA)
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", selectionA.SelectionID, "--operation-id", "catalog-executable-materialize-A", "delivery", "reconcile")
	f.configureReader(f.replacement)
	newSource := f.bind("catalog-executable-reverify-A", catalogExecutableAsset, revisionA, sealA, &sourceA)
	if newSource.SourceID != sourceA.SourceID || newSource.SourceVersion != sourceA.SourceVersion+1 {
		t.Fatal("explicit source reverify did not CAS the original source")
	}
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", selectionA.SelectionID, "delivery", "status")
	freshA := f.selectDelivery(f.request("catalog-executable-select-reverified-A", catalogExecutableAsset, revisionA, int64(len(dataA))))
	f.materialize(freshA, "catalog-executable-materialize-reverified-A", dataA)
	catalogExecutableCheckpoint(t, "replacement-leaf-needs-explicit-source-CAS-and-fresh-selection")
	// Corrupt only the synthetic committed custody artifact, using the same
	// production leaf naming helper as existing Gateway corruption tests.
	corruptSelection := f.selectDelivery(f.request("catalog-executable-select-corrupt-A", catalogExecutableAsset, revisionA, int64(len(dataA))))
	artifact := filepath.Join(f.cfg.Directory, (&Gateway{volume: f.cfg.Volume}).leaf(format+":"+sealA.Receipts[1].Chunk.ID))
	imageExecutableWrite(t, artifact, []byte("synthetic corrupt custody artifact"))
	_, err = fresh.ImageReadback(ctx, view, 1)
	requireHTTP(t, err, "409")
	if !strings.Contains(err.Error(), "custody_integrity_unconfirmed") {
		t.Fatal("corrupt custody did not produce its intended integrity refusal")
	}
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", corruptSelection.SelectionID, "--operation-id", "catalog-executable-materialize-corrupt-A", "delivery", "materialize")
	catalogExecutableCheckpoint(t, "real-custody-corruption-refuses-managed-ready-output")
	// A separate actual CLI commits the ordinary existing revoke. This proves
	// post-commit refusal in a fresh process; no concurrency claim is made.
	revoke := f.admin("catalog-executable-revoke", "revoke_credential", catalog.RevokeCredentialRequest{Selector: f.selector, ExpectedGeneration: 1})
	f.ok(catalogExecutableJSON(t, revoke), "credential", "revoke")
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", freshA.SelectionID, "delivery", "status")
	f.assertBytes(outputA.Path, dataA)
	catalogExecutableCheckpoint(t, "separate-CLI-credential-revoke-commits-and-fresh-status-denies")
	fresh.Close()
	f.producer.Close()
	f.reader.Close()
	cancel()
	f.gateway.stopNormally()
	imageExecutableRecord(t, map[string]any{"kind": "catalog_delivery_scope", "multi_chunk_images": 2, "chunks_per_nonempty_image": 3, "empty_images": 1, "required_boundary_complete": false, "gap": "deterministically gated transfer/final-exposure authority schedules and prepared-file whole-hash causal oracle remain unqualified"})
}

// This separately named test is deliberately red until the format-5 implementation
// is integrated. It uses only public CLI/Store semantics, never stream internals.
// Its absence from a selected run is an explicit missing required boundary case.
func TestCatalogDeliveryExecutableInactiveRestore(t *testing.T) {
	f := newCatalogExecutableFixture(t)
	seal, data := f.seal(1, [][]byte{[]byte("restore first\x00"), []byte("restore middle\xff"), []byte("restore last")})
	revision := f.revision(catalogExecutableAsset, data, "", "catalog-executable-restore-A")
	f.bind("catalog-executable-restore-bind", catalogExecutableAsset, revision, seal, nil)
	f.byteGrant()
	selection := f.selectDelivery(f.request("catalog-executable-restore-select", catalogExecutableAsset, "", int64(len(data))))
	f.materialize(selection, "catalog-executable-restore-materialize", data)
	stream := filepath.Join(f.dir, "metadata.export")
	budgets := []string{"--max-bytes", "67108864", "--max-events", "10000", "--max-rows", "10000"}
	exportArgs := append(append([]string{}, budgets...), "--output", stream, "export")
	f.ok(nil, exportArgs...)
	restored := filepath.Join(f.dir, "restored.sqlite")
	restoreArgs := append(append([]string{}, budgets...), "--input", stream, "restore")
	out, diagnostic, code := f.cli(restored, nil, restoreArgs...)
	if code != 0 || len(out) != 0 || len(diagnostic) != 0 {
		t.Fatal("public format5 Restore did not complete cleanly")
	}
	store, err := catalog.OpenContext(context.Background(), restored)
	if err != nil {
		t.Fatal(err)
	}
	owner, ownerErr := store.LocalOwner(catalogExecutableLibrary)
	var authority catalog.AuthorityStatus
	var statusErr error
	if ownerErr == nil {
		authority, statusErr = owner.Status(context.Background())
	}
	_, authErr := store.Authenticate(context.Background(), catalogExecutableLibrary, f.bearer)
	got, revisionErr := store.GetRevision(context.Background(), catalogExecutableLibrary, revision)
	closeErr := store.Close()
	if ownerErr != nil || statusErr != nil || closeErr != nil || authority.Incarnation != "" || authority.OwnerSubjectID != "" || !errors.Is(authErr, catalog.ErrUnauthenticated) || revisionErr != nil || got.Manifest.Content != selection.Content {
		t.Fatal("restored metadata is not inactive with preserved revision")
	}
	out, _, code = f.cli(restored, f.bearer, "auth", "whoami")
	if code != 2 || string(out) != "denied\n" {
		t.Fatal("restored catalog imported live credential authority")
	}
	// Owner bootstrap and fresh issuance still cannot recreate the omitted
	// operational source/selection/materialization state from portable policy.
	oldDatabase := f.database
	f.database = restored
	var recovery catalog.AdminReceipt
	recoverRequest := f.admin("catalog-executable-restore-bootstrap", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: catalogExecutableOwner})
	recoverRequest.ExpectedIncarnation = ""
	catalogExecutableDecode(t, f.ok(catalogExecutableJSON(t, recoverRequest), "owner", "bootstrap"), &recovery)
	f.incarnation = recovery.Result.Incarnation
	if f.incarnation == "" {
		t.Fatal("explicit restore bootstrap did not establish fresh authority")
	}
	f.bearer, _ = f.credentials(3, []string{"assets.discover", "bytes.read", "revisions.read"})
	if out := f.ok(f.bearer, "--action", "revisions.read", "--asset", string(catalogExecutableAsset), "auth", "check"); string(out) != "allowed\n" {
		t.Fatal("restored metadata policy positive baseline failed")
	}
	freshRoot := catalogExecutableDirectory(t, filepath.Join(f.dir, "restored-managed"))
	catalogExecutableDirectory(t, filepath.Join(freshRoot, "attempts"))
	catalogExecutableDirectory(t, filepath.Join(freshRoot, "ready"))
	f.managed = freshRoot
	f.registry = filepath.Join(f.dir, "restored-delivery.json")
	f.writeRegistry(f.registry, f.managed)
	f.denied(f.envelope(f.request("catalog-executable-restored-no-source", catalogExecutableAsset, revision, int64(len(data))), f.bearer), "--delivery-config", f.registry, "delivery", "select")
	f.denied(f.bearer, "--delivery-config", f.registry, "--selection-id", selection.SelectionID, "delivery", "status")
	if string(f.readyNames()) != "[]" {
		t.Fatal("restore created managed byte output")
	}
	catalogExecutableCheckpoint(t, "format5-restores-policy-and-revision-without-live-authority-or-source-selection-output")
	// No new policy grant is applied: only explicit fresh local source verification
	// follows. Delivery can now prove both metadata and byte policy survived.
	f.bind("catalog-executable-restored-explicit-bind", catalogExecutableAsset, revision, seal, nil)
	freshSelection := f.selectDelivery(f.request("catalog-executable-restored-explicit-select", catalogExecutableAsset, revision, int64(len(data))))
	f.materialize(freshSelection, "catalog-executable-restored-explicit-materialize", data)
	catalogExecutableCheckpoint(t, "restored-byte-policy-delivers-only-after-explicit-bootstrap-issuance-config-and-source-bind")
	f.database = oldDatabase
	f.producer.Close()
	f.reader.Close()
	f.gateway.stopNormally()
}
