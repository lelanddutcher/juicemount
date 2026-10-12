//go:build storageacceptance && (linux || darwin)

package miniodeploy

// This is a destructive test ONLY of generated objects in a controller-owned,
// disposable local fixture. It never creates a bucket or starts/stops a server.
// Run just TestStorageCompatibility, serially, with -count=1 and a fresh root.
//
// Controller admission (all paths must be real, not symlinks):
//   JM_STORAGE_ACCEPTANCE_ROOT: absolute owned 0700 directory, initially holding
//     only admission.json (owned regular 0600 file, at most 4096 bytes).
//   JM_STORAGE_ACCEPTANCE_NONCE: 32 cryptographically random bytes as 64 hex digits.
//   JM_STORAGE_ACCEPTANCE_{CLIENT,GC}_{ACCESS_KEY,SECRET_KEY}: generated fixture
//     credentials with exactly jm-client-policy.json / jm-gc-policy.json rights.
//
// admission.json has exactly the storageAdmission fields below. endpoint is
// http://<literal-loopback-IP>:<explicit-port>/zpool. The controller records the
// SHA256 of accessKey + NUL + secretKey for each generated credential pair.
// test_name is TestStorageCompatibility; test_binary_sha256 binds the exact
// compiled executable whose source/build provenance the controller retains.
// It must establish ownership of that endpoint; a marker is not host discovery.
// No inherited AWS_*, MINIO_*, MC_*, JFS_* or proxy settings are accepted.
//
// consumed.json is exclusively created before networking. Never reuse that root
// or retry an uncertain operation. Failure retains objects/uploads and reports
// FAIL_OR_UNKNOWN_RETAIN_FIXTURE. The controller owns eventual fixture retirement.
// Ordinary completion explicitly removes only this run's generated objects.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/juicedata/juicefs/pkg/object"
)

type storageAdmission struct {
	Schema       int    `json:"schema"`
	Purpose      string `json:"purpose"`
	Disposable   bool   `json:"disposable"`
	Endpoint     string `json:"endpoint"`
	Nonce        string `json:"nonce"`
	CreatedUnix  int64  `json:"created_unix"`
	ClientSHA256 string `json:"client_sha256"`
	GCSHA256     string `json:"gc_sha256"`
	TestName     string `json:"test_name"`
	BinarySHA256 string `json:"test_binary_sha256"`
}

type storageHTTPEvent struct {
	Method        string `json:"method"`
	Case          string `json:"case"`
	Status        int    `json:"status"`
	Failed        bool   `json:"transport_failed"`
	Range         string `json:"request_range"`
	ContentRange  string `json:"content_range"`
	ContentLength int64  `json:"content_length"`
}

type storageReceipt struct {
	Schema         int                `json:"schema"`
	Nonce          string             `json:"nonce"`
	Prefix         string             `json:"prefix"`
	State          string             `json:"state"`
	Checks         []string           `json:"checks"`
	Events         []storageHTTPEvent `json:"http_events"`
	BinarySHA256   string             `json:"test_binary_sha256"`
	DeclaredChecks int                `json:"declared_checks"`
	CleanupState   string             `json:"object_cleanup_state"`
}

type storageFixture struct {
	root, endpoint, nonce   string
	clientKey, clientSecret string
	gcKey, gcSecret         string
	binarySHA256            string
}

func storageOwned(info os.FileInfo, mode os.FileMode) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && st.Uid == uint32(os.Geteuid()) && info.Mode().Perm() == mode && info.Mode()&os.ModeSymlink == 0
}

func storageAdmit(t *testing.T) storageFixture {
	t.Helper()
	var f storageFixture
	f.root = os.Getenv("JM_STORAGE_ACCEPTANCE_ROOT")
	f.nonce = os.Getenv("JM_STORAGE_ACCEPTANCE_NONCE")
	f.clientKey = os.Getenv("JM_STORAGE_ACCEPTANCE_CLIENT_ACCESS_KEY")
	f.clientSecret = os.Getenv("JM_STORAGE_ACCEPTANCE_CLIENT_SECRET_KEY")
	f.gcKey = os.Getenv("JM_STORAGE_ACCEPTANCE_GC_ACCESS_KEY")
	f.gcSecret = os.Getenv("JM_STORAGE_ACCEPTANCE_GC_SECRET_KEY")
	hex64 := regexp.MustCompile(`^[0-9a-f]{64}$`)
	if !filepath.IsAbs(f.root) || filepath.Clean(f.root) != f.root || !hex64.MatchString(f.nonce) {
		t.Fatal("explicit absolute fixture root and 256-bit controller nonce required")
	}
	real, err := filepath.EvalSymlinks(f.root)
	if err != nil || real != f.root {
		t.Fatal("fixture root must exist without symlink components")
	}
	info, err := os.Lstat(f.root)
	if err != nil || !info.IsDir() || !storageOwned(info, 0700) {
		t.Fatal("fixture root must be an owned 0700 directory")
	}
	entries, err := os.ReadDir(f.root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "admission.json" {
		t.Fatal("fresh fixture root must contain only admission.json; never reuse consumed fixtures")
	}
	marker := filepath.Join(f.root, "admission.json")
	before, err := os.Lstat(marker)
	if err != nil || !before.Mode().IsRegular() || !storageOwned(before, 0600) || before.Size() > 4096 {
		t.Fatal("admission marker must be an owned bounded regular 0600 file")
	}
	file, err := os.OpenFile(marker, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal("cannot open admission marker")
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("admission marker identity changed")
	}
	var a storageAdmission
	decoder := json.NewDecoder(io.LimitReader(file, 4097))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&a) != nil || decoder.Decode(new(any)) != io.EOF {
		t.Fatal("invalid admission marker")
	}
	if a.Schema != 1 || a.Purpose != "juicemount-storage-compatibility" || !a.Disposable || a.Nonce != f.nonce {
		t.Fatal("fixture admission does not match this test")
	}
	if a.TestName != "TestStorageCompatibility" || !hex64.MatchString(a.BinarySHA256) {
		t.Fatal("admission must bind the exact test name and executable")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal("cannot identify test executable")
	}
	binary, err := os.Open(executable)
	if err != nil {
		t.Fatal("cannot read test executable identity")
	}
	hash := sha256.New()
	_, hashErr := io.Copy(hash, binary)
	closeErr := binary.Close()
	f.binarySHA256 = hex.EncodeToString(hash.Sum(nil))
	if hashErr != nil || closeErr != nil || f.binarySHA256 != a.BinarySHA256 {
		t.Fatal("test executable differs from fixture admission")
	}
	age := time.Since(time.Unix(a.CreatedUnix, 0))
	if age < 0 || age > 15*time.Minute {
		t.Fatal("fixture admission must be freshly issued within 15 minutes")
	}
	u, err := url.Parse(a.Endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || u.Path != "/zpool" {
		t.Fatal("fixture endpoint must be literal loopback HTTP with only the /zpool bucket path")
	}
	ip := net.ParseIP(u.Hostname())
	port, err := strconv.Atoi(u.Port())
	if ip == nil || !ip.IsLoopback() || err != nil || port < 1 || port > 65535 || u.Host != net.JoinHostPort(ip.String(), strconv.Itoa(port)) {
		t.Fatal("fixture endpoint requires a canonical literal loopback IP and explicit port")
	}
	f.endpoint = a.Endpoint
	keyRE := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{2,63}$`)
	secretRE := regexp.MustCompile(`^[A-Za-z0-9]{32,40}$`)
	if !keyRE.MatchString(f.clientKey) || !keyRE.MatchString(f.gcKey) || f.clientKey == f.gcKey || !secretRE.MatchString(f.clientSecret) || !secretRE.MatchString(f.gcSecret) || f.clientSecret == f.gcSecret {
		t.Fatal("distinct generated test-only client and GC credentials required")
	}
	fingerprint := func(key, secret string) string {
		sum := sha256.Sum256([]byte(key + "\x00" + secret))
		return hex.EncodeToString(sum[:])
	}
	if !hex64.MatchString(a.ClientSHA256) || !hex64.MatchString(a.GCSHA256) || a.ClientSHA256 != fingerprint(f.clientKey, f.clientSecret) || a.GCSHA256 != fingerprint(f.gcKey, f.gcSecret) {
		t.Fatal("credentials are not bound to this disposable fixture admission")
	}
	for _, variable := range os.Environ() {
		name, value, _ := strings.Cut(variable, "=")
		upper := strings.ToUpper(name)
		if value != "" && (strings.HasPrefix(upper, "AWS_") || strings.HasPrefix(upper, "MINIO_") || strings.HasPrefix(upper, "MC_") || strings.HasPrefix(upper, "JFS_") || strings.HasSuffix(upper, "_PROXY")) {
			t.Fatal("inherited storage or proxy settings must be absent")
		}
	}
	// Suppress shared profile, credential, metadata and endpoint fallback. Static
	// fixture credentials still enter through CreateStorage's explicit arguments.
	for name, value := range map[string]string{
		"AWS_CONFIG_FILE": os.DevNull, "AWS_SHARED_CREDENTIALS_FILE": os.DevNull,
		"AWS_EC2_METADATA_DISABLED": "true", "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "true",
		"MINIO_REGION": "us-east-1", "JFS_S3_VHOST_STYLE": "false",
	} {
		t.Setenv(name, value)
	}
	return f
}

func storageWriteExclusive(root, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(root, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(data, '\n'))
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// The underlying JuiceFS client still signs and performs every operation. This
// wrapper only refuses traffic outside this fixture and records HTTP outcomes;
// it never rewrites an authentication header or a request payload.
type storageTransport struct {
	base              *http.Transport
	host, prefix      string
	keys              map[string]string
	mu                sync.Mutex
	events            []storageHTTPEvent
	abortKey, abortID string
	abortEvents       []storageHTTPEvent
}

func (s *storageTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	query := r.URL.Query()
	label, keyOK := s.keys[strings.TrimPrefix(r.URL.Path, "/zpool/")]
	keyOK = keyOK && strings.HasPrefix(r.URL.Path, "/zpool/")
	bucketList := (r.URL.Path == "/zpool" || r.URL.Path == "/zpool/") && r.Method == http.MethodGet && query.Get("list-type") == "2" && query.Get("prefix") == s.prefix
	if r.URL.Scheme != "http" || r.URL.Host != s.host || r.URL.User != nil || (!keyOK && !bucketList) {
		return nil, errors.New("request outside admitted fixture refused")
	}
	if bucketList {
		label = "prefix-list"
	}
	response, err := s.base.RoundTrip(r)
	event := storageHTTPEvent{Method: r.Method, Case: label, Failed: err != nil,
		Range: r.Header.Get("Range"), ContentLength: -1}
	if response != nil {
		event.Status = response.StatusCode
		event.ContentRange = response.Header.Get("Content-Range")
		event.ContentLength = response.ContentLength
	}
	s.mu.Lock()
	s.events = append(s.events, event)
	if r.Method == http.MethodDelete && r.URL.Path == "/zpool/"+s.abortKey && query.Get("uploadId") == s.abortID && s.abortID != "" {
		s.abortEvents = append(s.abortEvents, event)
	}
	s.mu.Unlock()
	return response, err
}

func storageReadExact(t *testing.T, s object.ObjectStorage, key string, want []byte, transport *storageTransport) {
	t.Helper()
	head, err := s.Head(key)
	if err != nil || head == nil || head.Key() != key || head.Size() != int64(len(want)) || head.IsDir() {
		t.Fatal("HEAD did not match generated object; retain fixture")
	}
	read := func(offset, length int64, expected []byte) {
		transport.mu.Lock()
		start := len(transport.events)
		transport.mu.Unlock()
		r, err := s.Get(key, offset, length)
		if err != nil || r == nil {
			t.Fatal("GET failed; retain fixture")
		}
		got, readErr := io.ReadAll(io.LimitReader(r, int64(len(expected))+1))
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(got, expected) {
			t.Fatal("GET bytes or length differed; retain fixture")
		}
		transport.mu.Lock()
		events := append([]storageHTTPEvent(nil), transport.events[start:]...)
		transport.mu.Unlock()
		if len(events) != 1 || events[0].Method != http.MethodGet || events[0].Case != transport.keys[key] || events[0].Failed {
			t.Fatal("GET did not have one attributable response; retain fixture")
		}
		event := events[0]
		if event.ContentLength != int64(len(expected)) {
			t.Fatal("GET response length differed; retain fixture")
		}
		if length < 0 {
			if event.Status != http.StatusOK || event.Range != "" || event.ContentRange != "" {
				t.Fatal("full GET status or range headers differed; retain fixture")
			}
		} else {
			end := offset + length - 1
			if event.Status != http.StatusPartialContent ||
				event.Range != fmt.Sprintf("bytes=%d-%d", offset, end) ||
				event.ContentRange != fmt.Sprintf("bytes %d-%d/%d", offset, end, len(want)) {
				t.Fatal("ranged GET lacked exact 206/Content-Range; retain fixture")
			}
		}
	}
	read(0, -1, want)
	if len(want) > 0 {
		read(0, 1, want[:1])
		middle := len(want) / 2
		length := min(63, len(want)-middle)
		read(int64(middle), int64(length), want[middle:middle+length])
		read(int64(len(want)-1), 1, want[len(want)-1:])
	}
}

func storageListExact(t *testing.T, s object.ObjectStorage, prefix string, want map[string]int64) {
	t.Helper()
	got := make(map[string]int64)
	token := ""
	seen := make(map[string]bool)
	for page := 0; page < 16; page++ {
		items, more, next, err := s.List(prefix, "", token, "", 2, false)
		if err != nil {
			t.Fatal("prefix LIST failed; retain fixture")
		}
		for _, item := range items {
			if item == nil || !strings.HasPrefix(item.Key(), prefix) || item.IsDir() {
				t.Fatal("LIST returned an unexpected object; retain fixture")
			}
			if _, exists := got[item.Key()]; exists {
				t.Fatal("LIST returned a duplicate object; retain fixture")
			}
			got[item.Key()] = item.Size()
		}
		if !more {
			if len(got) != len(want) {
				t.Fatal("LIST inventory count differs; retain fixture")
			}
			for key, size := range want {
				actual, ok := got[key]
				if !ok || actual != size {
					t.Fatal("LIST key or size differs; retain fixture")
				}
			}
			return
		}
		if next == "" || seen[next] {
			t.Fatal("LIST continuation made no progress; retain fixture")
		}
		seen[next], token = true, next
	}
	t.Fatal("LIST exceeded bounded page count; retain fixture")
}

func storageRandomBytes(t *testing.T, size int) []byte {
	t.Helper()
	body := make([]byte, size)
	if _, err := rand.Read(body); err != nil {
		t.Fatal("cannot generate nonperiodic fixture content")
	}
	return body
}

func TestStorageCompatibility(t *testing.T) {
	f := storageAdmit(t)
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("cannot generate isolated object prefix")
	}
	prefix := "storage-acceptance/" + f.nonce + "/" + hex.EncodeToString(random[:]) + "/"
	receipt := storageReceipt{Schema: 1, Nonce: f.nonce, Prefix: prefix, State: "RUNNING",
		BinarySHA256: f.binarySHA256, DeclaredChecks: 10, CleanupState: "NOT_STARTED",
		Checks: []string{}, Events: []storageHTTPEvent{}}
	if storageWriteExclusive(f.root, "consumed.json", receipt) != nil {
		t.Fatal("cannot consume fresh admission; no networking permitted")
	}
	endpoint, _ := url.Parse(f.endpoint)
	keys := map[string]string{
		prefix + "zero.bin": "zero", prefix + "素材/café 01.bin": "unicode",
		prefix + "4MiB.bin": "4MiB", prefix + "create-only.bin": "create-only",
		prefix + "multipart.bin": "multipart", prefix + "abort.bin": "abort",
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &storageTransport{host: endpoint.Host, prefix: prefix, keys: keys}
	transport.base = &http.Transport{Proxy: nil, DisableCompression: true, ResponseHeaderTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if address != endpoint.Host {
				return nil, errors.New("dial outside admitted endpoint refused")
			}
			return dialer.DialContext(ctx, "tcp", endpoint.Host)
		},
	}
	// This tagged test is serial: it temporarily owns the SDK's shared client.
	httpClient := object.GetHttpClient()
	previous := *httpClient
	*httpClient = http.Client{Transport: transport, Timeout: 20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	t.Cleanup(func() {
		transport.base.CloseIdleConnections()
		*httpClient = previous
		if t.Failed() || receipt.State != "PASS" {
			receipt.State = "FAIL_OR_UNKNOWN_RETAIN_FIXTURE"
		}
		transport.mu.Lock()
		receipt.Events = append([]storageHTTPEvent{}, transport.events...)
		transport.mu.Unlock()
		if storageWriteExclusive(f.root, "result.json", receipt) != nil {
			t.Error("result publication failed; retain consumed marker and fixture")
		}
	})
	client, err := object.CreateStorage("minio", f.endpoint, f.clientKey, f.clientSecret, "")
	if err != nil {
		t.Fatal("cannot construct bundled client storage")
	}
	gc, err := object.CreateStorage("minio", f.endpoint, f.gcKey, f.gcSecret, "")
	if err != nil {
		t.Fatal("cannot construct bundled GC storage")
	}
	defer object.Shutdown(client)
	defer object.Shutdown(gc)
	want := make(map[string]int64)
	run := func(name string, fn func(*testing.T)) bool {
		entered, completed := false, false
		ok := t.Run(name, func(t *testing.T) {
			entered = true
			fn(t)
			completed = !t.Failed() && !t.Skipped()
		})
		if !ok || !entered || !completed {
			if !entered || !completed {
				t.Error("required compatibility case did not complete; retain fixture")
			}
			return false // No further mutations or cleanup after failed/unknown work.
		}
		receipt.Checks = append(receipt.Checks, name)
		return true
	}
	if !run("fresh_prefix", func(t *testing.T) { storageListExact(t, client, prefix, want) }) {
		return
	}
	fixtures := []struct {
		name, leaf string
		body       []byte
	}{
		{"zero", "zero.bin", []byte{}},
		{"unicode", "素材/café 01.bin", append([]byte("JuiceMount 素材 café\n"), storageRandomBytes(t, 1024)...)},
		{"4MiB", "4MiB.bin", storageRandomBytes(t, 4<<20)},
	}
	for _, fixture := range fixtures {
		if !run("client_"+fixture.name+"_put_read_range_head", func(t *testing.T) {
			key := prefix + fixture.leaf
			if client.Put(key, bytes.NewReader(fixture.body)) != nil {
				t.Fatal("client PUT failed; retain fixture")
			}
			storageReadExact(t, client, key, fixture.body, transport)
			want[key] = int64(len(fixture.body))
		}) {
			return
		}
	}
	if !run("create_only_absent_and_existing", func(t *testing.T) {
		createOnly, ok := client.(object.SupportCreateOnly)
		if !ok {
			t.Fatal("bundled provider does not implement SupportCreateOnly")
		}
		key, body := prefix+"create-only.bin", []byte("accepted original bytes")
		if createOnly.PutIfAbsent(key, bytes.NewReader(body)) != nil {
			t.Fatal("absent-key PutIfAbsent failed; retain fixture")
		}
		storageReadExact(t, client, key, body, transport)
		if !errors.Is(createOnly.PutIfAbsent(key, bytes.NewReader([]byte("different replacement bytes"))), os.ErrExist) {
			t.Fatal("existing-key PutIfAbsent did not return os.ErrExist; retain fixture")
		}
		storageReadExact(t, client, key, body, transport)
		want[key] = int64(len(body))
	}) {
		return
	}
	if !run("client_multipart_completion", func(t *testing.T) {
		key := prefix + "multipart.bin"
		limits := client.Limits()
		if !limits.IsSupportMultipartUpload || limits.MinPartSize != 5<<20 || limits.MaxPartCount < 2 {
			t.Fatal("unexpected multipart capabilities; no reduced coverage permitted")
		}
		upload, err := client.CreateMultipartUpload(key)
		if err != nil || upload == nil || upload.UploadID == "" || upload.MinPartSize != 5<<20 || upload.MaxCount < 2 {
			t.Fatal("multipart create failed or uncertain; retain fixture")
		}
		first, last := storageRandomBytes(t, 5<<20), storageRandomBytes(t, 137)
		parts := make([]*object.Part, 2)
		for i, body := range [][]byte{first, last} {
			parts[i], err = client.UploadPart(key, upload.UploadID, i+1, body)
			if err != nil || parts[i] == nil || parts[i].Num != i+1 || parts[i].ETag == "" {
				t.Fatal("multipart part failed or uncertain; retain fixture")
			}
		}
		if client.CompleteUpload(key, upload.UploadID, parts) != nil {
			t.Fatal("multipart completion failed or uncertain; retain fixture")
		}
		body := append(first, last...)
		storageReadExact(t, client, key, body, transport)
		want[key] = int64(len(body))
	}) {
		return
	}
	if !run("gc_multipart_abort", func(t *testing.T) {
		key := prefix + "abort.bin"
		upload, err := gc.CreateMultipartUpload(key)
		if err != nil || upload == nil || upload.UploadID == "" {
			t.Fatal("abort-fixture multipart create failed or uncertain; retain fixture")
		}
		part, err := gc.UploadPart(key, upload.UploadID, 1, []byte("owned abort-fixture part"))
		if err != nil || part == nil || part.Num != 1 || part.ETag == "" {
			t.Fatal("abort-fixture part failed or uncertain; retain fixture")
		}
		transport.mu.Lock()
		transport.abortKey, transport.abortID = key, upload.UploadID
		transport.mu.Unlock()
		gc.AbortUpload(key, upload.UploadID)
		transport.mu.Lock()
		events := append([]storageHTTPEvent(nil), transport.abortEvents...)
		transport.mu.Unlock()
		if len(events) != 1 || events[0].Failed || events[0].Status != http.StatusNoContent {
			t.Fatal("AbortUpload did not produce one explicit HTTP 204; retain fixture")
		}
		// HEAD alone cannot prove abort: an incomplete upload is never an
		// ordinary visible object. Reuse only this run's known upload ID in
		// a correctly authenticated ordinary call and require NoSuchUpload.
		_, err = gc.UploadPart(key, upload.UploadID, 2, []byte("post-abort owned probe"))
		var api smithy.APIError
		var status interface{ HTTPStatusCode() int }
		if !errors.As(err, &api) || api.ErrorCode() != "NoSuchUpload" ||
			!errors.As(err, &status) || status.HTTPStatusCode() != http.StatusNotFound {
			t.Fatal("aborted upload removal unproven; retain fixture")
		}
		if _, err := gc.Head(key); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("aborted upload exposed an object or absence is unknown; retain fixture")
		}
	}) {
		return
	}
	if !run("client_delete_denied_and_bytes_preserved", func(t *testing.T) {
		key := prefix + fixtures[2].leaf
		err := client.Delete(key)
		var api smithy.APIError
		var status interface{ HTTPStatusCode() int }
		if !errors.As(err, &api) || api.ErrorCode() != "AccessDenied" || !errors.As(err, &status) || status.HTTPStatusCode() != http.StatusForbidden {
			t.Fatal("client DELETE lacked explicit AccessDenied/403; retain fixture")
		}
		storageReadExact(t, client, key, fixtures[2].body, transport)
	}) {
		return
	}
	if !run("client_exact_paginated_inventory", func(t *testing.T) { storageListExact(t, client, prefix, want) }) {
		return
	}
	if !run("gc_delete_and_absence", func(t *testing.T) {
		receipt.CleanupState = "IN_PROGRESS"
		for _, leaf := range []string{"zero.bin", "素材/café 01.bin", "4MiB.bin", "create-only.bin", "multipart.bin"} {
			key := prefix + leaf
			if gc.Delete(key) != nil {
				t.Fatal("GC DELETE failed or uncertain; stop cleanup and retain fixture")
			}
			if _, err := gc.Head(key); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("deleted object absence unproven; stop cleanup and retain fixture")
			}
		}
		storageListExact(t, client, prefix, map[string]int64{})
		receipt.CleanupState = "COMPLETE"
	}) {
		return
	}
	if len(receipt.Checks) != receipt.DeclaredChecks || receipt.CleanupState != "COMPLETE" {
		t.Fatal("declared compatibility checks or cleanup incomplete; retain fixture")
	}
	receipt.State = "PASS"
	t.Logf("normal object compatibility passed: %d declared checks; whole migration and security acceptance remain open", len(receipt.Checks))
}
