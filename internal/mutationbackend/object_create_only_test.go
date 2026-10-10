package mutationbackend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

// Separate admission probes from the payload fault under test. Conditional
// decisions are atomic at storage time, not just when a request arrives.
func newCreateOnlyProbeFixture(t *testing.T) func(http.ResponseWriter, *http.Request) bool {
	t.Helper()
	var mu sync.Mutex
	objects := make(map[string][]byte)
	return func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.Contains(r.URL.Path, "/"+createOnlyProbePrefix) {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		body, exists := objects[r.URL.Path]
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("probe omitted create-only precondition")
				w.WriteHeader(400)
			} else if exists {
				w.WriteHeader(412)
			} else {
				var err error
				objects[r.URL.Path], err = io.ReadAll(io.LimitReader(r.Body, 256))
				if err != nil {
					t.Error(err)
				}
				w.WriteHeader(200)
			}
		case http.MethodGet:
			if !exists {
				w.WriteHeader(404)
			} else {
				sum := sha256.Sum256(body)
				w.Header().Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sum[:]))
				_, _ = w.Write(body)
			}
		default:
			t.Error("unexpected probe operation", r.Method)
			w.WriteHeader(405)
		}
		return true
	}
}

func TestSDKCreateOnlyAdmissionCoversEveryShard(t *testing.T) {
	for _, mode := range []string{"honor", "ignore-last-shard", "409", "lying-412"} {
		t.Run(mode, func(t *testing.T) {
			isolatedProviderEnvironment(t)
			var mu sync.Mutex
			stored := make(map[string][]byte)
			puts := make(map[string]int)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(io.LimitReader(r.Body, 256))
				mu.Lock()
				defer mu.Unlock()
				old, exists := stored[r.URL.Path]
				if r.Method == http.MethodGet {
					_, _ = w.Write(old)
					return
				}
				if r.Method != http.MethodPut || r.Header.Get("If-None-Match") != "*" {
					t.Error("admission used a nonconditional operation")
					w.WriteHeader(400)
					return
				}
				puts[strings.Split(r.URL.Path, "/")[1]]++
				if exists {
					switch mode {
					case "ignore-last-shard":
						if strings.HasPrefix(r.URL.Path, "/bucket-2/") {
							stored[r.URL.Path] = body
							w.WriteHeader(200)
							return
						}
					case "409":
						w.WriteHeader(409)
						return
					case "lying-412":
						stored[r.URL.Path] = body
					}
					w.WriteHeader(412)
					return
				}
				stored[r.URL.Path] = body
				w.WriteHeader(200)
			}))
			defer server.Close()
			style := true
			f := meta.Format{Name: "fixture", Storage: "minio", Bucket: server.URL + "/bucket-%d", Shards: 3, AccessKey: "fixture-key", SecretKey: "fixture-secret"}
			_, err := sdkStorage(f, SDKConfig{Region: "us-east-1", PathStyle: &style})
			if (err == nil) != (mode == "honor") {
				t.Fatal("incorrect provider admission", mode, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if mode == "honor" || mode == "ignore-last-shard" {
				for i := 0; i < 3; i++ {
					if puts[fmt.Sprintf("bucket-%d", i)] != 2 {
						t.Fatal("shard not probed twice", i, puts)
					}
				}
			}
		})
	}
}

func TestCreateOnlyProviderConflictsAndAttributes(t *testing.T) {
	for _, provider := range []string{"s3", "minio"} {
		t.Run(provider, func(t *testing.T) {
			isolatedProviderEnvironment(t)
			var mu sync.Mutex
			status, expectedCondition := 200, "*"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				_, _ = io.Copy(io.Discard, r.Body)
				if r.Header.Get("If-None-Match") != expectedCondition {
					t.Error("conditional header mismatch")
				}
				w.Header().Set("X-Amz-Request-Id", "fixture-request")
				w.WriteHeader(status)
			}))
			defer server.Close()
			style := true
			if err := providerEnvironment(SDKConfig{Region: "us-east-1", PathStyle: &style}); err != nil {
				t.Fatal(err)
			}
			store, err := object.CreateStorage(provider, server.URL+"/bucket", "fixture-key", "fixture-secret", "")
			if err != nil {
				t.Fatal(err)
			}
			creator := store.(object.SupportCreateOnly)
			for _, code := range []int{200, 412, 409} {
				mu.Lock()
				status = code
				mu.Unlock()
				var requestID string
				err := creator.PutIfAbsent("key", strings.NewReader("body"), object.WithRequestID(&requestID))
				if (err == nil) != (code == 200) || errors.Is(err, os.ErrExist) != (code == 412) || requestID != "fixture-request" {
					t.Fatal("create-only status or request attributes lost", code, err, requestID)
				}
			}
			mu.Lock()
			status, expectedCondition = 200, ""
			mu.Unlock()
			if err := store.Put("ordinary", strings.NewReader("body")); err != nil {
				t.Fatal("ordinary upstream PUT changed", err)
			}
		})
	}
}

type unsupportedCreateOnly struct {
	object.ObjectStorage
	puts int
}

func (s *unsupportedCreateOnly) Put(string, io.Reader, ...object.AttrGetter) error {
	s.puts++
	return nil
}

func TestCreateOnlyNeverFallsBackToOrdinaryPut(t *testing.T) {
	s := &unsupportedCreateOnly{}
	v := &verifiedObjects{ObjectStorage: s}
	if v.Put("key", bytes.NewReader([]byte("body"))) == nil || s.puts != 0 {
		t.Fatal("ordinary PUT fallback")
	}
	if qualifyCreateOnly(context.Background(), s, "fixture", 1) == nil || s.puts != 0 {
		t.Fatal("unsupported provider admitted")
	}
}

type heldCreateOnly struct {
	object.ObjectStorage
	entered, release, returned chan struct{}
}

type delayedObjectInput struct {
	entered, release chan struct{}
	body             io.Reader
	once             sync.Once
}

func (r *delayedObjectInput) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered); <-r.release })
	return r.body.Read(p)
}

func TestFileCreateOnlyLateWriterRetainsWinner(t *testing.T) {
	root := t.TempDir()
	store, err := object.CreateStorage("file", root+"/", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	creator := store.(object.SupportCreateOnly)
	input := &delayedObjectInput{entered: make(chan struct{}), release: make(chan struct{}), body: strings.NewReader("old")}
	var once sync.Once
	release := func() { once.Do(func() { close(input.release) }) }
	defer release()
	done := make(chan error, 1)
	go func() { done <- creator.PutIfAbsent("nested/parent/object", input) }()
	select {
	case <-input.entered:
	case err := <-done:
		t.Fatal("first write did not reach private inode", err)
	case <-time.After(5 * time.Second):
		t.Fatal("first write stalled")
	}
	if err := creator.PutIfAbsent("nested/parent/object", strings.NewReader("winner")); err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-done; !errors.Is(err, os.ErrExist) {
		t.Fatal("late file writer was not refused", err)
	}
	body, err := os.ReadFile(filepath.Join(root, "nested/parent/object"))
	if err != nil || string(body) != "winner" {
		t.Fatal("winner changed", err)
	}
	if err := creator.PutIfAbsent("nested/parent/failed", &countedInput{failure: true}); err == nil {
		t.Fatal("failed reader published")
	}
	entries, err := os.ReadDir(filepath.Join(root, "nested/parent"))
	if err != nil || len(entries) != 1 || entries[0].Name() != "object" {
		t.Fatal("failed write left a public object or private name", err)
	}
}

func TestFileCreateOnlyRejectsEscapingKeysBeforeReadingInput(t *testing.T) {
	root := t.TempDir()
	bucket := filepath.Join(root, "bucket")
	if err := os.Mkdir(bucket, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := object.CreateStorage("file", bucket+"/", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	creator := store.(object.SupportCreateOnly)
	for _, key := range []string{"", ".", "..", "../escape", "a/../../escape", "/absolute", "nested/"} {
		input := strings.NewReader("must remain unread")
		size := input.Len()
		if err := creator.PutIfAbsent(key, input); err == nil || errors.Is(err, os.ErrExist) || input.Len() != size {
			t.Fatal("invalid key reached private file creation", key, err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "bucket" {
		t.Fatal("invalid key escaped configured root", err)
	}
	entries, err = os.ReadDir(bucket)
	if err != nil || len(entries) != 0 {
		t.Fatal("invalid key created storage entries", err)
	}
}

func (s *heldCreateOnly) PutIfAbsent(string, io.Reader, ...object.AttrGetter) error {
	close(s.entered)
	<-s.release
	close(s.returned)
	return errors.New("held request ended")
}

func TestCreateOnlyProbeCancellationDoesNotClaimJoin(t *testing.T) {
	s := &heldCreateOnly{entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- qualifyCreateOnly(ctx, s, "fixture", 1) }()
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("probe never started")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("canceled probe admitted")
		}
	case <-time.After(time.Second):
		t.Error("probe cancellation unbounded")
	}
	select {
	case <-s.returned:
		t.Error("test did not retain a pending provider call")
	default:
	}
	close(s.release)
	<-s.returned
}
