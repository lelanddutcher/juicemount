package mutationbackend

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

func isolatedProviderEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{"AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_EC2_METADATA_DISABLED", "AWS_REGION", "AWS_DEFAULT_REGION", "MINIO_REGION", "JFS_S3_VHOST_STYLE"} {
		t.Setenv(key, os.Getenv(key))
	}
}

func TestSDKStorageRequiresExactReadback(t *testing.T) {
	for _, mode := range []string{"exact", "corrupt", "short", "long", "missing"} {
		t.Run(mode, func(t *testing.T) {
			isolatedProviderEnvironment(t)
			var mu sync.Mutex
			var body []byte
			puts, gets := 0, 0
			probe := newCreateOnlyProbeFixture(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if probe(w, r) {
					return
				}
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodPut:
					puts++
					var err error
					body, err = io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					w.WriteHeader(200)
				case http.MethodGet:
					gets++
					got := append([]byte(nil), body...)
					switch mode {
					case "corrupt":
						got[0] ^= 0xff
					case "short":
						got = got[:len(got)-1]
					case "long":
						got = append(got, 1)
					case "missing":
						w.WriteHeader(404)
						return
					}
					_, _ = w.Write(got)
				default:
					w.WriteHeader(405)
				}
			}))
			defer server.Close()
			pathStyle := true
			format := meta.Format{Name: "fixture", Storage: "minio", Bucket: server.URL + "/bucket", AccessKey: "fixture-key", SecretKey: "fixture-secret"}
			blob, err := sdkStorage(format, SDKConfig{Region: "us-east-1", PathStyle: &pathStyle})
			if err != nil {
				t.Fatal(err)
			}
			err = blob.Put("chunks/object", bytes.NewReader([]byte("exact uploaded object")))
			if (err == nil) != (mode == "exact") {
				t.Fatalf("provider acknowledged %s object: Put error=%v", mode, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if puts != 1 || gets != 1 {
				t.Fatalf("must verify one actual upload via direct GET: puts=%d gets=%d", puts, gets)
			}
		})
	}
}

type integrityFixture struct {
	object.ObjectStorage
	body                              []byte
	puts, gets, closes, deletes       int
	putErr, getErr, readErr, closeErr error
	endless                           bool
	received                          int64
	class                             string
	stopped                           bool
}

func (f *integrityFixture) PutIfAbsent(_ string, in io.Reader, getters ...object.AttrGetter) error {
	f.puts++
	if seeker, ok := in.(io.Seeker); ok {
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	var err error
	f.body, err = io.ReadAll(in)
	if err != nil {
		return err
	}
	attrs := new(object.ResponseAttrs)
	for _, get := range getters {
		get(attrs)
	}
	attrs.SetRequestID("put-response").SetStorageClass("fixture-class")
	return f.putErr
}
func (f *integrityFixture) Get(_ string, off, limit int64, _ ...object.AttrGetter) (io.ReadCloser, error) {
	f.gets++
	if off != 0 || limit != -1 {
		return nil, errors.New("verification was a range")
	}
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &integrityReader{fixture: f, body: bytes.NewReader(f.body)}, nil
}
func (f *integrityFixture) Delete(string, ...object.AttrGetter) error { f.deletes++; return nil }
func (f *integrityFixture) SetStorageClass(class string) error        { f.class = class; return nil }
func (f *integrityFixture) Shutdown()                                 { f.stopped = true }

type integrityReader struct {
	fixture *integrityFixture
	body    *bytes.Reader
}

func (r *integrityReader) Read(p []byte) (int, error) {
	if r.fixture.endless {
		clear(p)
		r.fixture.received += int64(len(p))
		return len(p), nil
	}
	n, err := r.body.Read(p)
	r.fixture.received += int64(n)
	if n > 0 && r.fixture.readErr != nil {
		err = r.fixture.readErr
	}
	return n, err
}
func (r *integrityReader) Close() error { r.fixture.closes++; return r.fixture.closeErr }

type plainSeeker struct{ body *bytes.Reader }

func (r *plainSeeker) Read(p []byte) (int, error)         { return r.body.Read(p) }
func (r *plainSeeker) Seek(n int64, w int) (int64, error) { return r.body.Seek(n, w) }

type countedInput struct {
	count   int64
	failure bool
}

func (r *countedInput) Read(p []byte) (int, error) {
	if r.failure {
		return 0, errors.New("input failure")
	}
	clear(p)
	r.count += int64(len(p))
	return len(p), nil
}

func TestObjectIntegrityPreservesReaderOffsetsAndProviderAttributes(t *testing.T) {
	for _, kind := range []string{"reader-at", "seeker", "reader", "empty"} {
		t.Run(kind, func(t *testing.T) {
			fixture := &integrityFixture{}
			v := &verifiedObjects{ObjectStorage: fixture}
			var in io.Reader
			want := []byte("payload")
			switch kind {
			case "reader-at":
				r := bytes.NewReader([]byte("prefixpayload"))
				_, _ = r.Seek(6, io.SeekStart)
				in = r
			case "seeker":
				r := &plainSeeker{bytes.NewReader([]byte("prefixpayload"))}
				_, _ = r.Seek(6, io.SeekStart)
				in = r
			case "reader":
				in = bytes.NewBuffer(want)
			case "empty":
				in = bytes.NewReader(nil)
				want = nil
			}
			var id, class string
			if err := v.Put("key", in, object.WithRequestID(&id), object.WithStorageClass(&class)); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(fixture.body, want) || id != "put-response" || class != "fixture-class" || fixture.gets != 1 || fixture.closes != 1 {
				t.Fatal("bytes/attributes/readback lifecycle changed")
			}
			if err := v.Delete("key"); !errors.Is(err, errObjectReclamation) || fixture.deletes != 0 || !bytes.Equal(fixture.body, want) {
				t.Fatal("successful verification authorized object reclamation", err)
			}
			if err := v.SetStorageClass("desired"); err != nil || fixture.class != "desired" {
				t.Fatal(err)
			}
			v.Shutdown()
			if !fixture.stopped {
				t.Fatal("shutdown not delegated")
			}
		})
	}
}

func TestObjectIntegrityFailsClosedAndBoundsUntrustedStreams(t *testing.T) {
	for _, kind := range []string{"put-error", "get-error", "read-error", "close-error", "endless-get", "oversized-input", "input-error", "nil-input"} {
		t.Run(kind, func(t *testing.T) {
			fixture := &integrityFixture{}
			v := &verifiedObjects{ObjectStorage: fixture}
			var in io.Reader = bytes.NewReader([]byte("payload"))
			input := &countedInput{}
			bad := errors.New("fixture error")
			switch kind {
			case "put-error":
				fixture.putErr = bad
			case "get-error":
				fixture.getErr = bad
			case "read-error":
				fixture.readErr = bad
			case "close-error":
				fixture.closeErr = bad
			case "endless-get":
				fixture.endless = true
			case "oversized-input":
				in = input
			case "input-error":
				input.failure = true
				in = input
			case "nil-input":
				in = nil
			}
			if err := v.Put("key", in); !errors.Is(err, errObjectIntegrity) {
				t.Fatal(err)
			}
			if err := v.Delete("key"); !errors.Is(err, errObjectReclamation) {
				t.Fatal("failed upload authorized object reclamation", err)
			}
			if fixture.deletes != 0 {
				t.Fatal("uncertain object deleted")
			}
			if kind == "oversized-input" && (input.count != maxRawObjectBytes+1 || fixture.puts != 0) {
				t.Fatal("input bound or before-PUT refusal failed", input.count, fixture.puts)
			}
			if kind == "endless-get" && (fixture.received != 8 || fixture.closes != 1) {
				t.Fatal("GET body unbounded or unclosed", fixture.received, fixture.closes)
			}
			if (kind == "read-error" || kind == "close-error") && fixture.closes != 1 {
				t.Fatal("failed readback unclosed")
			}
			if kind == "put-error" && fixture.gets != 0 {
				t.Fatal("failed PUT continued")
			}
		})
	}
}
