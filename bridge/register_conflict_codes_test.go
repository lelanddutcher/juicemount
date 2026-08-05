package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Every 409 from /derivatives/register must carry a discriminating code, and
// the `retryable` flag must match what retrying would actually do.
//
// WHY: this route returns 409 for six conditions whose remedies CONTRADICT each
// other — blob_not_visible means "keep retrying with this inode", synthetic_inode
// means "stop using this inode", source_stale means "recompute the derivative",
// producer_conflict is permanent. They were all bare prose, so the consumer
// could only apply one uniform bounded retry: that masks source_stale (the
// safety-relevant one — retrying unchanged would publish a derivative that does
// not describe the file) and burns the whole budget on the permanent ones.
// Requested by the consumer in the 409-DISCRIMINATOR row and answered yes.
//
// The retryable flag means strictly: "retrying THIS SAME request unchanged may
// succeed". Anything requiring the consumer to alter the request or the bytes is
// false, even though the consumer will act on it.
func TestRegisterConflictsCarryCodes(t *testing.T) {
	type want struct {
		code      string
		retryable bool
	}

	cases := []struct {
		name  string
		setup func(t *testing.T, tmp string, ms *metadata.Store) map[string]any
		want  want
	}{
		{
			name: "blob_not_visible",
			// Source exists, blob was never written.
			setup: func(t *testing.T, tmp string, ms *metadata.Store) map[string]any {
				ino, mt := seedSource(t, tmp, ms, "A/one.mov", 1_000_001, 2048)
				return regBody(ino, "thumbnail", 2048, mt)
			},
			want: want{"blob_not_visible", true}, // the write may still be draining
		},
		{
			name: "blob_empty",
			setup: func(t *testing.T, tmp string, ms *metadata.Store) map[string]any {
				ino, mt := seedSource(t, tmp, ms, "A/two.mov", 1_000_002, 2048)
				writeBlob(t, tmp, ino, "poster.jpg", nil) // 0 bytes
				return regBody(ino, "thumbnail", 2048, mt)
			},
			want: want{"blob_empty", false}, // retrying unchanged republishes 0 bytes
		},
		{
			name: "source_stale",
			setup: func(t *testing.T, tmp string, ms *metadata.Store) map[string]any {
				ino, mt := seedSource(t, tmp, ms, "A/three.mov", 1_000_003, 2048)
				writeBlob(t, tmp, ino, "poster.jpg", []byte("JPEG"))
				b := regBody(ino, "thumbnail", 2048, mt)
				b["source_size"] = 999 // vouch for bytes that are not there
				return b
			},
			want: want{"source_stale", false}, // must RECOMPUTE, not retry
		},
		{
			name: "synthetic_inode",
			setup: func(t *testing.T, tmp string, ms *metadata.Store) map[string]any {
				ino, mt := seedSource(t, tmp, ms, "A/four.mov", 1<<63+9, 2048)
				writeBlob(t, tmp, ino, "poster.jpg", []byte("JPEG"))
				return regBody(ino, "thumbnail", 2048, mt)
			},
			want: want{"synthetic_inode", true}, // resolves once reconciled
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			ms, err := metadata.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer ms.Close()
			ds, err := derivatives.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer ds.Close()

			body := tc.setup(t, tmp, ms)

			globalMu.Lock()
			oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
			globalStore, globalDerivStore, globalFUSEPath = ms, ds, tmp
			globalMu.Unlock()
			defer func() {
				globalMu.Lock()
				globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
				globalMu.Unlock()
			}()

			b, _ := json.Marshal(body)
			rr := httptest.NewRecorder()
			handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))

			if rr.Code != 409 {
				t.Fatalf("status %d, want 409; body %s", rr.Code, rr.Body.String())
			}
			var got registerConflict
			if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
				t.Fatalf("409 body is not JSON (%v) — the consumer cannot pick a remedy "+
					"without a discriminator; body=%s", err, rr.Body.String())
			}
			if got.Code != tc.want.code {
				t.Errorf("code = %q, want %q (body %s)", got.Code, tc.want.code, rr.Body.String())
			}
			if got.Retryable != tc.want.retryable {
				t.Errorf("retryable = %v, want %v for %q — a PERMANENT condition marked "+
					"retryable makes the consumer burn its whole budget and then report "+
					"flakiness; a RETRYABLE one marked permanent loses a contribution that "+
					"would have succeeded", got.Retryable, tc.want.retryable, tc.want.code)
			}
			if got.Message == "" {
				t.Error("message is empty — the code is for branching, the message is for humans")
			}
		})
	}
}

// No 409 on this route may be left as bare prose: a consumer that switches on
// `code` would silently fall through to its default branch. Guards against a
// later conflict being added in the old shape.
func TestNoBareProse409sRemainInRegister(t *testing.T) {
	src, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatal(err)
	}
	// Discriminate the two shapes precisely:
	//   http.Error(w, "prose", http.StatusConflict)  -> ", http.StatusConflict)"  BARE
	//   w.WriteHeader(http.StatusConflict)           -> "(http.StatusConflict)"   fine
	// The WriteHeader form is what writeRegisterConflict itself uses, and what a
	// couple of unrelated routes use; only the http.Error tail is the shape that
	// leaves a consumer switching on `code` with nothing to switch on.
	if n := bytes.Count(src, []byte(", http.StatusConflict)")); n != 0 {
		t.Errorf("%d bare-prose 409 site(s) remain (http.Error(..., http.StatusConflict)) — "+
			"use writeRegisterConflict so the condition carries a code and a retryable flag", n)
	}
}

func seedSource(t *testing.T, tmp string, ms *metadata.Store, rel string, ino uint64, size int) (uint64, int64) {
	t.Helper()
	p := filepath.Join(tmp, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, bytes.Repeat([]byte("z"), size), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000500, 0)
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}
	ms.InsertToCache(&metadata.Entry{
		Path: rel, Name: filepath.Base(rel), ParentPath: filepath.Dir(rel),
		Inode: ino, Size: int64(size), Mtime: mt,
	})
	return ino, mt.Unix()
}

func writeBlob(t *testing.T, tmp string, ino uint64, name string, data []byte) {
	t.Helper()
	d := filepath.Join(tmp, ".juicemount", "derivatives", itoa(ino))
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func regBody(ino uint64, kind string, size int, mtime int64) map[string]any {
	return map[string]any{
		"inode": ino, "kind": kind, "producer": "on-device",
		"source_size": size, "source_mtime": mtime, "blob_rel_path": "poster.jpg",
	}
}
