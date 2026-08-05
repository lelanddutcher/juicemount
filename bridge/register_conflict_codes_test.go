package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
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
			want: want{"blob_empty", true}, // the drainer creates at the final name then
			// copies, so 0 bytes is a real part of the drain window (nfs/drainer.go:729/759)
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
			want: want{"synthetic_inode", false}, // the branch is a pure function of
			// req.Inode, so an UNCHANGED retry always lands here; both remedies
			// change the request. Offline files hold a synthetic inode forever.
		},
		{
			name: "blob_not_regular",
			setup: func(t *testing.T, tmp string, ms *metadata.Store) map[string]any {
				ino, mt := seedSource(t, tmp, ms, "A/five.mov", 1_000_005, 2048)
				d := filepath.Join(tmp, ".juicemount", "derivatives", itoa(ino))
				if err := os.MkdirAll(filepath.Join(d, "poster.jpg"), 0o755); err != nil {
					t.Fatal(err) // a DIRECTORY at the reserved blob name
				}
				return regBody(ino, "thumbnail", 2048, mt)
			},
			want: want{"blob_not_regular", false}, // retrying cannot un-directory it
		},
		{
			name: "source_unreadable",
			// Mirror row points at a path that is not there — the stale-mirror
			// shape an out-of-band rename produces. Transient: reconcile fixes it.
			setup: func(t *testing.T, tmp string, ms *metadata.Store) map[string]any {
				ino, mt := seedSource(t, tmp, ms, "A/six.mov", 1_000_006, 2048)
				writeBlob(t, tmp, ino, "poster.jpg", []byte("JPEG"))
				if err := os.Remove(filepath.Join(tmp, "A/six.mov")); err != nil {
					t.Fatal(err)
				}
				return regBody(ino, "thumbnail", 2048, mt)
			},
			want: want{"source_unreadable", true},
		},
		{
			name: "producer_conflict",
			setup: func(t *testing.T, tmp string, ms *metadata.Store) map[string]any {
				ino, mt := seedSource(t, tmp, ms, "A/seven.mov", 1_000_007, 2048)
				writeBlob(t, tmp, ino, "poster.jpg", []byte("JPEG"))
				b := regBody(ino, "thumbnail", 2048, mt)
				b["__seedFarmRow"] = true // handled by the runner below
				return b
			},
			want: want{"producer_conflict", false}, // farm wins by precedence, not timing
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
			if _, ok := body["__seedFarmRow"]; ok {
				delete(body, "__seedFarmRow")
				ino := body["inode"].(uint64)
				blobRel, media := "poster.jpg", "image/jpeg"
				if err := ds.PutDeriv(ino, derivatives.DerivRow{
					Kind: "thumbnail", Status: "ready", Producer: "linux-farm",
					Version: 1, BlobRelPath: &blobRel, MediaType: &media,
				}); err != nil {
					t.Fatal(err)
				}
			}

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
	// PARSE, don't grep. Adversarial review demonstrated with a compiled example
	// that a substring check for ", http.StatusConflict)" scores ZERO on both of
	// these, and gofmt keeps them that way:
	//
	//	http.Error(w, "long prose that pushes the status onto its own line",
	//		http.StatusConflict)          // comma and const on different lines
	//	http.Error(w, "prose", 409)       // numeric literal
	//
	// The numeric form is the dominant style in this very handler (400/404/500
	// are all written as literals), so a maintainer adding a 409 in the local
	// idiom would slip past a string scan and leave a consumer's switch falling
	// through to default.
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "cbridge.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	is409 := func(e ast.Expr) bool {
		switch v := e.(type) {
		case *ast.BasicLit:
			return v.Kind == token.INT && v.Value == "409"
		case *ast.SelectorExpr:
			pkg, ok := v.X.(*ast.Ident)
			return ok && pkg.Name == "http" && v.Sel.Name == "StatusConflict"
		}
		return false
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 3 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Error" {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "http" {
			return true
		}
		if is409(call.Args[2]) {
			t.Errorf("%s: bare-prose 409 via http.Error — use writeRegisterConflict so the "+
				"condition carries a code and a retryable flag",
				fset.Position(call.Pos()))
		}
		return true
	})
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
