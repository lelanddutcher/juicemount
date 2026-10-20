package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testFrame(kind byte, data []byte) []byte {
	header := frameHeader(kind, len(data))
	return append(header[:], data...)
}

func streamFrames(t *testing.T, data []byte) [][]byte {
	t.Helper()
	var frames [][]byte
	for offset := len(streamMagic); offset < len(data); {
		if len(data)-offset < 5 {
			t.Fatal("bad test fixture frame")
		}
		size := int(binary.BigEndian.Uint32(data[offset+1 : offset+5]))
		if size > len(data)-offset-5 {
			t.Fatal("bad test fixture frame length")
		}
		frames = append(frames, append([]byte(nil), data[offset:offset+5+size]...))
		offset += 5 + size
	}
	return frames
}

func joinFrames(frames [][]byte) []byte {
	data := []byte(streamMagic)
	for _, frame := range frames {
		data = append(data, frame...)
	}
	return data
}

func TestPortabilityMalformedFramesAndStrictJSON(t *testing.T) {
	_, valid := loadPortabilityVector(t, "legacy")
	mutations := map[string]func([][]byte) []byte{
		"magic":           func(f [][]byte) []byte { data := joinFrames(f); data[0] ^= 1; return data },
		"unknown kind":    func(f [][]byte) []byte { f[0][0] = 9; return joinFrames(f) },
		"oversized frame": func(f [][]byte) []byte { binary.BigEndian.PutUint32(f[0][1:5], ^uint32(0)); return joinFrames(f) },
		"zero frame":      func(f [][]byte) []byte { f[0] = []byte{1, 0, 0, 0, 0}; return joinFrames(f) },
		"header version": func(f [][]byte) []byte {
			f[0] = testFrame(1, bytes.Replace(f[0][5:], []byte(`"version":1`), []byte(`"version":2`), 1))
			return joinFrames(f)
		},
		"schema version": func(f [][]byte) []byte {
			f[0] = testFrame(1, bytes.Replace(f[0][5:], []byte(`"schema_version":2`), []byte(`"schema_version":3`), 1))
			return joinFrames(f)
		},
		"header library": func(f [][]byte) []byte {
			f[0] = testFrame(1, bytes.Replace(f[0][5:], []byte(testLibrary), []byte(otherLibrary), 1))
			return joinFrames(f)
		},
		"missing field": func(f [][]byte) []byte {
			f[0] = testFrame(1, bytes.Replace(f[0][5:], []byte(`,"event_count":2`), nil, 1))
			return joinFrames(f)
		},
		"duplicate field": func(f [][]byte) []byte {
			f[0] = testFrame(1, bytes.Replace(f[0][5:], []byte(`"version":1`), []byte(`"version":1,"version":1`), 1))
			return joinFrames(f)
		},
		"unknown field": func(f [][]byte) []byte {
			f[0] = testFrame(1, bytes.Replace(f[0][5:], []byte(`"version":1`), []byte(`"version":1,"extra":0`), 1))
			return joinFrames(f)
		},
		"whitespace JSON": func(f [][]byte) []byte { f[0] = testFrame(1, append([]byte(" "), f[0][5:]...)); return joinFrames(f) },
		"invalid UTF8": func(f [][]byte) []byte {
			f[0] = testFrame(1, bytes.Replace(f[0][5:], []byte("lib_"), []byte{0xff}, 1))
			return joinFrames(f)
		},
		"reordered events":     func(f [][]byte) []byte { f[1], f[2] = f[2], f[1]; return joinFrames(f) },
		"missing event":        func(f [][]byte) []byte { return joinFrames(append(f[:1], f[2:]...)) },
		"extra event":          func(f [][]byte) []byte { return joinFrames(append(f[:3], f[2], f[3])) },
		"inner request length": func(f [][]byte) []byte { binary.BigEndian.PutUint32(f[1][5:9], ^uint32(0)); return joinFrames(f) },
		"extra event byte":     func(f [][]byte) []byte { f[1] = testFrame(2, append(f[1][5:], 0)); return joinFrames(f) },
		"stream hash": func(f [][]byte) []byte {
			var footer streamFooter
			_ = json.Unmarshal(f[3][5:], &footer)
			footer.StreamSHA256 = strings.Repeat("0", 64)
			data, _ := json.Marshal(footer)
			f[3] = testFrame(3, data)
			return joinFrames(f)
		},
		"projection hash": func(f [][]byte) []byte {
			var footer streamFooter
			_ = json.Unmarshal(f[3][5:], &footer)
			footer.ProjectionSHA256 = strings.Repeat("A", 64)
			data, _ := json.Marshal(footer)
			f[3] = testFrame(3, data)
			return joinFrames(f)
		},
		"trailing byte":  func(f [][]byte) []byte { return append(joinFrames(f), '\n') },
		"missing footer": func(f [][]byte) []byte { return joinFrames(f[:3]) },
	}
	// These failures must be recognized while parsing the header, before any
	// destination is reserved. An unrelated later stream error is not evidence.
	earlyErrors := map[string]string{
		"magic":           "export magic",
		"unknown kind":    "unexpected stream frame kind",
		"oversized frame": "stream frame length",
		"zero frame":      "stream frame length",
		"header version":  "export header identity or version",
		"schema version":  "export header identity or version",
		"header library":  "export header identity or version",
		"missing field":   "noncanonical portability JSON",
		"duplicate field": "noncanonical portability JSON",
		"unknown field":   "unknown field",
		"whitespace JSON": "noncanonical portability JSON",
		"invalid UTF8":    "noncanonical portability JSON",
	}
	assertEarlyRefusal := func(t *testing.T, path string, err error, want string) {
		t.Helper()
		if !errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), want) {
			t.Errorf("header refusal = %v, want format error containing %q", err, want)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Errorf("malformed header reserved a destination: %v", err)
		}
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "restore.sqlite")
			err := Restore(context.Background(), path, testLibrary, bytes.NewReader(mutate(streamFrames(t, valid))), portabilityLimits())
			if want, early := earlyErrors[name]; early {
				assertEarlyRefusal(t, path, err, want)
				return
			}
			if err == nil {
				t.Fatal("malformed stream accepted")
			}
			assertIncomplete(t, path)
		})
	}
	t.Run("empty missing event_count", func(t *testing.T) {
		_, empty := loadPortabilityVector(t, "empty")
		frames := streamFrames(t, empty)
		header := bytes.Replace(frames[0][5:], []byte(`,"event_count":0`), nil, 1)
		if bytes.Equal(header, frames[0][5:]) {
			t.Fatal("fixture did not contain the required zero event_count field")
		}
		frames[0] = testFrame(1, header)
		path := filepath.Join(t.TempDir(), "restore.sqlite")
		err := Restore(context.Background(), path, testLibrary, bytes.NewReader(joinFrames(frames)), portabilityLimits())
		assertEarlyRefusal(t, path, err, "noncanonical portability JSON")
	})
	// Every framing boundary and the byte immediately before it is truncation.
	boundaries := []int{0, len(streamMagic) - 1, len(streamMagic)}
	offset := len(streamMagic)
	for _, frame := range streamFrames(t, valid) {
		boundaries = append(boundaries, offset+4, offset+5, offset+len(frame)-1)
		offset += len(frame)
		if offset < len(valid) {
			boundaries = append(boundaries, offset)
		}
	}
	for _, boundary := range boundaries {
		path := filepath.Join(t.TempDir(), "truncated.sqlite")
		if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(valid[:boundary]), portabilityLimits()); err == nil {
			t.Fatalf("truncation at %d accepted", boundary)
		}
		assertIncomplete(t, path)
	}
}

func TestPortabilityDuplicateOperationAndFailedBatchRemainIncomplete(t *testing.T) {
	_, stream := loadPortabilityVector(t, "legacy")
	frames := streamFrames(t, stream)
	event, _, err := decodeStreamEvent(frames[1][5:], testLibrary, 1)
	if err != nil {
		t.Fatal(err)
	}
	var receipt Receipt
	if err := json.Unmarshal(event.receipt, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Sequence = 2
	event.sequence = 2
	event.receipt, _ = json.Marshal(receipt)
	frames[2] = testFrame(2, encodeEvent(event))
	path := filepath.Join(t.TempDir(), "duplicate.sqlite")
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(joinFrames(frames)), portabilityLimits()); err == nil || !strings.Contains(err.Error(), "duplicate restore operation") {
		t.Fatalf("duplicate operation: %v", err)
	}
	assets, revisions, parents, events := queryIncompleteCounts(t, path)
	if assets != 1 || revisions != 1 || parents != 0 || events != 1 {
		t.Fatal("duplicate request altered prior replay")
	}
	// A validly encoded batch with a failing later change must not commit its
	// earlier creation. No footer is needed to observe this bounded failure.
	op := initialOperation()
	op.Changes = append(op.Changes, Change{RenameAsset: &RenameAsset{AssetID: testAsset, ExpectedLabel: "wrong", Label: "new"}})
	request, digest, err := CanonicalRequest(op)
	if err != nil {
		t.Fatal(err)
	}
	badReceipt, _ := json.Marshal(Receipt{1, testLibrary, op.ID, 1, digest, 1})
	header, _ := json.Marshal(streamHeader{1, 2, testLibrary, 1})
	data := joinFrames([][]byte{testFrame(1, header), testFrame(2, encodeEvent(eventRow{request: request, receipt: badReceipt}))})
	path = filepath.Join(t.TempDir(), "batch.sqlite")
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(data), portabilityLimits()); !errors.Is(err, ErrConflict) {
		t.Fatalf("failed batch: %v", err)
	}
	assets, revisions, parents, events = queryIncompleteCounts(t, path)
	if assets+revisions+parents+events != 0 {
		t.Fatal("failed batch left partial projections/history")
	}
}

type portReadFunc func([]byte) (int, error)

func (f portReadFunc) Read(p []byte) (int, error) { return f(p) }

type portWriteFunc func([]byte) (int, error)

func (f portWriteFunc) Write(p []byte) (int, error) { return f(p) }

func TestPortabilityReaderErrorsNoProgressAndTerminalEOF(t *testing.T) {
	_, stream := loadPortabilityVector(t, "empty")
	readErr := errors.New("source reader failed")
	for name, reader := range map[string]io.Reader{
		"late error after footer": io.MultiReader(bytes.NewReader(stream), portReadFunc(func([]byte) (int, error) { return 0, readErr })),
		"impossible negative":     portReadFunc(func([]byte) (int, error) { return -1, nil }),
		"impossible positive":     portReadFunc(func(p []byte) (int, error) { return len(p) + 1, nil }),
		"read bytes plus error":   portReadFunc(func(p []byte) (int, error) { return copy(p, stream), readErr }),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "restore.sqlite")
			err := Restore(context.Background(), path, testLibrary, reader, portabilityLimits())
			if err == nil {
				t.Fatal("reader failure accepted")
			}
			if strings.Contains(name, "error") && !errors.Is(err, readErr) {
				t.Fatalf("lost reader error: %v", err)
			}
			assertIncomplete(t, path)
		})
	}
	calls := 0
	reader := portReadFunc(func([]byte) (int, error) { calls++; return 0, nil })
	if err := Restore(context.Background(), filepath.Join(t.TempDir(), "restore.sqlite"), testLibrary, reader, portabilityLimits()); !errors.Is(err, io.ErrNoProgress) || calls != 100 {
		t.Fatalf("no progress: %v, calls %d", err, calls)
	}
	for _, chunk := range []int{1, len(stream)} {
		r := bytes.NewReader(stream)
		reader := portReadFunc(func(p []byte) (int, error) {
			if len(p) > chunk {
				p = p[:chunk]
			}
			n, err := r.Read(p)
			if r.Len() == 0 {
				return n, io.EOF
			}
			return n, err
		})
		if err := Restore(context.Background(), filepath.Join(t.TempDir(), "valid.sqlite"), testLibrary, reader, Limits{int64(len(stream)), 1, 1}); err != nil {
			t.Fatalf("terminal EOF/short reads: %v", err)
		}
	}
	// Even a complete final payload accompanied by an error must remain incomplete.
	r := bytes.NewReader(stream)
	reader = portReadFunc(func(p []byte) (int, error) {
		n, err := r.Read(p)
		if r.Len() == 0 {
			return n, readErr
		}
		return n, err
	})
	path := filepath.Join(t.TempDir(), "payload-error.sqlite")
	if err := Restore(context.Background(), path, testLibrary, reader, portabilityLimits()); !errors.Is(err, readErr) {
		t.Fatalf("complete payload error: %v", err)
	}
	assertIncomplete(t, path)
}

func TestPortabilityWriterFailuresReleaseSnapshot(t *testing.T) {
	s, _, _ := vectorSource(t, "legacy")
	writeErr := errors.New("destination write failed")
	for name, writer := range map[string]io.Writer{
		"short":                 portWriteFunc(func(p []byte) (int, error) { return len(p) - 1, nil }),
		"error with full write": portWriteFunc(func(p []byte) (int, error) { return len(p), writeErr }),
		"negative count":        portWriteFunc(func([]byte) (int, error) { return -1, nil }),
		"oversized count":       portWriteFunc(func(p []byte) (int, error) { return len(p) + 1, nil }),
	} {
		t.Run(name, func(t *testing.T) {
			err := s.Export(context.Background(), testLibrary, writer, portabilityLimits())
			if err == nil {
				t.Fatal("writer failure accepted")
			}
			if name == "short" && !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("short write: %v", err)
			}
			if name == "error with full write" && !errors.Is(err, writeErr) {
				t.Fatalf("lost write error: %v", err)
			}
		})
	}
	applyTest(t, s, testOperation("after-errors", Change{CreateAsset: &CreateAsset{AssetID: otherAsset}}))
	var busy, log, checkpointed int
	if err := s.db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &log, &checkpointed); err != nil || busy != 0 {
		t.Fatalf("export leaked a read snapshot: busy=%d, %v", busy, err)
	}
}

func TestPortabilityFooterWriteErrorCanDeliverCompleteArtifact(t *testing.T) {
	s, _, expected := vectorSource(t, "legacy")
	frames := streamFrames(t, expected)
	footerPayload := frames[len(frames)-1][5:]
	writeErr := errors.New("footer acknowledgment failed")
	var captured bytes.Buffer
	footerWrites := 0
	writer := portWriteFunc(func(p []byte) (int, error) {
		n, _ := captured.Write(p)
		if bytes.Equal(p, footerPayload) {
			footerWrites++
			return n, writeErr
		}
		return n, nil
	})
	if err := s.Export(context.Background(), testLibrary, writer, portabilityLimits()); !errors.Is(err, writeErr) || footerWrites != 1 {
		t.Fatalf("footer write outcome: %v, calls %d", err, footerWrites)
	}
	if !bytes.Equal(captured.Bytes(), expected) {
		t.Fatal("full footer write did not capture complete golden artifact")
	}
	if err := Restore(context.Background(), filepath.Join(t.TempDir(), "restore.sqlite"), testLibrary, bytes.NewReader(captured.Bytes()), portabilityLimits()); err != nil {
		t.Fatalf("independently complete artifact cannot restore: %v", err)
	}
}

func TestPortabilityRestoreRefusesExistingAndUnsafeTargets(t *testing.T) {
	_, stream := loadPortabilityVector(t, "empty")
	s, path := newTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream), portabilityLimits()); !errors.Is(err, ErrConflict) {
		t.Fatalf("existing target: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("existing target changed")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		t.Run(suffix, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "restore.sqlite")
			if err := os.WriteFile(path+suffix, []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream), portabilityLimits()); err == nil {
				t.Fatal("orphan sidecar accepted")
			}
			got, err := os.ReadFile(path + suffix)
			if err != nil || string(got) != "retained" {
				t.Fatal("orphan sidecar changed")
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("sidecar refusal reserved main")
			}
		})
	}
	for _, kind := range []string{"symlink", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "restore.sqlite")
			var err error
			if kind == "symlink" {
				err = os.Symlink(s.path, path)
			} else {
				err = os.Link(s.path, path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream), portabilityLimits()); err == nil {
				t.Fatal("linked target accepted")
			}
		})
	}
}
