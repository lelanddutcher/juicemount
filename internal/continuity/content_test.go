package continuity

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

type readFunc func([]byte) (int, error)

func (f readFunc) Read(p []byte) (int, error) { return f(p) }

type countingReader struct {
	r     io.Reader
	bytes int
	calls int
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.calls++
	n, err := r.r.Read(p)
	r.bytes += n
	return n, err
}

func TestHashContentSuccessfulEOF(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
		max  int64
		sha  string
	}{
		{"empty", "", 0, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"exact", "abc", 3, abcSHA256},
		{"below limit", "abc", 4, abcSHA256},
		{"max int64 limit", "abc", math.MaxInt64, abcSHA256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := HashContent(strings.NewReader(tc.data), tc.max)
			want := Content{SHA256: tc.sha, Size: int64(len(tc.data))}
			if err != nil || got != want {
				t.Fatalf("HashContent = %+v, %v; want %+v", got, err, want)
			}
		})
	}
	// A reader may return its last bytes together with EOF.
	got, err := HashContent(readFunc(func(p []byte) (int, error) {
		return copy(p, "abc"), io.EOF
	}), 3)
	if err != nil || got != (Content{SHA256: abcSHA256, Size: 3}) {
		t.Fatalf("bytes with EOF: %+v, %v", got, err)
	}
}

func TestHashContentRejectsFailedOrOversizedStreams(t *testing.T) {
	readErr := errors.New("source read failed")
	for _, tc := range []struct {
		name string
		r    io.Reader
		max  int64
		want error
	}{
		{"nil reader", nil, 0, nil},
		{"negative limit", strings.NewReader(""), -1, nil},
		{"over limit", strings.NewReader("abcd"), 3, ErrContentTooLarge},
		{"zero limit", strings.NewReader("a"), 0, ErrContentTooLarge},
		{"read error", readFunc(func([]byte) (int, error) { return 0, readErr }), 3, readErr},
		{"bytes with read error", readFunc(func(p []byte) (int, error) {
			return copy(p, "abc"), readErr
		}), 3, readErr},
		{"negative read count", readFunc(func([]byte) (int, error) { return -1, nil }), 3, nil},
		{"excess read count", readFunc(func(p []byte) (int, error) { return len(p) + 1, nil }), 3, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := HashContent(tc.r, tc.max)
			if err == nil || got != (Content{}) {
				t.Fatalf("HashContent = %+v, %v; want zero and error", got, err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
	// Reaching the declared size does not excuse a failed EOF probe.
	r := io.MultiReader(strings.NewReader("abc"), readFunc(func([]byte) (int, error) {
		return 0, readErr
	}))
	if got, err := HashContent(r, 3); got != (Content{}) || !errors.Is(err, readErr) {
		t.Fatalf("failed EOF probe: %+v, %v", got, err)
	}
}

func TestHashContentBoundsReadsAndNoProgress(t *testing.T) {
	r := &countingReader{r: strings.NewReader(strings.Repeat("a", 100))}
	if got, err := HashContent(r, 3); got != (Content{}) || !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("over limit: %+v, %v", got, err)
	}
	if r.bytes != 4 {
		t.Fatalf("read %d bytes, want limit+1", r.bytes)
	}
	empty := &countingReader{r: readFunc(func([]byte) (int, error) { return 0, nil })}
	if got, err := HashContent(empty, math.MaxInt64); got != (Content{}) || !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("no progress: %+v, %v", got, err)
	}
	if empty.calls != 100 {
		t.Fatalf("unproductive calls = %d, want 100", empty.calls)
	}
	calls := 0
	rProgress := readFunc(func(p []byte) (int, error) {
		calls++
		switch calls {
		case 100:
			return copy(p, "abc"), nil
		case 200:
			return 0, io.EOF
		default:
			return 0, nil
		}
	})
	if got, err := HashContent(rProgress, 3); err != nil || got != (Content{SHA256: abcSHA256, Size: 3}) {
		t.Fatalf("intermittent progress: %+v, %v", got, err)
	}
}

func TestHashContentIncludesMiddleBytes(t *testing.T) {
	data := bytes.Repeat([]byte{'a'}, 3*1024*1024)
	first, err := HashContent(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(data)
	if first.SHA256 != hex.EncodeToString(want[:]) || first.Size != int64(len(data)) {
		t.Fatalf("full digest/size mismatch: %+v", first)
	}
	data[len(data)/2] = 'b' // Outside both 1 MiB head/tail windows.
	second, err := HashContent(bytes.NewReader(data), int64(len(data)))
	if err != nil || first == second || first.Size != second.Size {
		t.Fatalf("same-size middle change was lost: first %+v, second %+v, %v", first, second, err)
	}
	if err := VerifyContent(bytes.NewReader(data), first); !errors.Is(err, ErrContentMismatch) {
		t.Fatalf("VerifyContent accepted changed middle: %v", err)
	}
}

func TestVerifyContentExactBytesAndLength(t *testing.T) {
	expected := Content{SHA256: abcSHA256, Size: 3}
	readErr := errors.New("read failed")
	for _, tc := range []struct {
		name string
		r    io.Reader
		want error
	}{
		{"exact", strings.NewReader("abc"), nil},
		{"short", strings.NewReader("ab"), ErrContentMismatch},
		{"long", strings.NewReader("abcdef"), ErrContentTooLarge},
		{"changed", strings.NewReader("abd"), ErrContentMismatch},
		{"error", readFunc(func([]byte) (int, error) { return 0, readErr }), readErr},
		{"no progress", readFunc(func([]byte) (int, error) { return 0, nil }), io.ErrNoProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &countingReader{r: tc.r}
			err := VerifyContent(r, expected)
			if !errors.Is(err, tc.want) {
				t.Fatalf("VerifyContent = %v, want %v", err, tc.want)
			}
			if r.bytes > 4 {
				t.Fatalf("read %d bytes, want at most expected+1", r.bytes)
			}
		})
	}
	for _, invalid := range []Content{
		{}, {SHA256: abcSHA256, Size: -1}, {SHA256: strings.ToUpper(abcSHA256), Size: 3},
		{SHA256: strings.Repeat("g", 64), Size: 3}, {SHA256: abcSHA256 + "0", Size: 3},
	} {
		r := &countingReader{r: strings.NewReader("abc")}
		if err := VerifyContent(r, invalid); err == nil || r.calls != 0 {
			t.Fatalf("invalid descriptor read source: %+v, %v, calls %d", invalid, err, r.calls)
		}
	}
	// A validly encoded false digest is not detectable by syntax.
	forged := Content{SHA256: strings.Repeat("0", 64), Size: 3}
	if err := forged.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyContent(strings.NewReader("abc"), forged); !errors.Is(err, ErrContentMismatch) {
		t.Fatalf("forged digest verification = %v", err)
	}
}

func TestVerifyContentSizeBoundaries(t *testing.T) {
	empty := Content{SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Size: 0}
	if err := VerifyContent(strings.NewReader(""), empty); err != nil {
		t.Fatal(err)
	}
	r := &countingReader{r: strings.NewReader("abc")}
	if err := VerifyContent(r, empty); !errors.Is(err, ErrContentTooLarge) || r.bytes != 1 {
		t.Fatalf("zero size boundary: %v, read %d", err, r.bytes)
	}
	maximal := Content{SHA256: abcSHA256, Size: math.MaxInt64}
	if err := VerifyContent(strings.NewReader("abc"), maximal); !errors.Is(err, ErrContentMismatch) {
		t.Fatalf("MaxInt64 length claim on short source: %v", err)
	}
}
