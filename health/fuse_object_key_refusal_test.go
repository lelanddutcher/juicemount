package health

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// H-8 per-Mac object keys (design §5 "Mac side", test C2): the juicefs log
// hook notices that the object store refused this Mac's key, and the mount
// records which key its child runs with (the poll's object_key_in_use).

// jfsLine is a juicefs log record in the shape the bundled juicefs writes
// (utils logger: "<date> <time> juicefs[pid] <LEVEL>: msg [file:line]").
func jfsLine(level, msg string) string {
	return "2026/10/07 10:00:00.123456 juicefs[4242] <" + level + ">: " + msg + " [cached_store.go:423]"
}

// s3Err is an aws-sdk-go-v2 error as juicefs's s3 backend returns it.
func s3Err(op, code string) string {
	return "operation error S3: " + op + ", https response error StatusCode: 403, RequestID: 17F0A1B2C3D4E5F6, HostID: dd9025bab4ad464b, api error " + code + ": refused."
}

func TestObjectKeyRefusalCodeClassifiesJuiceFSLines(t *testing.T) {
	refused := map[string]string{
		// Upload retries after a revoke (svcacct rm): the key ID is unknown.
		jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: "+s3Err("PutObject", "InvalidAccessKeyId")+" (try 1)"): "InvalidAccessKeyId",
		// A wrong secret.
		jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: "+s3Err("PutObject", "SignatureDoesNotMatch")+" (try 2)"): "SignatureDoesNotMatch",
		// The policy refuses a write or a read.
		jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: "+s3Err("PutObject", "AccessDenied")+" (try 1)"):                        "AccessDenied",
		jfsLine("WARNING", "fail to read sliceId 7 (off:0, size:4096, clen: 4096, inode: 9): "+s3Err("GetObject", "AccessDenied")): "AccessDenied",
		jfsLine("ERROR", "read file 9: "+s3Err("GetObject", "InvalidAccessKeyId")):                                                 "InvalidAccessKeyId",
	}
	for line, want := range refused {
		if got := ObjectKeyRefusalCode(line); got != want {
			t.Errorf("ObjectKeyRefusalCode(%q) = %q, want %q", line, got, want)
		}
	}
	notRefused := []string{
		// Not a WARNING or ERROR record.
		strings.Replace(jfsLine("WARNING", "Upload x: "+s3Err("PutObject", "InvalidAccessKeyId")), "<WARNING>", "<DEBUG>", 1),
		// AccessDenied for an operation the Mac's policy never allowed says
		// nothing about the key.
		jfsLine("WARNING", "delete chunks/0/1/1_0_4194304: "+s3Err("DeleteObject", "AccessDenied")),
		jfsLine("WARNING", "abort upload: "+s3Err("AbortMultipartUpload", "AccessDenied")),
		jfsLine("WARNING", "something AccessDenied without an operation"),
		// Other failures.
		jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: "+s3Err("PutObject", "RequestTimeTooSkewed")+" (try 1)"),
		jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: dial tcp 192.0.2.1:30151: connect: connection refused (try 1)"),
		jfsLine("ERROR", "NOPERM this user has no permissions to run the 'flushall' command"),
		jfsLine("WARNING", "space not enough on device, upload it directly"),
		// A word that merely contains a code.
		jfsLine("WARNING", "Upload x: XInvalidAccessKeyIdX PutObject"),
	}
	for _, line := range notRefused {
		if got := ObjectKeyRefusalCode(line); got != "" {
			t.Errorf("ObjectKeyRefusalCode(%q) = %q, want none", line, got)
		}
	}
}

// The real retry form: when the SDK retried first, its text sits between
// the operation and the response error (seen in a real juicefs log with
// RequestTimeTooSkewed). A refusal in that form still counts.
func TestObjectKeyRefusalCodeAcceptsRetryForm(t *testing.T) {
	line := "2026/10/07 10:00:00.123456 juicefs[4242] <WARNING>: slow request: PUT chunks/7/7962/7962641_3_4194304 " +
		`(req_id: "18CF90138FA89017", err: operation error S3: PutObject, exceeded maximum number of attempts, 1, ` +
		"https response error StatusCode: 403, RequestID: 18CF90138FA89017, HostID: dd9025bab4ad464b, " +
		"api error InvalidAccessKeyId: The Access Key Id you provided does not exist in our records., cost: 12ms) [logRequest@cached_store.go:123]"
	if got := ObjectKeyRefusalCode(line); got != "InvalidAccessKeyId" {
		t.Fatalf("ObjectKeyRefusalCode(retry form) = %q, want InvalidAccessKeyId", got)
	}
}

// Review keys-l3 F4: juicefs logs file names that any user of the volume
// chose (pkg/meta/redis.go "no attribute for inode %d (%d, %s)", "ignore
// broken entry %s", ...). A name that contains a code, or even the "api
// error <Code>:" text, is not a refusal unless the whole SDK error form is
// there.
func TestObjectKeyRefusalCodeIgnoresFileNames(t *testing.T) {
	lines := []string{
		jfsLine("WARNING", "no attribute for inode 77 (1, InvalidAccessKeyId.mov)"),
		jfsLine("WARNING", "no attribute for inode 77 (1, PutObject AccessDenied.mov)"),
		jfsLine("WARNING", "ignore broken entry SignatureDoesNotMatch (inode: 5) in 1"),
		jfsLine("ERROR", "remove 9/InvalidAccessKeyId: no such file or directory"),
		jfsLine("WARNING", "no attribute for inode 77 (1, api error InvalidAccessKeyId: x.mov)"),
		jfsLine("WARNING", "no attribute for inode 77 (1, PutObject StatusCode: 403 api error AccessDenied: x.mov)"),
		jfsLine("WARNING", "Upload x: api error SignatureDoesNotMatch: PutObject"),
		// The full form with another status is not a refusal of the key.
		jfsLine("WARNING", "Upload x: "+strings.Replace(s3Err("PutObject", "AccessDenied"), "StatusCode: 403", "StatusCode: 503", 1)),
	}
	for _, l := range lines {
		if code := ObjectKeyRefusalCode(l); code != "" {
			t.Errorf("a file name counted as a key refusal (%s): %q", code, l)
		}
	}
}

// Zero refusals in real juicefs output. testdata/juicefs_log_scrubbed.txt
// holds 637 lines from a real ~/.juicefs/juicefs.log, picked by message
// template (no line that carries a file name or a local path) and scrubbed
// (addresses -> 192.0.2.10, volume name -> vol, HostID fixed). It includes
// 40 real HTTP 403 lines (RequestTimeTooSkewed), the closest real shape to a
// refusal.
func TestObjectKeyRefusalCodeNoHitsInRealJuiceFSLog(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "juicefs_log_scrubbed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 300 {
		t.Fatalf("fixture has %d lines, want >= 300", len(lines))
	}
	forbidden := 0
	for _, l := range lines {
		if strings.Contains(l, "StatusCode: 403") {
			forbidden++
		}
		if code := ObjectKeyRefusalCode(l); code != "" {
			t.Errorf("real juicefs line counted as a refusal (%s): %q", code, l)
		}
	}
	if forbidden < 20 {
		t.Fatalf("fixture has %d HTTP 403 lines, want >= 20", forbidden)
	}
}

// The mount records the access-key ID its child received (never the secret),
// and a new child clears the previous child's refusals. The secret is a
// MinIO-generated one with '+' and '/', which must reach juicefs unchanged.
func TestMountRecordsObjectKeyInUseAndResetsRefusals(t *testing.T) {
	const (
		accessKey = "mac-4f1c2a9e7b3d-2"
		plusSlash = "fixture+not/a+key/fixture+not/a+key/fixture"
	)
	home := t.TempDir()
	t.Setenv("HOME", home)
	// An earlier child's log: the new child's tail starts after it.
	earlier := []byte(jfsLine("WARNING", "earlier child") + "\n")
	if err := os.MkdirAll(filepath.Join(home, ".juicefs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".juicefs", "juicefs.log"), earlier, 0600); err != nil {
		t.Fatal(err)
	}
	noteObjectKeyInUse("mac-4f1c2a9e7b3d-1")
	noteObjectKeyLogLine(jfsLine("WARNING", "Upload x: "+s3Err("PutObject", "InvalidAccessKeyId")), time.Now())
	if st := CurrentObjectKeyStatus(); st.Refusals != 1 {
		t.Fatalf("setup: refusals = %d", st.Refusals)
	}
	dir := t.TempDir()
	record := filepath.Join(dir, "child.txt")
	fake := filepath.Join(dir, "fake-juicefs")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nenv > '"+record+"'\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	fm := NewFUSEManager(FUSEConfig{
		JuiceFSBin: fake,
		MountPoint: filepath.Join(dir, "fuse"),
		RedisURL:   "redis://mac-4f1c2a9e7b3d:pw-0123456789@nas.local:30179/1",
		ObjectKey:  ObjectKey{AccessKey: accessKey, SecretKey: plusSlash},
	})
	fm.observeMount = testEmptyFUSESnapshot
	fm.custodyPreflight = func() error { return nil }
	receipt, err := fm.MountWithReceipt()
	if receipt == nil {
		t.Fatalf("child never started: %v", err)
	}
	if receipt.juiceFSLogStart != int64(len(earlier)) {
		t.Fatalf("tail start offset = %d, want %d (the log size before the child started)", receipt.juiceFSLogStart, len(earlier))
	}
	select {
	case <-receipt.waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stand-in juicefs did not exit")
	}
	_ = receipt.Rollback()
	st := CurrentObjectKeyStatus()
	if st.InUse != accessKey || st.Refusals != 0 || st.RefusedCode != "" {
		t.Fatalf("status after a new child = %+v, want in-use %s and no refusals", st, accessKey)
	}
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range strings.Split(string(data), "\n") {
		found = found || l == "MINIO_SECRET_KEY="+plusSlash
	}
	if !found {
		t.Fatal("the '+' secret did not reach juicefs unchanged")
	}

	// A child with no key: nothing is in use.
	fm2 := NewFUSEManager(FUSEConfig{JuiceFSBin: fake, MountPoint: filepath.Join(dir, "fuse2"), RedisURL: "redis://nas.local:30179/1"})
	fm2.observeMount = testEmptyFUSESnapshot
	fm2.custodyPreflight = func() error { return nil }
	r2, _ := fm2.MountWithReceipt()
	if r2 == nil {
		t.Fatal("second child never started")
	}
	<-r2.waitDone
	_ = r2.Rollback()
	if st := CurrentObjectKeyStatus(); st.InUse != "" {
		t.Fatalf("in use without a key = %q", st.InUse)
	}
}

// C2 (health half): a refusal line appended to the live juicefs log is
// noticed by the tail. The bridge turns the count into an immediate poll.
func TestTailJuiceFSLogFlagsObjectKeyRefusal(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	logPath := filepath.Join(home, ".juicefs", "juicefs.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte(jfsLine("WARNING", "old "+s3Err("PutObject", "InvalidAccessKeyId"))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	noteObjectKeyInUse("mac-4f1c2a9e7b3d-1")
	fm := NewFUSEManager(FUSEConfig{MountPoint: filepath.Join(home, "fuse")})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); fm.tailJuiceFSLog(stop, -1) }()
	defer func() { close(stop); <-done }()

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// A harmless line first: the tail must not count it.
	benign := jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: dial tcp: i/o timeout (try 1)")
	refusal := jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: "+s3Err("PutObject", "InvalidAccessKeyId")+" (try 1)")
	deadline := time.Now().Add(10 * time.Second)
	for CurrentObjectKeyStatus().Refusals == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the log tail never flagged an InvalidAccessKeyId upload line")
		}
		// The tail seeks to the end when it opens the file, so keep
		// appending until one line lands after that.
		if _, err := f.WriteString(benign + "\n" + refusal + "\n"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	st := CurrentObjectKeyStatus()
	if st.RefusedCode != "InvalidAccessKeyId" || st.InUse != "mac-4f1c2a9e7b3d-1" || st.RefusedAtMS == 0 {
		t.Fatalf("status = %+v", st)
	}
}

// Review keys-l3 F5: the log is truncated in place (the user clears
// ~/.juicefs/juicefs.log; juicefs keeps appending through its O_APPEND
// descriptor). The reader starts again at 0 and sees the next refusal.
func TestLogLineReaderRewindsAfterTruncation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "juicefs.log")
	w, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	pad := strings.Repeat("x", 4000)
	for i := 0; i < 50; i++ {
		_, _ = w.WriteString(jfsLine("INFO", pad) + "\n")
	}
	r, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	lr := newLogLineReader(r, 1<<20)
	if _, ok, _ := lr.next(); ok {
		t.Fatal("a line before any was written")
	}
	_, _ = w.WriteString("partial line that the truncation cuts")
	if _, ok, _ := lr.next(); ok {
		t.Fatal("a partial line was returned")
	}
	if err := os.Truncate(p, 0); err != nil {
		t.Fatal(err)
	}
	refusal := jfsLine("WARNING", "Upload x: "+s3Err("PutObject", "InvalidAccessKeyId"))
	_, _ = w.WriteString(refusal + "\n")
	got, ok, err := lr.next()
	if err != nil || !ok || got != refusal {
		t.Fatalf("after an in-place truncation next() = %.80q, %v, %v; want the new refusal line", got, ok, err)
	}
}

// Review keys-l3 F5: the tail starts after mount verification (up to a
// minute), so it reads from the log size recorded before the child started:
// a refusal the child logged during verification is seen, and an older line
// from an earlier child is not.
func TestTailJuiceFSLogReadsFromChildStart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	logPath := filepath.Join(home, ".juicefs", "juicefs.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0700); err != nil {
		t.Fatal(err)
	}
	refusal := jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: "+s3Err("PutObject", "InvalidAccessKeyId")+" (try 1)")
	older := refusal + "\n" + refusal + "\n"
	if err := os.WriteFile(logPath, []byte(older+refusal+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	noteObjectKeyInUse("mac-4f1c2a9e7b3d-1")
	fm := NewFUSEManager(FUSEConfig{MountPoint: filepath.Join(home, "fuse")})
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); fm.tailJuiceFSLog(stop, int64(len(older))) }()
	defer func() { close(stop); <-done }()
	deadline := time.Now().Add(5 * time.Second)
	for CurrentObjectKeyStatus().Refusals == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the refusal logged during mount verification was never seen")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	if n := CurrentObjectKeyStatus().Refusals; n != 1 {
		t.Fatalf("refusals = %d, want 1 (lines before the child started must not count)", n)
	}
}

// The tail reads lines appended after it reached EOF, and joins a line that
// arrived in two writes (a bufio.Scanner did neither: it stops at its first
// EOF).
func TestLogLineReaderFollowsAGrowingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	w, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	lr := newLogLineReader(r, 32)
	expect := func(want string, wantOK bool) {
		t.Helper()
		got, ok, err := lr.next()
		if err != nil || ok != wantOK || got != want {
			t.Fatalf("next() = %q, %v, %v; want %q, %v", got, ok, err, want, wantOK)
		}
	}
	expect("", false)
	_, _ = w.WriteString("one\ntw")
	expect("one", true)
	expect("", false)
	_, _ = w.WriteString("o\r\n")
	expect("two", true)
	_, _ = w.WriteString(strings.Repeat("x", 100) + "\nthree\n")
	expect("three", true)
	expect("", false)
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Now that the tail reads lines again, a burst of juicefs warnings (a
// revoked key retries every upload) must not flood the JuiceMount log: at
// most juiceFSPromoteBudget lines a minute are copied one by one, and every
// line still reaches the refusal hook.
func TestTailJuiceFSLogPromotionIsBounded(t *testing.T) {
	var buf lockedBuffer
	if err := jmlog.Init(jmlog.Config{Stderr: &buf, Level: jmlog.LevelDebug}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jmlog.Init(jmlog.Config{}) })
	home := t.TempDir()
	t.Setenv("HOME", home)
	logPath := filepath.Join(home, ".juicefs", "juicefs.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	noteObjectKeyInUse("mac-4f1c2a9e7b3d-1")
	fm := NewFUSEManager(FUSEConfig{MountPoint: filepath.Join(home, "fuse")})
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); fm.tailJuiceFSLog(stop, -1) }()
	defer func() { close(stop); <-done }()
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	refusal := jfsLine("WARNING", "Upload chunks/0/1/1_0_4194304: "+s3Err("PutObject", "InvalidAccessKeyId")+" (try 1)")
	// Wait until the tail is reading (it seeks to the end when it opens).
	deadline := time.Now().Add(10 * time.Second)
	for CurrentObjectKeyStatus().Refusals == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the tail never started reading")
		}
		_, _ = f.WriteString(refusal + "\n")
		time.Sleep(100 * time.Millisecond)
	}
	before := CurrentObjectKeyStatus().Refusals
	var burst strings.Builder
	for i := 0; i < 200; i++ {
		burst.WriteString(refusal + "\n")
	}
	_, _ = f.WriteString(burst.String())
	for CurrentObjectKeyStatus().Refusals < before+200 {
		if time.Now().After(deadline) {
			t.Fatalf("the refusal hook saw %d of 200 burst lines", CurrentObjectKeyStatus().Refusals-before)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// A literal, not juiceFSPromoteBudget: a test that compares against
	// the constant it checks passes whatever the constant is (review
	// keys-l3 F7, neuter N9).
	const wantBudget = 30
	if juiceFSPromoteBudget != wantBudget {
		t.Fatalf("juiceFSPromoteBudget = %d, want %d", juiceFSPromoteBudget, wantBudget)
	}
	if n := strings.Count(buf.String(), "InvalidAccessKeyId"); n > wantBudget || n == 0 {
		t.Fatalf("%d juicefs lines promoted to the JuiceMount log in one burst, budget %d", n, wantBudget)
	}
}
