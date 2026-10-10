package nfscustody

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"golang.org/x/sys/unix"
)

func testProvisionRequest() provisionRequest {
	return provisionRequest{
		Version: 1, Mode: "create", Nonce: strings.Repeat("a", 32),
		Boot: "test-boot", Operation: strings.Repeat("b", 32),
		Path: "/Volumes/test-owned-leaf", UID: 501, GID: 0,
		ExpectedParentDev: 1, ExpectedParentIno: 2,
	}
}

func provisionArgs(r provisionRequest) []string {
	return []string{r.Mode, r.Nonce, r.Boot, r.Operation, r.Path,
		strconv.FormatUint(uint64(r.UID), 10), strconv.FormatUint(uint64(r.GID), 10),
		strconv.FormatUint(r.ExpectedParentDev, 10), strconv.FormatUint(r.ExpectedParentIno, 10),
		strconv.FormatUint(r.ExpectedLeafDev, 10), strconv.FormatUint(r.ExpectedLeafIno, 10)}
}

func TestProvisionRequestPrincipalAndPath(t *testing.T) {
	r := testProvisionRequest()
	if !r.valid() {
		t.Fatal("nonroot UID with GID zero must be valid")
	}
	for _, change := range []struct {
		name string
		fn   func(*provisionRequest)
	}{
		{"root-uid", func(x *provisionRequest) { x.UID = 0 }},
		{"uid-chown-sentinel", func(x *provisionRequest) { x.UID = ^uint32(0) }},
		{"gid-chown-sentinel", func(x *provisionRequest) { x.GID = ^uint32(0) }},
		{"foreign-parent", func(x *provisionRequest) { x.Path = "/tmp/test-owned-leaf" }},
		{"preexisting-create-leaf", func(x *provisionRequest) { x.ExpectedLeafIno = 9 }},
		{"unbound-migration", func(x *provisionRequest) { x.Mode = "migrate" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			bad := r
			change.fn(&bad)
			if bad.valid() {
				t.Fatal("unsafe provision request accepted")
			}
		})
	}
	r.Path = "/Volumes/O'Neil $(printf unsafe) `echo nope`"
	if !r.valid() {
		t.Fatal("literal shell metacharacters are valid leaf bytes and must be quoted in transport")
	}
}

func TestProvisionRunTerminalEvidence(t *testing.T) {
	r := testProvisionRequest()
	for _, tc := range []struct {
		name          string
		effect        bool
		nativeErr     error
		wantState     string
		wantNoEffects bool
	}{
		{"known-refusal", false, errors.New("collision"), "refused", true},
		{"partial-effect", true, errors.New("postcondition"), "uncertain", false},
		{"success", true, nil, "created", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, diagnostic bytes.Buffer
			called := 0
			status := provisionRunWith(provisionArgs(r), &out, &diagnostic, 0, func(got provisionRequest) (provisionReceipt, bool, error) {
				called++
				if got != r {
					t.Fatalf("helper correlation lost: %+v", got)
				}
				return provisionReceipt{Request: got, State: "created", ParentDev: 1, ParentIno: 2, LeafDev: 1, LeafIno: 3, AfterUID: got.UID, AfterGID: got.GID, AfterMode: 0700}, tc.effect, tc.nativeErr
			})
			if status != 0 || called != 1 {
				t.Fatalf("status=%d called=%d diagnostic=%s", status, called, diagnostic.String())
			}
			var reply provisionReceipt
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			if reply.Request != r || reply.State != tc.wantState || reply.NoEffects != tc.wantNoEffects {
				t.Fatalf("unexpected terminal evidence: %+v", reply)
			}
		})
	}
	var out, diagnostic bytes.Buffer
	called := false
	bad := r
	bad.GID = ^uint32(0)
	if status := provisionRunWith(provisionArgs(bad), &out, &diagnostic, 0, func(provisionRequest) (provisionReceipt, bool, error) {
		called = true
		return provisionReceipt{}, false, nil
	}); status == 0 || called || out.Len() != 0 {
		t.Fatalf("invalid principal reached native effect: status=%d called=%t", status, called)
	}
}

func TestPersistProvisionReceiptSameOutcomeRetry(t *testing.T) {
	dir := t.TempDir()
	r := testProvisionRequest()
	receipt := provisionReceipt{Request: r, State: "created", ParentDev: r.ExpectedParentDev, ParentIno: r.ExpectedParentIno, LeafDev: 1, LeafIno: 3, AfterUID: r.UID, AfterGID: r.GID, AfterMode: 0700}
	if err := persistProvisionReceipt(dir, receipt); err != nil {
		t.Fatal(err)
	}
	if err := persistProvisionReceipt(dir, receipt); err != nil {
		t.Fatalf("same original outcome must be retryable: %v", err)
	}
	receipt.LeafIno++
	if err := persistProvisionReceipt(dir, receipt); err == nil {
		t.Fatal("different outcome overwrote fixed operation receipt")
	}
	data, err := os.ReadFile(filepath.Join(dir, r.Operation+".provision.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved provisionReceipt
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.LeafIno != 3 {
		t.Fatal("same-operation mismatch changed durable receipt")
	}
}

func TestProvisionAppleScriptQuotedArgumentTransport(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("osascript is Darwin-only")
	}
	if _, err := exec.LookPath("osascript"); err != nil {
		t.Skip("osascript unavailable")
	}
	dir := t.TempDir()
	helper := filepath.Join(dir, "literal ' $() ` helper.sh")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "shell-executed")
	r := testProvisionRequest()
	r.Path = "/Volumes/O'Neil $(touch shell-executed) `touch shell-executed`"
	if !r.valid() {
		t.Fatal("test path invalid")
	}
	// Only the final privileged execution line is replaced. The production
	// on-run argv loop and its quoted-form construction remain byte-identical.
	safeScript := strings.Replace(provisionAppleScript, "return do shell script commandText with administrator privileges with prompt promptText", "return commandText", 1)
	if safeScript == provisionAppleScript {
		t.Fatal("privileged line not isolated")
	}
	args := append([]string{"-e", safeScript, helper}, provisionArgs(r)...)
	commandText, err := exec.Command("/usr/bin/osascript", args...).Output()
	if err != nil {
		t.Fatal(err)
	}
	shell := exec.Command("/bin/sh", "-c", strings.TrimSpace(string(commandText)))
	shell.Dir = dir
	out, err := shell.Output()
	if err != nil {
		t.Fatal(err)
	}
	want := append([]string{"--nfs-provision-leaf"}, provisionArgs(r)...)
	got := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv changed across quoted transport: got=%q want=%q", got, want)
	}
	if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shell metacharacters caused an effect: %v", err)
	}
}

func TestStopRetainsUnknownProvisionOutcome(t *testing.T) {
	c, _, _ := noMountController(t)
	done := c.createOperation(context.Background(), func(rec mountbroker.Record) error {
		c.mu.Lock()
		c.record = &rec
		c.provisionAttempted = true
		c.mu.Unlock()
		return errors.New("privilege wrapper output lost")
	})
	<-done
	if err := c.Stop(context.Background(), time.Second); err == nil || !strings.Contains(err.Error(), "provision outcome remains uncertain") {
		t.Fatalf("uncertain privilege operation was not retained by provision guard: %v", err)
	}
	if c.retired {
		t.Fatal("uncertain privilege operation was dropped")
	}
}

func TestProvisionOrdinaryOwnedEmptyDirectoryNoACL(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin ACL and NFS target fixture")
	}
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "volume")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	g, err := openOwnedTarget(path)
	if err != nil {
		t.Fatalf("ordinary owned no-ACL target rejected: %v", err)
	}
	defer g.Close()
	if err := g.Revalidate(); err != nil {
		t.Fatalf("ordinary target revalidation rejected: %v", err)
	}
}

func TestParseProvisionReplyRefusesUnboundOrAmbiguousFrames(t *testing.T) {
	r := testProvisionRequest()
	good := provisionReceipt{Request: r, State: "created", ParentDev: r.ExpectedParentDev, ParentIno: r.ExpectedParentIno, LeafDev: 1, LeafIno: 3, AfterUID: r.UID, AfterGID: r.GID, AfterMode: 0700}
	encode := func(x provisionReceipt) []byte {
		b, err := json.Marshal(x)
		if err != nil {
			t.Fatal(err)
		}
		return append(b, '\n')
	}
	if _, refused, err := parseProvisionReply(encode(good), r); err != nil || refused {
		t.Fatalf("valid original receipt rejected: %v", err)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"second-frame", append(encode(good), encode(good)...)},
		{"trailing-non-json", append(encode(good), []byte("not-json")...)},
		{"wrong-operation", func() []byte { x := good; x.Request.Operation = strings.Repeat("c", 32); return encode(x) }()},
		{"wrong-gid", func() []byte { x := good; x.AfterGID++; return encode(x) }()},
		{"wrong-mode", func() []byte { x := good; x.AfterMode = 0777; return encode(x) }()},
		{"wrong-action", func() []byte { x := good; x.State = "migrated"; return encode(x) }()},
		{"effectful-refusal", func() []byte { x := good; x.State = "refused"; x.NoEffects = false; return encode(x) }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := parseProvisionReply(tc.data, r); err == nil {
				t.Fatal("unbound or ambiguous helper frame accepted")
			}
		})
	}
	refused := good
	refused.State = "refused"
	refused.NoEffects = true
	if _, known, err := parseProvisionReply(encode(refused), r); err != nil || !known {
		t.Fatalf("known no-effect refusal not terminal: %v", err)
	}
}

func TestProvisionRetainsUncertainTransportEvidence(t *testing.T) {
	r := testProvisionRequest()
	partial := provisionReceipt{Request: r, State: "uncertain", LeafDev: 1, LeafIno: 9}
	raw, err := json.Marshal(partial)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		output  []byte
		waitErr error
	}{
		{"partial-effect", raw, nil},
		{"abnormal-wrapper", raw, errors.New("wrapper exit")},
		{"malformed", []byte("incomplete receipt"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Controller{}
			input := append([]byte(nil), tc.output...)
			if err := c.finishProvision(r, input, nil, nil, tc.waitErr); err == nil {
				t.Fatal("uncertain transport accepted")
			}
			input[0] = '!'
			if !bytes.Equal(c.provisionOutput, tc.output) {
				t.Fatal("original bounded provision output was lost or aliased")
			}
			if c.provisionReceipt != nil || c.provisionSettled {
				t.Fatal("diagnostic output became settlement authority")
			}
			if c.provisionWaitErr != tc.waitErr {
				t.Fatal("original wrapper Wait result was lost")
			}
		})
	}
}

func TestProvisionAncestryACLAllowsOnlyOwnerMutation(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin ACL policy")
	}
	dir := t.TempDir()
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	// These ACLs are limited to the test-owned directory; no ancestor is changed.
	uid := strconv.Itoa(os.Getuid())
	account, err := user.LookupId(uid)
	if err != nil || account == nil || account.Uid != uid || account.Username == "" {
		t.Fatalf("current fixture owner account unavailable: %v", err)
	}
	grant := "user:" + account.Username + " allow add_file,add_subdirectory,writeattr,writesecurity"
	if out, err := exec.Command("/bin/chmod", "+a", grant, dir).CombinedOutput(); err != nil {
		t.Fatalf("owned fixture ACL: %v %s", err, out)
	}
	if err := provisionACL(fd, false); err != nil {
		t.Fatalf("owner-only ancestry mutation rejected: %v", err)
	}
	if err := provisionACL(fd, true); err == nil {
		t.Fatal("leaf ACL unexpectedly accepted")
	}
	if out, err := exec.Command("/bin/chmod", "+a", "everyone allow add_file", dir).CombinedOutput(); err != nil {
		t.Fatalf("foreign fixture ACL: %v %s", err, out)
	}
	if err := provisionACL(fd, false); err == nil {
		t.Fatal("foreign mutation ACL unexpectedly accepted")
	}
}
