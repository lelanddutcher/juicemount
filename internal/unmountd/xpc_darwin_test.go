//go:build darwin && cgo

package unmountd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// These tests evaluate the exact requirement strings with the Security
// framework (SecStaticCodeCheckValidity) against real signed files. That is
// the engine libxpc uses, but on a file, not a running peer: what libxpc does
// with a live connection (audit-token based) remains unproven here.
//
// Optional fixtures (skipped when unset):
//
//	JM_UNMOUNTD_SIGNED_FIXTURES  dir with devid-service (Developer ID, team
//	                             PKUE74YS72, identifier juicemount-service,
//	                             hardened runtime, NOT notarized), devid-gta
//	                             (same plus get-task-allow), adhoc-service,
//	                             appledev-service (our team, Apple Development cert).
//	JM_UNMOUNTD_NOTARIZED_APP    a notarized, stapled JuiceMount.app.

func allRequirements() map[string]string {
	return map[string]string{
		"release service": client.ServiceRequirement(client.ChannelRelease),
		"release status":  client.StatusRequirement(client.ChannelRelease),
		"dev service":     client.ServiceRequirement(client.ChannelDev),
		"dev status":      client.StatusRequirement(client.ChannelDev),
		"daemon":          client.DaemonRequirement(),
	}
}

func TestRequirementStringsCompile(t *testing.T) {
	for name, r := range allRequirements() {
		if err := RequirementValid(r); err != nil {
			t.Errorf("%s: %v\n%s", name, err, r)
		}
	}
	if err := RequirementValid(`anchor apple generic and identifier`); err == nil {
		t.Fatal("a malformed requirement compiled: the validity check proves nothing")
	}
}

func mustReject(t *testing.T, what, path, req string) {
	t.Helper()
	// Exactly errSecCSReqFailed: the signature is valid and the requirement
	// is what failed. Any other status (unreadable file, broken signature)
	// would not show that the requirement string does the rejecting.
	if err := StaticSatisfies(path, req); err != ErrRequirementNotSatisfied { //nolint:errorlint // exact sentinel = errSecCSReqFailed
		t.Errorf("%s (%s): got %v, want the requirement itself to fail (errSecCSReqFailed)", what, path, err)
	}
}

func mustAccept(t *testing.T, what, path, req string) {
	t.Helper()
	if err := StaticSatisfies(path, req); err != nil {
		t.Errorf("%s (%s) was rejected: %v", what, path, err)
	}
}

func TestRealRequirementRejectsAdHocAndWrongTeam(t *testing.T) {
	self, err := os.Executable() // the go test binary: linker-signed ad-hoc
	if err != nil {
		t.Fatal(err)
	}
	peers := map[string]string{"ad-hoc (go test binary)": self, "Apple platform binary": "/bin/ls"}
	if _, err := os.Stat("/Applications/Google Chrome.app"); err == nil {
		peers["wrong-team Developer ID (Google)"] = "/Applications/Google Chrome.app"
	}
	for what, path := range peers {
		for name, req := range allRequirements() {
			mustReject(t, what+" vs "+name, path, req)
		}
	}
}

func TestRealRequirementSeparatesDevFromRelease(t *testing.T) {
	dir := os.Getenv("JM_UNMOUNTD_SIGNED_FIXTURES")
	if dir == "" {
		t.Skip("JM_UNMOUNTD_SIGNED_FIXTURES not set: Developer-ID dev fixtures unavailable")
	}
	devid := filepath.Join(dir, "devid-service")
	gta := filepath.Join(dir, "devid-gta")
	adhoc := filepath.Join(dir, "adhoc-service")
	rel := client.ServiceRequirement(client.ChannelRelease)
	dev := client.ServiceRequirement(client.ChannelDev)

	// A Developer-ID, unnotarized dev build: only the dev requirement admits it.
	mustReject(t, "unnotarized Developer-ID service vs release", devid, rel)
	mustAccept(t, "unnotarized Developer-ID service vs dev", devid, dev)
	// get-task-allow is refused in both channels.
	mustReject(t, "debuggable service vs release", gta, rel)
	mustReject(t, "debuggable service vs dev", gta, dev)
	// Right identifier, ad-hoc signature: refused everywhere.
	mustReject(t, "ad-hoc service vs release", adhoc, rel)
	mustReject(t, "ad-hoc service vs dev", adhoc, dev)
	// Same team, but an Apple Development certificate (not Developer ID):
	// the Developer ID OID clauses refuse it in both channels.
	appleDev := filepath.Join(dir, "appledev-service")
	mustReject(t, "Apple Development-signed service vs release", appleDev, rel)
	mustReject(t, "Apple Development-signed service vs dev", appleDev, dev)
	// The daemon requirement does not stretch to a wrong-identifier binary.
	mustReject(t, "service-identifier binary vs daemon requirement", devid, client.DaemonRequirement())
}

func TestRealRequirementAdmitsNotarizedBundle(t *testing.T) {
	app := os.Getenv("JM_UNMOUNTD_NOTARIZED_APP")
	if app == "" {
		t.Skip("JM_UNMOUNTD_NOTARIZED_APP not set: no notarized bundle to check")
	}
	service := filepath.Join(app, "Contents", "MacOS", "juicemount-service")
	mustAccept(t, "notarized service vs release service", service, client.ServiceRequirement(client.ChannelRelease))
	mustAccept(t, "notarized service vs release status", service, client.StatusRequirement(client.ChannelRelease))
	mustAccept(t, "notarized app vs release status", app, client.StatusRequirement(client.ChannelRelease))
	mustReject(t, "notarized app vs release service", app, client.ServiceRequirement(client.ChannelRelease))
}

// The validation seam driven by the real engine: a peer whose signed code
// fails the listener's requirement is refused before anything reaches the
// core.
func TestTransportSeamWithRealRequirementEngine(t *testing.T) {
	tr, cs := newTestTransport(t, newFake(rootEntry(), ourEntry()))
	self, _ := os.Executable()
	static := func(path string) func(string) error {
		return func(req string) error { return StaticSatisfies(path, req) }
	}
	if tr.peerOpen(lService, 1, testUID, 1, static(self)) {
		t.Fatal("ad-hoc peer admitted on the service listener")
	}
	if tr.peerOpen(lStatus, 2, testUID, 1, static("/bin/ls")) {
		t.Fatal("wrong-team peer admitted on the status listener")
	}
	if dir := os.Getenv("JM_UNMOUNTD_SIGNED_FIXTURES"); dir != "" {
		if tr.peerOpen(lService, 3, testUID, 1, static(filepath.Join(dir, "devid-service"))) {
			t.Fatal("unnotarized dev service admitted by the release transport")
		}
	}
	if n := cs.calls.Load(); n != 0 {
		t.Fatalf("refused peers reached the core %d times", n)
	}
	if app := os.Getenv("JM_UNMOUNTD_NOTARIZED_APP"); app != "" {
		if !tr.peerOpen(lService, 4, testUID, 1, static(filepath.Join(app, "Contents", "MacOS", "juicemount-service"))) {
			t.Fatal("notarized service refused")
		}
		if r := send(t, tr, 4, client.Request{Op: client.OpPing}); r.Ping == nil || r.Ping.Role != "service" {
			t.Fatalf("ping: %+v", r)
		}
	}
}

func TestExecutableReplacedDetectsAnUpdate(t *testing.T) {
	p := filepath.Join(t.TempDir(), "juicemount-unmountd")
	write := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write(p, "build 1")
	start, err := identifyExecutable(p)
	if err != nil {
		t.Fatal(err)
	}
	if executableReplaced(p, start) {
		t.Fatal("an untouched executable was reported replaced")
	}
	// A touch changes only the mtime: not an update.
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(p, later, later); err != nil {
		t.Fatal(err)
	}
	if executableReplaced(p, start) {
		t.Fatal("a touch was reported as a replacement")
	}
	// Replaced by rename with identical bytes (same build re-installed).
	write(p+".new", "build 1")
	if err := os.Rename(p+".new", p); err != nil {
		t.Fatal(err)
	}
	if executableReplaced(p, start) {
		t.Fatal("identical content was reported as a replacement")
	}
	// An update replaces the file by rename with new content.
	write(p+".new", "build 2")
	if err := os.Rename(p+".new", p); err != nil {
		t.Fatal(err)
	}
	if !executableReplaced(p, start) {
		t.Fatal("a replaced executable was not detected")
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if !executableReplaced(p, start) {
		t.Fatal("a removed executable was not detected")
	}
}

func TestServiceIdleOnlyWithoutRetirementWork(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	if !svc.idle() {
		t.Fatal("not idle with only a registered receipt (a bound lease is not retirement work)")
	}
	if err := svc.Disconnected(testUID, "op1", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	if svc.idle() {
		t.Fatal("idle while an orphan timer is pending: exiting would drop it")
	}
	if ok, _ := svc.Reclaim(service(), "op1"); !ok {
		t.Fatal("reclaim")
	}
	if !svc.idle() {
		t.Fatal("not idle after the orphan timer was reclaimed")
	}
	if _, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 20*time.Millisecond); err == nil {
		t.Fatal("busy retire finished")
	}
	if svc.idle() {
		t.Fatal("idle while a retirement job runs")
	}
}
