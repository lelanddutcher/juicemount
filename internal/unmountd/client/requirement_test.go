package client

import (
	"strings"
	"testing"
)

const devIDTeam = `anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists` +
	` and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = "PKUE74YS72"`

// The exact strings are pinned: a change here is a change to who may drive
// a root daemon, and must be reviewed as such.
func TestRequirementStringsExact(t *testing.T) {
	want := map[string]string{
		"release service": devIDTeam + ` and notarized and !(entitlement["com.apple.security.get-task-allow"] exists) and (identifier "juicemount-service")`,
		"release status":  devIDTeam + ` and notarized and !(entitlement["com.apple.security.get-task-allow"] exists) and (identifier "com.juicemount.app" or identifier "juicemount-service")`,
		"dev service":     devIDTeam + ` and !(entitlement["com.apple.security.get-task-allow"] exists) and (identifier "juicemount-service")`,
		"dev status":      devIDTeam + ` and !(entitlement["com.apple.security.get-task-allow"] exists) and (identifier "com.juicemount.app" or identifier "juicemount-service")`,
		"daemon":          devIDTeam + ` and (identifier "com.juicemount.unmountd")`,
	}
	got := map[string]string{
		"release service": ServiceRequirement(ChannelRelease),
		"release status":  StatusRequirement(ChannelRelease),
		"dev service":     ServiceRequirement(ChannelDev),
		"dev status":      StatusRequirement(ChannelDev),
		"daemon":          DaemonRequirement(),
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s:\n got %s\nwant %s", k, got[k], w)
		}
	}
}

func TestReleaseRequirementExcludesDevBuilds(t *testing.T) {
	for _, r := range []string{ServiceRequirement(ChannelRelease), StatusRequirement(ChannelRelease)} {
		if !strings.Contains(r, " and notarized and ") {
			t.Errorf("release requirement does not require notarization: %s", r)
		}
		if !strings.Contains(r, `!(entitlement["com.apple.security.get-task-allow"] exists)`) {
			t.Errorf("release requirement admits get-task-allow: %s", r)
		}
	}
	// Any channel string other than "dev" must fail closed to release.
	for _, ch := range []string{"", "Dev", "development", "release"} {
		if ServiceRequirement(ch) != ServiceRequirement(ChannelRelease) {
			t.Errorf("channel %q did not fail closed to release", ch)
		}
	}
	for _, r := range []string{ServiceRequirement(ChannelDev), StatusRequirement(ChannelDev)} {
		if strings.Contains(r, "notarized") {
			t.Errorf("dev requirement requires notarization: %s", r)
		}
		if !strings.HasPrefix(r, devIDTeam+" and ") {
			t.Errorf("dev requirement lost the Developer ID team anchor: %s", r)
		}
	}
}

func TestServiceListenerAdmitsOnlyTheService(t *testing.T) {
	for _, ch := range []string{ChannelRelease, ChannelDev} {
		if strings.Contains(ServiceRequirement(ch), AppSigningID) {
			t.Errorf("%s service requirement admits the GUI app", ch)
		}
		if !strings.Contains(StatusRequirement(ch), `identifier "`+AppSigningID+`"`) {
			t.Errorf("%s status requirement does not admit the GUI app", ch)
		}
	}
}
