package client

import "strings"

// Signing identities. The requirement strings below name these exactly, and
// scripts/verify-build.sh checks that the bundle's binaries carry them.
const (
	// TeamID is the Developer ID team that signs every JuiceMount binary.
	TeamID = "PKUE74YS72"
	// ServiceSigningID is the code-signing identifier of
	// Contents/MacOS/juicemount-service. build-app.sh signs that binary
	// without --identifier, so codesign uses the file name. If the build
	// ever passes --identifier, change this constant in the same commit;
	// verify-build.sh fails the bundle until they agree.
	ServiceSigningID = "juicemount-service"
	// AppSigningID is the GUI app's bundle identifier.
	AppSigningID = "com.juicemount.app"
	// HelperSigningID is the daemon's explicit --identifier.
	HelperSigningID = "com.juicemount.unmountd"
)

// Channels.
const (
	ChannelRelease = "release"
	ChannelDev     = "dev"
)

// developerIDClauses is the Developer ID anchor for TeamID:
//   - anchor apple generic: the chain ends at Apple's root (no ad-hoc, no self-signed);
//   - 1.2.840.113635.100.6.2.6 on the intermediate: Developer ID Certification Authority;
//   - 1.2.840.113635.100.6.1.13 on the leaf: Developer ID Application;
//   - the leaf's OU is our team.
const developerIDClauses = `anchor apple generic` +
	` and certificate 1[field.1.2.840.113635.100.6.2.6] exists` +
	` and certificate leaf[field.1.2.840.113635.100.6.1.13] exists` +
	` and certificate leaf[subject.OU] = "` + TeamID + `"`

// noDebugClause rejects a peer that allows any same-user process to take its
// task port. It is in both channels.
const noDebugClause = `!(entitlement["com.apple.security.get-task-allow"] exists)`

// notarizedClause separates release from dev. Dev builds are Developer-ID
// signed but not notarized, and build-app.sh gives them no get-task-allow,
// so the entitlement clause alone does not exclude them.
const notarizedClause = `notarized`

func identifiers(ids ...string) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = `identifier "` + id + `"`
	}
	return "(" + strings.Join(parts, " or ") + ")"
}

func peerRequirement(channel string, ids ...string) string {
	r := developerIDClauses
	if channel != ChannelDev {
		r += " and " + notarizedClause
	}
	return r + " and " + noDebugClause + " and " + identifiers(ids...)
}

// ServiceRequirement is the requirement the daemon sets on every connection
// to MachService. Only the service binary passes it (RoleService).
// Any channel other than ChannelDev yields the release string.
func ServiceRequirement(channel string) string {
	return peerRequirement(channel, ServiceSigningID)
}

// StatusRequirement is the requirement on StatusMachService (RoleGUI).
func StatusRequirement(channel string) string {
	return peerRequirement(channel, AppSigningID, ServiceSigningID)
}

// DaemonRequirement is what clients require of the daemon. It is the same in
// both channels: the client authenticates the server's team and identifier;
// notarization of the server adds nothing a root-owned launchd mach service
// does not already give, and a dev daemon must be reachable from a dev
// service built without any tag.
func DaemonRequirement() string {
	return developerIDClauses + " and " + identifiers(HelperSigningID)
}
