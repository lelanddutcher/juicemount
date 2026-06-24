// Package version is the single version-of-record for JuiceMount (contract JM-6).
//
// Before this package, three different values disagreed: Info.plist said
// "2.0.0", the manager binary said "dev", and no Go constant existed. The
// control-plane /whoami endpoint and the OpenLoupe contract need ONE
// authoritative public version string that matches the git tag and the
// notarized release.
//
// Keep Version in sync with the git tag and app/JuiceMount/Resources/Info.plist
// CFBundleShortVersionString. The release process may override it at build time
// with: -ldflags "-X github.com/lelanddutcher/juicemount/internal/version.Version=x.y.z".
package version

// Version is the public release version string (the git tag / notarized
// release). Distinct from the wire contract_version (see internal/cplane).
var Version = "0.1.0"
