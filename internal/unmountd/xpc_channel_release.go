//go:build !jmdevunmountd

package unmountd

import "github.com/lelanddutcher/juicemount/internal/unmountd/client"

// peerChannel selects the release peer requirements (notarized). A dev build
// (-tags jmdevunmountd) drops only the notarization clause; verify-build.sh
// refuses that tag in a release bundle.
const peerChannel = client.ChannelRelease
