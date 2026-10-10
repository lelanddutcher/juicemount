//go:build jmdevunmountd

package unmountd

import "github.com/lelanddutcher/juicemount/internal/unmountd/client"

// peerChannel: DEV BUILD. Peers must still be Developer-ID signed by our team,
// carry the right identifier and no get-task-allow, but need not be notarized.
// build-app.sh compiles this only with JM_UNMOUNTD_DEV=1 and refuses it with
// --distribution; verify-build.sh --release fails a helper carrying the tag.
const peerChannel = client.ChannelDev
