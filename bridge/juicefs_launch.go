package main

import (
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
)

// engineLaunchJuiceFS chooses the juicefs the engine start path mounts with.
// A retained FUSE manager keeps the executable it already runs (it was chosen
// by this function when it started). Otherwise an empty configuration takes
// the bundled juicefs and an explicit one must be the bundled juicefs.
func engineLaunchJuiceFS(retained bool, retainedBin, configured string) (string, error) {
	if retained {
		return retainedBin, nil
	}
	return ownerLaunchJuiceFS(configured)
}

// ownerLaunchJuiceFS chooses the juicefs a volume owner claim mounts with:
// the bundled one, or a typed *juicefsbin.NotBundledError for any other
// explicitly configured path.
func ownerLaunchJuiceFS(configured string) (string, error) {
	if configured == "" {
		return health.ResolveJuiceFSBinary(), nil
	}
	return juicefsbin.Validate(configured, "start configuration")
}
