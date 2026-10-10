//go:build darwin && cgo

package main

import (
	"fmt"

	"github.com/lelanddutcher/juicemount/internal/unmountd"
)

func init() {
	serveTransport = unmountd.ServeXPC
	checkPeer = func(role, path string) error {
		service, status := unmountd.PeerRequirements()
		switch role {
		case "service":
			return unmountd.StaticSatisfies(path, service)
		case "status":
			return unmountd.StaticSatisfies(path, status)
		}
		return fmt.Errorf("unknown listener role %q (want service or status)", role)
	}
}
