//go:build !darwin && !linux

package servicecontrol

import (
	"errors"
	"net"
)

func platformSupported() bool { return false }

func peerUID(*net.UnixConn) (uint32, error) {
	return 0, errors.New("service control peer authentication unsupported on this platform")
}
