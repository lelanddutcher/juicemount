package servicecontrol

import (
	"errors"
	"net"
	"os"
)

func requireSameUID(conn *net.UnixConn) error {
	uid, err := peerUID(conn)
	if err != nil {
		return err
	}
	if uid != uint32(os.Geteuid()) {
		return errors.New("service control peer UID differs from current user")
	}
	return nil
}
