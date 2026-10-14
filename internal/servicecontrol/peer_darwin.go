//go:build darwin

package servicecontrol

import (
	"net"

	"golang.org/x/sys/unix"
)

func platformSupported() bool { return true }

func peerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var uid uint32
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		var cred *unix.Xucred
		cred, socketErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if socketErr == nil {
			uid = cred.Uid
		}
	})
	if err != nil {
		return 0, err
	}
	return uid, socketErr
}

func peerPID(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var got int
	var socketErr error
	err = raw.Control(func(fd uintptr) { got, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID) })
	if err != nil {
		return 0, err
	}
	return got, socketErr
}
