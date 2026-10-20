//go:build darwin || linux

package main

import (
	"errors"
	"os"
	"syscall"
)

func descriptorFlags(fd int) (int, error) {
	r, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFL, 0)
	if e != 0 {
		return 0, e
	}
	return int(r), nil
}
func setDescriptorFlags(fd, flags int) error {
	_, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFL, uintptr(flags))
	if e != 0 {
		return e
	}
	return nil
}
func duplicateDescriptor(fd uintptr) (int, error) {
	r, _, e := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_DUPFD, 3)
	if e != 0 {
		return -1, e
	}
	syscall.CloseOnExec(int(r))
	return int(r), nil
}

type stdioDescriptor struct {
	original             *os.File
	control, ioFD, flags int
	pollable             bool
}

func terminationSignals() []os.Signal { return []os.Signal{os.Interrupt, syscall.SIGTERM} }

// Capture every original status flag before modifying any shared description.
// Raw control descriptors survive canceled I/O closes so restoration is possible.
// IO duplicates >2 keep Go's stdoutOrErr SIGPIPE special case out of this path.
func prepareOwnedStdio(originals []*os.File) (files []*os.File, cleanup func() error, resultErr error) {
	var descriptors []stdioDescriptor
	var opened []*os.File
	cleanup = func() error {
		var result error
		for i := len(descriptors) - 1; i >= 0; i-- {
			d := descriptors[i]
			if d.control >= 0 {
				result = errors.Join(result, setDescriptorFlags(d.control, d.flags), syscall.Close(d.control))
			}
		}
		seen := make(map[*os.File]bool)
		for _, f := range originals {
			if f != nil && !seen[f] {
				seen[f] = true
				result = errors.Join(result, f.Close())
			}
		}
		return result
	}
	defer func() {
		if resultErr != nil {
			for _, f := range opened {
				resultErr = errors.Join(resultErr, f.Close())
			}
			for _, d := range descriptors {
				if d.ioFD >= 0 {
					resultErr = errors.Join(resultErr, syscall.Close(d.ioFD))
				}
			}
			resultErr = errors.Join(resultErr, cleanup())
			cleanup = nil
		}
	}()
	for _, f := range originals {
		if f == nil {
			return nil, cleanup, errors.New("standard stream unavailable")
		}
		info, err := f.Stat()
		if err != nil {
			return nil, cleanup, err
		}
		connection, err := f.SyscallConn()
		if err != nil {
			return nil, cleanup, err
		}
		d := stdioDescriptor{original: f, control: -1, ioFD: -1, pollable: info.Mode()&(os.ModeNamedPipe|os.ModeCharDevice) != 0}
		var captureErr error
		err = connection.Control(func(fd uintptr) {
			d.flags, captureErr = descriptorFlags(int(fd))
			if captureErr != nil {
				return
			}
			d.control, captureErr = duplicateDescriptor(fd)
			if captureErr != nil {
				return
			}
			d.ioFD, captureErr = duplicateDescriptor(fd)
		})
		descriptors = append(descriptors, d)
		if err != nil || captureErr != nil {
			return nil, cleanup, errors.Join(err, captureErr)
		}
	}
	for i := range descriptors {
		d := &descriptors[i]
		if d.pollable {
			if err := setDescriptorFlags(d.ioFD, d.flags|syscall.O_NONBLOCK); err != nil {
				return nil, cleanup, err
			}
		}
		f := os.NewFile(uintptr(d.ioFD), "catalog-owned-stdio")
		if f == nil {
			return nil, cleanup, errors.New("standard stream unavailable")
		}
		d.ioFD = -1
		files = append(files, f)
		opened = append(opened, f)
	}
	return files, cleanup, nil
}
