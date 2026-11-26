package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// noReadahead turns off the kernel's read-ahead on f (F_RDAHEAD 0), so a
// read fetches only what the probe asks for.
func noReadahead(f *os.File) { _, _ = unix.FcntlInt(f.Fd(), unix.F_RDAHEAD, 0) }

// controlName is the control file the app uses on macOS (patch 0033: its own
// lane, not the shared .control).
func controlName() string { return ".jmcontrol" }
