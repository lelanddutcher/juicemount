package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// noReadahead turns off the kernel's read-ahead on f, so a read fetches only
// what the probe asks for.
func noReadahead(f *os.File) { _ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_RANDOM) }

// controlName is the control file the app uses on this system.
func controlName() string { return ".control" }
