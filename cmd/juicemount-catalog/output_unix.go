//go:build darwin || linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func reserveOutput(input string) (*os.File, error) {
	abs, err := filepath.Abs(input)
	if err != nil {
		return nil, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return nil, err
	}
	path := filepath.Join(parent, filepath.Base(abs))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err := f.Chmod(0600); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Nlink != 1 {
		return nil, errors.Join(errors.New("output identity"), f.Close())
	}
	return f, nil
}

// Named inputs are regular files only. Nonblocking open refuses a FIFO without
// waiting for a writer before the cancellation registry can own the handle.
func openInput(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(errors.New("input must be a regular file"), f.Close())
	}
	return f, nil
}
