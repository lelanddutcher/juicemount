//go:build !darwin && !linux

package main

import (
	"errors"
	"os"
)

func reserveOutput(string) (*os.File, error) {
	return nil, errors.New("output identity unqualified on this platform")
}

func openInput(string) (*os.File, error) {
	return nil, errors.New("input identity unqualified on this platform")
}
func prepareOwnedStdio(files []*os.File) ([]*os.File, func() error, error) {
	var err error
	for _, f := range files {
		if f != nil {
			err = errors.Join(err, f.Close())
		}
	}
	return nil, nil, errors.Join(errors.New("stdio cancellation unqualified on this platform"), err)
}

func terminationSignals() []os.Signal { return []os.Signal{os.Interrupt} }
