//go:build !linux && !darwin && !windows

package jfsmeta

import (
	"errors"
	"time"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

// The vendored JuiceFS metadata client does not build on this system.

const OpenTimeout = 15 * time.Second

type FS struct{ smartfolders.FS }

func Open(string) (*FS, error) { return nil, errors.ErrUnsupported }

func OpenWithin(string, time.Duration) (*FS, error) { return nil, errors.ErrUnsupported }

func (f *FS) Flush()       {}
func (f *FS) Close() error { return nil }
