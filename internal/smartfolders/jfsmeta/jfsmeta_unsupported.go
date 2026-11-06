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

func OpenReadOnly(string) (*FS, error) { return nil, errors.ErrUnsupported }

func (f *FS) Flush()       {}
func (f *FS) Close() error { return nil }

func (f *FS) VolumeUUID() string                { return "" }
func (f *FS) LookupIno(string) (Node, error)    { return Node{}, errors.ErrUnsupported }
func (f *FS) StatIno(uint64) (Node, error)      { return Node{}, errors.ErrUnsupported }
func (f *FS) GetPaths(uint64) ([]string, error) { return nil, errors.ErrUnsupported }
