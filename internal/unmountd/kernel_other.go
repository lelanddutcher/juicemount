//go:build !darwin || !cgo

package unmountd

import "syscall"

// DarwinKernel is unavailable off darwin/cgo; every call fails closed.
type DarwinKernel struct{}

func (DarwinKernel) Snapshot() (Snapshot, error)   { return Snapshot{}, syscall.ENOTSUP }
func (DarwinKernel) UnmountByFSID(FSID, int) error { return syscall.ENOTSUP }

// ProcHolders is unavailable off darwin/cgo.
type ProcHolders struct{ PIDs func() ([]int32, error) }

func (ProcHolders) Holders(uint32, FSID) ([]Holder, error) { return nil, syscall.ENOTSUP }
