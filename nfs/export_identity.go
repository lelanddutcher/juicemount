package nfs

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

const (
	exportHandleVersion  uint32 = 1
	exportGenerationSize        = 16
	// A uint32 version keeps the opaque handle aligned for XDR consumers.
	exportHandleSize = 4 + exportGenerationSize + 8
)

// An export identity separates server lifetimes. It is not an authentication
// secret or proof of ownership of a kernel mount. No external restoration API
// exists: adoption after process restart requires a verified broker receipt.
type exportIdentity struct {
	generation [exportGenerationSize]byte
	initErr    error
}

func newExportIdentity(entropy io.Reader) exportIdentity {
	var identity exportIdentity
	if _, err := io.ReadFull(entropy, identity.generation[:]); err != nil {
		identity.initErr = fmt.Errorf("initialize NFS export identity: %w", err)
	}
	return identity
}

func (e exportIdentity) validate() error {
	if e.initErr != nil {
		return e.initErr
	}
	if e.generation == [exportGenerationSize]byte{} {
		return errors.New("NFS export identity is uninitialized")
	}
	return nil
}

// ExportPath is the only export accepted by this handler. An empty result
// means identity initialization failed; callers must not publish a mount.
func (h *JuiceMountHandler) ExportPath() string {
	if h == nil || h.export.validate() != nil {
		return ""
	}
	return "/jm-" + hex.EncodeToString(h.export.generation[:])
}

// ExportPath remains stable across Stop/Start of this same Server. A newly
// constructed Server has a fresh export and rejects handles from the old one.
func (s *Server) ExportPath() string { return s.handler.ExportPath() }

func (e exportIdentity) handle(inode uint64) []byte {
	handle := make([]byte, exportHandleSize)
	binary.BigEndian.PutUint32(handle, exportHandleVersion)
	copy(handle[4:], e.generation[:])
	binary.BigEndian.PutUint64(handle[4+exportGenerationSize:], inode)
	return handle
}

func (e exportIdentity) inode(handle []byte) (uint64, error) {
	if e.validate() != nil || len(handle) != exportHandleSize ||
		binary.BigEndian.Uint32(handle[:4]) != exportHandleVersion ||
		[exportGenerationSize]byte(handle[4:4+exportGenerationSize]) != e.generation {
		return 0, &nfslib.NFSStatusError{NFSStatus: nfslib.NFSStatusStale}
	}
	return binary.BigEndian.Uint64(handle[4+exportGenerationSize:]), nil
}
