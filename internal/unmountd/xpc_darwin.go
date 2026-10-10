//go:build darwin && cgo

package unmountd

/*
#cgo CFLAGS: -fblocks -mmacosx-version-min=26.0
#cgo LDFLAGS: -mmacosx-version-min=26.0 -framework Security -framework CoreFoundation
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

int  jm_ud_listen(const char *name, int listener);
int  jm_ud_set_requirement(uintptr_t peer, const char *requirement);
void jm_ud_reply(uintptr_t msg, int64_t version, const void *body, size_t len);
void jm_ud_drop(uintptr_t msg);
int  jm_ud_requirement_valid(const char *requirement);
int  jm_ud_static_satisfies(const char *path, int is_dir, const char *requirement);
*/
import "C"

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// activeXPC is the transport the libxpc callbacks deliver to.
var activeXPC atomic.Pointer[xpcTransport]

// listenerFailed receives the index of a listener libxpc invalidated.
var listenerFailed = make(chan int, 2)

// staleCheckEvery is how often ServeXPC looks for a replaced executable.
const staleCheckEvery = 30 * time.Second

// ServeXPC runs the XPC transport for svc until the daemon should exit. It
// listens on client.MachService (RoleService) and client.StatusMachService
// (RoleGUI). It returns nil when the executable on disk was replaced (an app
// update) and no retirement work is in progress: launchd then relaunches the
// new binary on the next message, and each bound owner re-binds through
// "reclaim" (client.Lease) while the new daemon's launch sweep holds its
// receipt in the orphan window. It never returns nil while a job runs.
func ServeXPC(svc *Service, logger *log.Logger) error {
	t := newXPCTransport(svc, peerChannel, logger.Printf)
	for _, l := range t.listeners {
		if err := RequirementValid(l.requirement); err != nil {
			return fmt.Errorf("listener %s: %w", l.name, err)
		}
	}
	if !activeXPC.CompareAndSwap(nil, t) {
		return errors.New("unmountd: XPC transport already running")
	}
	for i, l := range t.listeners {
		cs := C.CString(l.name)
		rc := C.jm_ud_listen(cs, C.int(i))
		C.free(unsafe.Pointer(cs))
		if rc != 0 {
			return fmt.Errorf("listen %s: libxpc returned no connection", l.name)
		}
	}
	logger.Printf("xpc: listening on %s (service) and %s (status); peer requirement channel=%s",
		client.MachService, client.StatusMachService, peerChannel)

	exe, _ := os.Executable()
	var start *exeIdentity
	if exe != "" {
		start, _ = identifyExecutable(exe)
	}
	tick := time.NewTicker(staleCheckEvery)
	defer tick.Stop()
	for {
		select {
		case i := <-listenerFailed:
			return fmt.Errorf("listener %s invalidated by libxpc (is the daemon running under launchd with this MachServices name?)", t.listeners[i].name)
		case <-tick.C:
			if start == nil || !executableReplaced(exe, start) {
				continue
			}
			// Refuse new requests and check idleness atomically with
			// dispatch; only then exit.
			if t.quiesce(svc.idle) {
				logger.Printf("xpc: executable replaced on disk (app update); exiting idle so launchd runs the new binary")
				return nil
			}
		}
	}
}

// exeIdentity is the executable's content identity. The stat fields only
// gate the (cheap, ~10 MB) re-hash; the SHA-256 decides. A `touch` or an
// update that ships an identical binary is not a replacement.
type exeIdentity struct {
	fi  os.FileInfo
	sum [sha256.Size]byte
}

func identifyExecutable(path string) (*exeIdentity, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	id := &exeIdentity{fi: fi}
	copy(id.sum[:], h.Sum(nil))
	return id, nil
}

func executableReplaced(path string, start *exeIdentity) bool {
	now, err := os.Stat(path)
	if err != nil {
		return true // the old bundle was moved away
	}
	if os.SameFile(start.fi, now) && now.ModTime().Equal(start.fi.ModTime()) && now.Size() == start.fi.Size() {
		return false
	}
	cur, err := identifyExecutable(path)
	if err != nil {
		return true
	}
	return cur.sum != start.sum
}

// RequirementValid compiles a code-signing requirement string with the
// Security framework (SecRequirementCreateWithString).
func RequirementValid(req string) error {
	cs := C.CString(req)
	defer C.free(unsafe.Pointer(cs))
	if st := C.jm_ud_requirement_valid(cs); st != 0 {
		return fmt.Errorf("unmountd: invalid code-signing requirement (OSStatus %d)", int(st))
	}
	return nil
}

// ErrRequirementNotSatisfied: the code at a path does not satisfy a requirement.
var ErrRequirementNotSatisfied = errors.New("unmountd: code does not satisfy the requirement")

// StaticSatisfies checks the signed code at path against req with the same
// Security-framework engine libxpc uses (SecStaticCodeCheckValidity). It is
// a STATIC check of a file on disk: it proves what the requirement string
// accepts and rejects, not that a running process is admitted. verify-build
// (via `juicemount-unmountd -check-peer`) and the tests use it.
func StaticSatisfies(path, req string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	cp, cr := C.CString(path), C.CString(req)
	defer C.free(unsafe.Pointer(cp))
	defer C.free(unsafe.Pointer(cr))
	isDir := C.int(0)
	if fi.IsDir() {
		isDir = 1
	}
	switch st := int(C.jm_ud_static_satisfies(cp, isDir, cr)); st {
	case 0:
		return nil
	case -67050: // errSecCSReqFailed
		return ErrRequirementNotSatisfied
	default:
		return fmt.Errorf("%w (OSStatus %d)", ErrRequirementNotSatisfied, st)
	}
}

//export jmUnmountdPeerOpen
func jmUnmountdPeerOpen(listener C.int, id C.uint64_t, peer C.uintptr_t, euid C.uint32_t, asid C.int) C.int {
	t := activeXPC.Load()
	if t == nil {
		return 0
	}
	set := func(req string) error {
		cs := C.CString(req)
		defer C.free(unsafe.Pointer(cs))
		if rc := C.jm_ud_set_requirement(peer, cs); rc != 0 {
			return fmt.Errorf("xpc_connection_set_peer_code_signing_requirement returned %d", int(rc))
		}
		return nil
	}
	if t.peerOpen(int(listener), uint64(id), uint32(euid), uint32(asid), set) {
		return 1
	}
	return 0
}

//export jmUnmountdMessage
func jmUnmountdMessage(id C.uint64_t, msg C.uintptr_t, version C.int64_t, body unsafe.Pointer, n C.size_t) {
	t := activeXPC.Load()
	if t == nil {
		C.jm_ud_drop(msg)
		return
	}
	var b []byte
	if body != nil && n > 0 {
		m := int(n)
		if m > client.MaxBody+1 {
			m = client.MaxBody + 1 // enough for the decoder to refuse it
		}
		b = C.GoBytes(body, C.int(m))
	}
	// Off the libxpc queue: a retire may wait up to client.MaxRetireWait.
	go func() {
		resp := t.handle(uint64(id), int64(version), b)
		if resp == nil {
			C.jm_ud_drop(msg)
			return
		}
		p := C.CBytes(resp)
		C.jm_ud_reply(msg, C.int64_t(client.ProtocolVersion), p, C.size_t(len(resp)))
		C.free(p)
	}()
}

//export jmUnmountdPeerRejected
func jmUnmountdPeerRejected(id C.uint64_t) {
	if t := activeXPC.Load(); t != nil {
		t.peerRejected(uint64(id))
	}
}

//export jmUnmountdPeerClosed
func jmUnmountdPeerClosed(id C.uint64_t) {
	if t := activeXPC.Load(); t != nil {
		t.peerClosed(uint64(id))
	}
}

//export jmUnmountdListenerFailed
func jmUnmountdListenerFailed(listener C.int) {
	select {
	case listenerFailed <- int(listener):
	default:
	}
}
