package nfs

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

// RPCError provides the error interface for errors thrown by
// procedures to be transmitted over the XDR RPC channel
type RPCError interface {
	// An RPCError is an `error` with this method
	Error() string
	// Code is the RPC Response code to send
	Code() ResponseCode
	// BinaryMarshaler is the on-wire representation of this error
	encoding.BinaryMarshaler
}

// AuthStat is an enumeration of why authentication ahs failed
type AuthStat uint32

// AuthStat Codes
const (
	AuthStatOK AuthStat = iota
	AuthStatBadCred
	AuthStatRejectedCred
	AuthStatBadVerifier
	AuthStatRejectedVerfier
	AuthStatTooWeak
	AuthStatInvalidResponse
	AuthStatFailed
	AuthStatKerbGeneric
	AuthStatTimeExpire
	AuthStatTktFile
	AuthStatDecode
	AuthStatNetAddr
	AuthStatRPCGSSCredProblem
	AuthStatRPCGSSCTXProblem
)

// AuthError is an RPCError
type AuthError struct {
	AuthStat
}

// Code for AuthErrors is ResponseCodeAuthError
func (a *AuthError) Code() ResponseCode {
	return ResponseCodeAuthError
}

// Error is a textual representaiton of the auth error. From the RFC
func (a *AuthError) Error() string {
	switch a.AuthStat {
	case AuthStatOK:
		return "Auth Status: OK"
	case AuthStatBadCred:
		return "Auth Status: bad credential"
	case AuthStatRejectedCred:
		return "Auth Status: client must begin new session"
	case AuthStatBadVerifier:
		return "Auth Status: bad verifier"
	case AuthStatRejectedVerfier:
		return "Auth Status: verifier expired or replayed"
	case AuthStatTooWeak:
		return "Auth Status: rejected for security reasons"
	case AuthStatInvalidResponse:
		return "Auth Status: bogus response verifier"
	case AuthStatFailed:
		return "Auth Status: reason unknown"
	case AuthStatKerbGeneric:
		return "Auth Status: kerberos generic error"
	case AuthStatTimeExpire:
		return "Auth Status: time of credential expired"
	case AuthStatTktFile:
		return "Auth Status: problem with ticket file"
	case AuthStatDecode:
		return "Auth Status: can't decode authenticator"
	case AuthStatNetAddr:
		return "Auth Status: wrong net address in ticket"
	case AuthStatRPCGSSCredProblem:
		return "Auth Status: no credentials for user"
	case AuthStatRPCGSSCTXProblem:
		return "Auth Status: problem with context"
	}
	return "Auth Status: Unknown"
}

// MarshalBinary sends the specific auth status
func (a *AuthError) MarshalBinary() (data []byte, err error) {
	var resp [4]byte
	binary.LittleEndian.PutUint32(resp[:], uint32(a.AuthStat))
	return resp[:], nil
}

// RPCMismatchError is an RPCError
type RPCMismatchError struct {
	Low  uint32
	High uint32
}

// Code for RPCMismatchError is ResponseCodeRPCMismatch
func (r *RPCMismatchError) Code() ResponseCode {
	return ResponseCodeRPCMismatch
}

func (r *RPCMismatchError) Error() string {
	return fmt.Sprintf("RPC Mismatch: Expected version between %d and %d.", r.Low, r.High)
}

// MarshalBinary sends the specific rpc mismatch range
func (r *RPCMismatchError) MarshalBinary() (data []byte, err error) {
	var resp [8]byte
	binary.LittleEndian.PutUint32(resp[0:4], uint32(r.Low))
	binary.LittleEndian.PutUint32(resp[4:8], uint32(r.High))
	return resp[:], nil
}

// ResponseCodeProcUnavailableError is an RPCError
type ResponseCodeProcUnavailableError struct {
}

// Code for ResponseCodeProcUnavailableError
func (r *ResponseCodeProcUnavailableError) Code() ResponseCode {
	return ResponseCodeProcUnavailable
}

func (r *ResponseCodeProcUnavailableError) Error() string {
	return "The requested procedure is unexported"
}

// MarshalBinary - this error has no associated body
func (r *ResponseCodeProcUnavailableError) MarshalBinary() (data []byte, err error) {
	return []byte{}, nil
}

// ResponseCodeSystemError is an RPCError
type ResponseCodeSystemError struct {
}

// Code for ResponseCodeSystemError
func (r *ResponseCodeSystemError) Code() ResponseCode {
	return ResponseCodeSystemErr
}

func (r *ResponseCodeSystemError) Error() string {
	return "memory allocation failure"
}

// MarshalBinary - this error has no associated body
func (r *ResponseCodeSystemError) MarshalBinary() (data []byte, err error) {
	return []byte{}, nil
}

// basicErrorFormatter is the default error handler for response errors.
// if the error is already formatted, it is directly written. Otherwise,
// ResponseCodeSystemError is sent to the client.
func basicErrorFormatter(err error) RPCError {
	var rpcErr RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr
	}
	return &ResponseCodeSystemError{}
}

// ErrFUSETimeout is returned by the handler's filesystem layer (nfs package)
// when a JuiceFS FUSE syscall (Stat/Lstat/ReadDir/OpenFile) does not complete
// within its deadline because the mount is wedged. The RPC dispatch
// (conn.handle) maps any error that wraps this to NFS3ERR_JUKEBOX so the
// client retries ("server busy") instead of aborting on a permanent error.
// Critically, the handler returns immediately and frees its rpcSem slot rather
// than parking it on the wedged mount — without that, a JuiceFS wedge consumes
// all 128 in-flight slots, the server stops reading requests, and the whole
// NFS mount goes stale (Finder "error 100060"). Lives in this lower package so
// both the handlers (internal/nfs) and the filesystem layer (nfs, imported as
// nfslib) reference the same value.
var ErrFUSETimeout = errors.New("nfs: fuse operation timed out (mount wedged)")

// ErrBackendBlip marks a data-plane op that failed with a transport-class
// error while the metadata backend was inside a blip window (#9 / task #36).
// conn.handle maps it — like ErrFUSETimeout — to the retryable
// NFS3ERR_JUKEBOX so the client retries across the blip instead of aborting.
var ErrBackendBlip = errors.New("nfs: backend blip — parked for client retry")

// NFSStatusError represents an error at the NFS level.
type NFSStatusError struct {
	NFSStatus
	WrappedErr error
}

// Error is The wrapped error
func (s *NFSStatusError) Error() string {
	message := s.NFSStatus.String()
	if s.WrappedErr != nil {
		message = fmt.Sprintf("%s: %v", message, s.WrappedErr)
	}
	return message
}

// Code for NFS issues are successful RPC responses
func (s *NFSStatusError) Code() ResponseCode {
	return ResponseCodeSuccess
}

// MarshalBinary - The binary form of the code.
func (s *NFSStatusError) MarshalBinary() (data []byte, err error) {
	var resp [4]byte
	binary.BigEndian.PutUint32(resp[0:4], uint32(s.NFSStatus))
	return resp[:], nil
}

// Unwrap unpacks wrapped errors
func (s *NFSStatusError) Unwrap() error {
	return s.WrappedErr
}

// StatusErrorWithBody is an NFS error with a payload.
type StatusErrorWithBody struct {
	NFSStatusError
	Body []byte
}

// MarshalBinary provides the wire format of the error response
func (s *StatusErrorWithBody) MarshalBinary() (data []byte, err error) {
	head, err := s.NFSStatusError.MarshalBinary()
	return append(head, s.Body...), err
}

// errFormatterWithBody appends a provided body to errors
func errFormatterWithBody(body []byte) func(err error) RPCError {
	return func(err error) RPCError {
		if nerr, ok := err.(*NFSStatusError); ok {
			return &StatusErrorWithBody{*nerr, body[:]}
		}
		var rErr RPCError
		if errors.As(err, &rErr) {
			return rErr
		}
		return &ResponseCodeSystemError{}
	}
}

var (
	opAttrErrorBody       = [4]byte{}
	opAttrErrorFormatter  = errFormatterWithBody(opAttrErrorBody[:])
	wccDataErrorBody      = [8]byte{}
	wccDataErrorFormatter = errFormatterWithBody(wccDataErrorBody[:])
)

// nfsStatusErrorFrom guarantees an error is reply-encodable at the NFS
// level. A raw Go error that reaches errFormatterWithBody falls through to
// ResponseCodeSystemError — an RPC-level SYSTEM_ERR reply with NO NFS status
// or wcc/attr body — which the macOS client surfaces to applications as
// EBADRPC "RPC struct is bad" (errno 72). RFC 1813 requires every SETATTR /
// WRITE / CREATE reply, success OR failure, to carry its post-op body, so
// every error leaving a handler must be an RPCError. Existing RPCErrors
// (including *NFSStatusError) pass through untouched.
func nfsStatusErrorFrom(err error) error {
	// Unwrap to the CONCRETE *NFSStatusError rather than passing a wrapper
	// through: errFormatterWithBody appends the mandatory post-op body via an
	// exact type assertion `err.(*NFSStatusError)`, so an NFSStatusError
	// wrapped in fmt.Errorf("%w: …") returned verbatim would encode a
	// body-less reply — the EBADRPC shape this function exists to prevent.
	var statusErr *NFSStatusError
	if errors.As(err, &statusErr) {
		return statusErr
	}
	var rpcErr RPCError
	if errors.As(err, &rpcErr) {
		return err
	}
	if os.IsNotExist(err) {
		return &NFSStatusError{NFSStatusNoEnt, err}
	}
	if errors.Is(err, os.ErrPermission) {
		return &NFSStatusError{NFSStatusAccess, err}
	}
	if errors.Is(err, syscall.ENOSPC) {
		// "Disk full" must reach the client as NFS3ERR_NOSPC, not a
		// generic I/O error — it's the difference between Finder saying
		// "the disk is full" (actionable) and "an I/O error occurred"
		// (panic-inducing). The write spool's capacity sentinel
		// (nfs.ErrSpoolFull) wraps syscall.ENOSPC precisely so this
		// match fires without this package importing nfs/ (cycle).
		return &NFSStatusError{NFSStatusNoSPC, err}
	}
	if errors.Is(err, syscall.EROFS) {
		// See writeRefusalStatus: a write refused by a read-only backend is
		// NFS3ERR_ROFS, never the generic NFS3ERR_IO.
		//
		// Callers that reach this branch: SETATTR (through
		// SetFileAttributes.Apply), WRITE (OpenFile and WriteAt failures),
		// and READ (readTransientStatusError in nfs_onread.go). RFC 1813 does
		// not list ROFS among the READ3 errors. READ should never get here:
		// read-only JuiceFS refuses only operations that change data or
		// metadata (meta txn and the base.go write ops), never a read. If a
		// read ever does carry EROFS, ROFS is still a more truthful reply
		// than IO, so the branch is not special-cased for READ.
		return &NFSStatusError{NFSStatusROFS, err}
	}
	return &NFSStatusError{NFSStatusIO, err}
}

// writeRefusalStatus maps an error from a namespace or data mutation to the
// status a write-path handler (CREATE, MKDIR, SYMLINK, MKNOD, LINK, REMOVE,
// RMDIR, RENAME, WRITE, SETATTR) returns. An error that carries
// syscall.EROFS, at any wrapping depth and including one already wrapped in
// an *NFSStatusError with another status, becomes NFS3ERR_ROFS (30). Every
// other error keeps the handler's existing fallback status, so this changes
// no mapping except the read-only one.
//
// Why: JuiceFS returns EROFS from every metadata transaction when the client
// runs read-only (third_party/juicefs/pkg/meta/redis.go txn, base.go). Before
// this mapping the handlers turned it into NFS3ERR_IO or NFS3ERR_ACCES, and
// Finder reported "an I/O error occurred" (-36) or a permission error
// (-5000), both on the release bar's forbidden list.
//
// COMMIT deliberately does not use this: RFC 1813 lists only IO, STALE,
// BADHANDLE and SERVERFAULT as COMMIT3 errors, and JuiceMount's COMMIT is a
// local spool fsync that never reaches JuiceFS.
//
// What the macOS client does with NFS3ERR_ROFS (evidence level noted; none of
// this was tested against a live mount):
//   - NFSv3 status values equal the BSD errno values, and the xnu NFS client
//     returns the reply status to the system call as that errno. So ROFS
//     reaches the application as EROFS (30), and IO reaches it as EIO (5).
//     (From the xnu NFS client design; not measured here.)
//   - Finder: EIO is the -36 "I/O error" dialog. The dialog text and OSErr
//     code Finder shows for EROFS are UNVERIFIED; no test, comment or memory
//     note in this tree records one. EROFS is not one of the codes the QA
//     battery tracks (-36, -48, -5000, 100060, 100070), but it is still a
//     failure dialog, not a silent success.
//   - Volume-wide read-only flip: no evidence either way in this tree. xnu
//     takes the volume read-only flag (MNT_RDONLY) from the mount arguments,
//     not from per-operation errors, so a single ROFS reply is not expected to
//     mark the whole volume read-only for the session. UNVERIFIED in Finder.
//   - Buffered writes: macOS sends UNSTABLE WRITE RPCs after write(2) has
//     already returned, so a WRITE refusal surfaces late, at close(2) or
//     fsync(2). In JuiceMount the gap is larger: with the write spool enabled,
//     CREATE and WRITE land in the local spool and never call JuiceFS inside
//     the RPC, so a read-only refusal there happens at drain time and no NFS
//     reply carries it. The status mapped here applies to the operations that
//     do reach JuiceFS inside the RPC: when online, MKDIR, SYMLINK, REMOVE,
//     and RENAME of an already-drained entry (nfs/handler.go juiceFS).
func writeRefusalStatus(err error, fallback NFSStatus) *NFSStatusError {
	if errors.Is(err, syscall.EROFS) {
		return &NFSStatusError{NFSStatusROFS, err}
	}
	return &NFSStatusError{fallback, err}
}
