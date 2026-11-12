package meta

// JuiceMount patch 0026: the ops of the JMPinCache control message
// (pkg/vfs/jm_pin.go). The app (nfs/jfs_pin.go) tests that its copies equal
// these, so a renumber cannot desync the two sides.
const (
	JMPinOpStart  = 1 // ino(8) [workers(1)]
	JMPinOpStatus = 2 // ino(8)
	JMPinOpCheck  = 3 // ino(8)
	JMPinOpSweep  = 4 // phase(1) n(4) ino(8)*n
	JMPinOpCancel = 5 // ino(8)
	JMPinOpVerify = 6 // n(4) (ino(8) length(8) mtime_ns(8))*n [flags(1)]
	JMPinOpSetCap = 7 // bytes(8)

	// JMPinVerifyMax bounds the items of one VERIFY and the inodes of one
	// SWEEP message.
	JMPinVerifyMax = 4096
)
