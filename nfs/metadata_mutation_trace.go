package nfs

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

const (
	metadataTracePathLimit           = 512
	metadataTraceIdentityLimit       = 128
	metadataTraceOperationLimit      = 4096
	metadataTraceRecordLimit         = 16 * 1024 // JSON payload, plus the fixed jmlog envelope.
	metadataTraceRecordsPerOperation = 7
)

// Diagnostic only. At most 7*4096 operation records and one incomplete record,
// each with at most 16KiB of JSON payload. Log rotation can lose records: a
// terminal, missing event, or exhausted trace never proves complete coverage.
// Count/output bounds are NOT latency bounds. Observations and the synchronous
// logger retain the caller's existing namespace/gate or completion-FD ownership.
// All seams are immutable after construction; no package-global test hooks.
type metadataMutationTrace struct {
	prefix     string
	generation string
	origin     time.Time
	next       atomic.Uint64
	incomplete atomic.Bool
	lstat      func(string, *unix.Stat_t) error
	fstat      func(int, *unix.Stat_t) error
	emit       func(metadataMutationRecord)
}

type metadataMutationStat struct {
	Dev     uint64 `json:"dev"`
	Ino     uint64 `json:"ino"`
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
	CtimeNS int64  `json:"ctime_ns"`
	Mode    uint32 `json:"mode"`
}

type metadataMutationError struct {
	Kind  string `json:"kind"`
	Errno *int   `json:"errno,omitempty"`
}

type metadataMutationObservation struct {
	OK    bool                   `json:"ok"`
	Stat  *metadataMutationStat  `json:"stat,omitempty"`
	Error *metadataMutationError `json:"error,omitempty"`
}

type metadataMutationRecord struct {
	Generation             string                       `json:"generation"`
	EventID                uint64                       `json:"event_id"`
	ClockDomain            string                       `json:"clock_domain"`
	Operation              string                       `json:"operation"`
	Target                 string                       `json:"target"`
	Sibling                string                       `json:"associated_sibling,omitempty"`
	Stage                  string                       `json:"stage"`
	OperationStartNS       int64                        `json:"operation_start_ns"`
	StartNS                int64                        `json:"start_ns"`
	EndNS                  int64                        `json:"end_ns"`
	RequestedMode          uint32                       `json:"requested_mode"`
	SpoolChecked           bool                         `json:"spool_checked"`
	SpoolPending           bool                         `json:"spool_pending"`
	SpoolID                int64                        `json:"spool_id,omitempty"`
	ReceiptSpoolID         int64                        `json:"receipt_spool_id,omitempty"`
	CompletionID           string                       `json:"completion_id,omitempty"`
	PublishedIncarnation   string                       `json:"published_incarnation,omitempty"`
	PublishedRevision      int64                        `json:"published_revision,omitempty"`
	ItemBackingIdentity    string                       `json:"item_backing_identity,omitempty"`
	ReceiptBackingIdentity string                       `json:"receipt_backing_identity,omitempty"`
	Observation            *metadataMutationObservation `json:"observation,omitempty"`
	SyscallOK              *bool                        `json:"syscall_ok,omitempty"`
	SyscallError           *metadataMutationError       `json:"syscall_error,omitempty"`
	TargetIdentity         string                       `json:"target_identity,omitempty"`
	SiblingIdentity        string                       `json:"sibling_identity,omitempty"`
	ObserverComplete       bool                         `json:"observer_complete"`
	WorkerJoined           bool                         `json:"worker_joined"`
	Incomplete             bool                         `json:"incomplete"`
	Reason                 string                       `json:"reason,omitempty"`
	Interpretation         string                       `json:"interpretation"`
}

func metadataTraceCanonical(name string) bool {
	return name != "" && name != "." && len(name) <= metadataTracePathLimit &&
		!path.IsAbs(name) && path.Clean(name) == name && utf8.ValidString(name) &&
		!strings.ContainsRune(name, 0) && name != ".." && !strings.HasPrefix(name, "../")
}

// The environment is read only by NewHandler, never by an operation.
func newMetadataMutationTrace(prefix, isolation string) *metadataMutationTrace {
	if isolation != "1" || !metadataTraceCanonical(prefix) {
		return nil
	}
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		jmlog.Warn("nfs: metadata mutation trace unavailable", "reason", "generation_entropy")
		return nil
	}
	return &metadataMutationTrace{
		prefix: prefix, generation: hex.EncodeToString(generation[:]), origin: time.Now(),
		lstat: unix.Lstat, fstat: unix.Fstat,
		emit: func(record metadataMutationRecord) {
			jmlog.Info("nfs: metadata mutation trace", "trace", record)
		},
	}
}

func (t *metadataMutationTrace) matches(name string) bool {
	return name == t.prefix || strings.HasPrefix(name, t.prefix+"/")
}

func (t *metadataMutationTrace) now() int64 { return time.Since(t.origin).Nanoseconds() }

func (t *metadataMutationTrace) signalIncomplete(reason string) {
	if t.incomplete.CompareAndSwap(false, true) {
		n := t.now()
		t.emit(metadataMutationRecord{Generation: t.generation, ClockDomain: "handler-generation-monotonic-ns",
			Stage: "incomplete", StartNS: n, EndNS: n, Incomplete: true, Reason: reason,
			Interpretation: "interval_association_only; overlaps_ambiguous; absent_events_exclude_no_actor"})
	}
}

func (t *metadataMutationTrace) write(record metadataMutationRecord) {
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > metadataTraceRecordLimit {
		t.signalIncomplete("record_bound")
		return
	}
	t.emit(record)
}

type metadataMutationInterval struct {
	owner                       *metadataMutationTrace
	record                      metadataMutationRecord
	beforeTarget, beforeSibling metadataMutationObservation
	syscallStart                int64
}

func (t *metadataMutationTrace) begin(record metadataMutationRecord) *metadataMutationInterval {
	if !t.matches(record.Target) {
		return nil
	}
	if !metadataTraceCanonical(record.Target) ||
		len(record.CompletionID) > metadataTraceIdentityLimit ||
		len(record.PublishedIncarnation) > metadataTraceIdentityLimit ||
		len(record.ItemBackingIdentity) > metadataTraceIdentityLimit ||
		len(record.ReceiptBackingIdentity) > metadataTraceIdentityLimit {
		t.signalIncomplete("unrepresentable_target_or_receipt")
		return nil
	}
	var id uint64
	for {
		old := t.next.Load()
		if old >= metadataTraceOperationLimit {
			t.signalIncomplete("operation_cap")
			return nil
		}
		if t.next.CompareAndSwap(old, old+1) {
			id = old + 1
			break
		}
	}
	if !strings.HasPrefix(path.Base(record.Target), "._") {
		record.Sibling = path.Join(path.Dir(record.Target), "._"+path.Base(record.Target))
		if len(record.Sibling) > metadataTracePathLimit {
			t.signalIncomplete("unrepresentable_sibling")
			return nil
		}
	}
	record.Generation = t.generation
	record.EventID = id
	record.ClockDomain = "handler-generation-monotonic-ns"
	record.Interpretation = "interval_association_only; overlaps_ambiguous; absent_events_exclude_no_actor"
	record.OperationStartNS = t.now()
	record.StartNS, record.EndNS = record.OperationStartNS, record.OperationStartNS
	record.Stage = "start"
	t.write(record) // Before any potentially blocking stat or logger following this start.
	return &metadataMutationInterval{owner: t, record: record}
}

func (t *metadataMutationTrace) beginChmod(name string, mode os.FileMode, checked, pending bool) *metadataMutationInterval {
	return t.begin(metadataMutationRecord{Operation: "namespace_chmod", Target: name,
		RequestedMode: uint32(mode.Perm()), SpoolChecked: checked, SpoolPending: pending})
}

func (t *metadataMutationTrace) beginCompletion(item metadata.DrainCommitItem, receipt metadata.DrainCommitResult) *metadataMutationInterval {
	return t.begin(metadataMutationRecord{Operation: "completion_fd_chmod", Target: item.NFSPath,
		RequestedMode: uint32(receipt.Mode.Perm()), SpoolID: item.SpoolID, ReceiptSpoolID: receipt.SpoolID,
		CompletionID: receipt.CompletionID, PublishedIncarnation: receipt.Published.Incarnation,
		PublishedRevision: receipt.Published.Revision, ItemBackingIdentity: item.BackingIdentity,
		ReceiptBackingIdentity: receipt.BackingIdentity})
}

func metadataTraceError(err error) *metadataMutationError {
	if err == nil {
		return nil
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		n := int(errno)
		return &metadataMutationError{Kind: "errno", Errno: &n}
	}
	// No raw error text, which may include unbounded or private path content.
	return &metadataMutationError{Kind: "other"}
}

func (i *metadataMutationInterval) observe(stage, root string, fd *os.File, sibling bool) metadataMutationObservation {
	r := i.record
	r.Stage, r.StartNS = stage, i.owner.now()
	var st unix.Stat_t
	var err error
	if sibling && !i.owner.matches(r.Sibling) {
		observation := metadataMutationObservation{Error: &metadataMutationError{Kind: "outside_trace_prefix"}}
		r.Observation, r.EndNS = &observation, i.owner.now()
		i.owner.write(r)
		return observation
	}
	if sibling {
		err = i.owner.lstat(path.Join(root, r.Sibling), &st)
	} else if fd != nil {
		err = i.owner.fstat(int(fd.Fd()), &st)
	} else {
		err = i.owner.lstat(path.Join(root, r.Target), &st)
	}
	observation := metadataMutationObservation{OK: err == nil, Error: metadataTraceError(err)}
	if err == nil {
		observation.Stat = &metadataMutationStat{Dev: uint64(st.Dev), Ino: uint64(st.Ino), Size: st.Size,
			MtimeNS: st.Mtim.Nano(), CtimeNS: st.Ctim.Nano(), Mode: uint32(st.Mode)}
	}
	r.Observation, r.EndNS = &observation, i.owner.now()
	i.owner.write(r)
	return observation
}

func (i *metadataMutationInterval) before(root string, fd *os.File) {
	i.beforeTarget = i.observe("before_target", root, fd, false)
	if i.record.Sibling != "" {
		i.beforeSibling = i.observe("before_sibling", root, fd, true)
	}
	i.syscallStart = i.owner.now()
}

func metadataTraceIdentity(before, after metadataMutationObservation) string {
	if !before.OK || !after.OK || before.Stat == nil || after.Stat == nil {
		return "unavailable_inconclusive"
	}
	if before.Stat.Mode&unix.S_IFMT == unix.S_IFLNK || after.Stat.Mode&unix.S_IFMT == unix.S_IFLNK {
		return "symlink_inconclusive"
	}
	if before.Stat.Dev != after.Stat.Dev || before.Stat.Ino != after.Stat.Ino {
		return "changed_inconclusive"
	}
	return "same_dev_ino_interval_only"
}

func (i *metadataMutationInterval) after(root string, fd *os.File, syscallErr error) {
	r := i.record
	ok := syscallErr == nil
	r.Stage, r.StartNS, r.EndNS = "syscall", i.syscallStart, i.owner.now()
	r.SyscallOK, r.SyscallError = &ok, metadataTraceError(syscallErr)
	i.owner.write(r) // Preserve raw syscall result before another observation can block.
	afterTarget := i.observe("after_target", root, fd, false)
	targetIdentity := metadataTraceIdentity(i.beforeTarget, afterTarget)
	siblingIdentity := "not_applicable"
	if i.record.Sibling != "" {
		afterSibling := i.observe("after_sibling", root, fd, true)
		siblingIdentity = metadataTraceIdentity(i.beforeSibling, afterSibling)
	}
	r = i.record
	r.Stage, r.StartNS, r.EndNS = "terminal", i.record.OperationStartNS, i.owner.now()
	r.TargetIdentity, r.SiblingIdentity = targetIdentity, siblingIdentity
	r.ObserverComplete = true // Explicitly NOT namespace worker join, FD close, or completion success.
	r.Incomplete = targetIdentity != "same_dev_ino_interval_only" ||
		(siblingIdentity != "not_applicable" && siblingIdentity != "same_dev_ino_interval_only")
	i.owner.write(r)
}
