package metrics

import "sync/atomic"

// AdmissionReason is a fixed, process-wide refusal reason. Namespace counters
// include other bounded mutations as well as WRITE opens; continuation counters
// apply only to qualified same-row WRITE opens. No path or inode is recorded.
type AdmissionReason uint8

const (
	AdmissionContinuationStopped AdmissionReason = iota
	AdmissionContinuationEntryExpired
	AdmissionContinuationWaiterCap
	AdmissionContinuationQueuedExpired
	AdmissionNamespaceStopping
	AdmissionNamespaceActiveCap
	AdmissionNamespaceOverlap
	AdmissionNamespaceServingBlocked
	AdmissionNamespacePreDeadline
	AdmissionNamespacePostDeadline
	AdmissionNamespaceWorkerDeadline
	admissionReasonCount
)

// AdmissionSnapshot is emitted under /metrics as cumulative, fixed-key counts.
// Deltas between scrapes identify the refusal branch without high-cardinality
// labels or per-RPC logging. One refusal increments exactly one branch here.
type AdmissionSnapshot struct {
	ContinuationStopped       uint64 `json:"continuation_stopped"`
	ContinuationEntryExpired  uint64 `json:"continuation_entry_expired"`
	ContinuationWaiterCap     uint64 `json:"continuation_waiter_cap"`
	ContinuationQueuedExpired uint64 `json:"continuation_queued_expired"`
	NamespaceStopping         uint64 `json:"namespace_stopping"`
	NamespaceActiveCap        uint64 `json:"namespace_active_cap"`
	NamespaceOverlap          uint64 `json:"namespace_overlap"`
	NamespaceServingBlocked   uint64 `json:"namespace_serving_blocked"`
	NamespacePreDeadline      uint64 `json:"namespace_pre_deadline"`
	NamespacePostDeadline     uint64 `json:"namespace_post_deadline"`
	NamespaceWorkerDeadline   uint64 `json:"namespace_worker_deadline"`
}

type admissionCounters [admissionReasonCount]atomic.Uint64

// IncAdmissionRefusal records an already-decided refusal. It never decides
// admission or changes the returned error. Unknown values are ignored.
func (r *Registry) IncAdmissionRefusal(reason AdmissionReason) {
	if reason < admissionReasonCount {
		r.admissionRefusals[reason].Add(1)
	}
}

// AdmissionRefusals returns only the fixed counters, without invoking any
// other /metrics providers. Production scrapes still use Registry.Snapshot.
func (r *Registry) AdmissionRefusals() AdmissionSnapshot {
	return r.snapshotAdmissionRefusals()
}

func (r *Registry) snapshotAdmissionRefusals() AdmissionSnapshot {
	c := &r.admissionRefusals
	return AdmissionSnapshot{
		ContinuationStopped:       c[AdmissionContinuationStopped].Load(),
		ContinuationEntryExpired:  c[AdmissionContinuationEntryExpired].Load(),
		ContinuationWaiterCap:     c[AdmissionContinuationWaiterCap].Load(),
		ContinuationQueuedExpired: c[AdmissionContinuationQueuedExpired].Load(),
		NamespaceStopping:         c[AdmissionNamespaceStopping].Load(),
		NamespaceActiveCap:        c[AdmissionNamespaceActiveCap].Load(),
		NamespaceOverlap:          c[AdmissionNamespaceOverlap].Load(),
		NamespaceServingBlocked:   c[AdmissionNamespaceServingBlocked].Load(),
		NamespacePreDeadline:      c[AdmissionNamespacePreDeadline].Load(),
		NamespacePostDeadline:     c[AdmissionNamespacePostDeadline].Load(),
		NamespaceWorkerDeadline:   c[AdmissionNamespaceWorkerDeadline].Load(),
	}
}
