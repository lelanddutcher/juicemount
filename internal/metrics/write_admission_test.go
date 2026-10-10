package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestAdmissionRefusalFixedMetricsJSON(t *testing.T) {
	r := NewRegistry()
	reasons := []AdmissionReason{
		AdmissionContinuationStopped, AdmissionContinuationEntryExpired,
		AdmissionContinuationWaiterCap, AdmissionContinuationQueuedExpired,
		AdmissionNamespaceStopping, AdmissionNamespaceActiveCap,
		AdmissionNamespaceOverlap, AdmissionNamespaceServingBlocked,
		AdmissionNamespacePreDeadline, AdmissionNamespacePostDeadline,
		AdmissionNamespaceWorkerDeadline,
	}
	for _, reason := range reasons {
		r.IncAdmissionRefusal(reason)
	}
	r.IncAdmissionRefusal(admissionReasonCount) // invalid input must not alias a real bucket
	got := r.Snapshot().AdmissionRefusals
	fields := []uint64{got.ContinuationStopped, got.ContinuationEntryExpired,
		got.ContinuationWaiterCap, got.ContinuationQueuedExpired,
		got.NamespaceStopping, got.NamespaceActiveCap, got.NamespaceOverlap,
		got.NamespaceServingBlocked, got.NamespacePreDeadline, got.NamespacePostDeadline,
		got.NamespaceWorkerDeadline}
	for i, count := range fields {
		if count != 1 {
			t.Fatalf("reason %d count=%d, want 1", i, count)
		}
	}
	recorder := httptest.NewRecorder()
	NewServer("127.0.0.1:0", r).handleMetrics(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if recorder.Code != 200 {
		t.Fatalf("metrics status=%d", recorder.Code)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	var published map[string]uint64
	if err := json.Unmarshal(raw["admission_refusals"], &published); err != nil {
		t.Fatal(err)
	}
	want := []string{"continuation_stopped", "continuation_entry_expired", "continuation_waiter_cap",
		"continuation_queued_expired", "namespace_stopping", "namespace_active_cap",
		"namespace_overlap", "namespace_serving_blocked", "namespace_pre_deadline",
		"namespace_post_deadline", "namespace_worker_deadline"}
	if len(published) != len(want) {
		t.Fatalf("keys=%d, want %d", len(published), len(want))
	}
	for _, key := range want {
		if published[key] != 1 {
			t.Fatalf("%s=%d, want 1", key, published[key])
		}
	}
}
