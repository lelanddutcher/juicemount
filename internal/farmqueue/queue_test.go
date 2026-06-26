package farmqueue

import "testing"

// TestJobStatusRoundTrip guards the HASH<->struct field mapping (the wire
// contract a producer reads back). No Redis needed — exercises toMap/fromMap.
func TestJobStatusRoundTrip(t *testing.T) {
	in := JobStatus{
		ID: "abc123", Status: StatusRunning, Path: "/jfs/a/b", Kinds: "proxy,transcript",
		Producer: "manager", EnqueuedAt: "2026-06-25T12:00:00Z",
		StartedAt: "2026-06-25T12:00:05Z", Processed: 7, Failed: 1, Error: "boom",
	}
	// toMap stores ints as strings (Redis HASH is all-strings); reconstitute
	// via a string map mirroring HGETALL.
	m := in.toMap()
	sm := make(map[string]string, len(m))
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("field %q is not a string in the HASH map: %T", k, v)
		}
		sm[k] = s
	}
	got := jobStatusFromMap(sm)
	if got != in {
		t.Fatalf("round-trip mismatch:\n in=%+v\nout=%+v", in, got)
	}
}

// TestJobStatusOptionalFieldsOmitted ensures empty optional fields don't get
// written (so HGETALL on a fresh queued job has no stray started_at/error).
func TestJobStatusOptionalFieldsOmitted(t *testing.T) {
	in := JobStatus{ID: "x", Status: StatusQueued, Path: "/jfs/x", Producer: "manager"}
	m := in.toMap()
	for _, k := range []string{"started_at", "finished_at", "error"} {
		if _, present := m[k]; present {
			t.Errorf("optional field %q should be omitted when empty", k)
		}
	}
	// processed/failed always present (counters default to 0).
	if m["processed"] != "0" || m["failed"] != "0" {
		t.Errorf("counters should default to 0, got processed=%v failed=%v", m["processed"], m["failed"])
	}
}

// TestNewJob stamps an id, timestamp, and carries kinds/producer.
func TestNewJob(t *testing.T) {
	j := NewJob("/jfs/clip.mov", []string{KindProxy, KindTranscript}, "openloupe")
	if j.ID == "" || j.EnqueuedAt == "" {
		t.Fatalf("NewJob must stamp id + enqueued_at, got %+v", j)
	}
	if j.Producer != "openloupe" || len(j.Kinds) != 2 {
		t.Fatalf("NewJob carried fields wrong: %+v", j)
	}
	if NewID() == NewID() {
		t.Error("NewID should not collide")
	}
}
