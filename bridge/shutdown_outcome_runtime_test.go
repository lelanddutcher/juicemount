//go:build !serviceclient

package main

import (
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// The serving process must answer "shutdown-outcome" with the typed reason
// its own shutdownServer recorded, so the GUI can explain a 0 result.
func TestServingDispatchReturnsRecordedShutdownOutcome(t *testing.T) {
	old := lastShutdownOutcome()
	t.Cleanup(func() { recordShutdownOutcome(old) })
	for _, want := range []shutdownOutcome{
		{Reason: shutdownReasonNetworkVolumesNotGranted, Detail: "permission_denied: fixture"},
		{Reason: shutdownReasonFUSEAdmissionPending, Detail: "admission_pending: fixture"},
		{Complete: true, Reason: shutdownReasonComplete},
	} {
		recordShutdownOutcome(want)
		got := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "shutdown-outcome"})
		if got.Error != "" {
			t.Fatalf("shutdown-outcome refused: %s", got.Error)
		}
		var decoded shutdownOutcome
		if err := json.Unmarshal([]byte(got.Text), &decoded); err != nil {
			t.Fatalf("shutdown-outcome is not JSON %q: %v", got.Text, err)
		}
		if decoded != want {
			t.Fatalf("shutdown-outcome = %+v, want %+v", decoded, want)
		}
	}
	bad := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "shutdown-outcome", Strings: []string{"x"}})
	if bad.Error == "" {
		t.Fatal("shutdown-outcome accepted arguments it does not take")
	}
}
