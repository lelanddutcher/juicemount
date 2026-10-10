package main

import (
	"reflect"
	"testing"
)

func TestRestartConfigDrift(t *testing.T) {
	effective := map[string]any{"nice": float64(10), "ionice": float64(3)}
	requested := []string{"nice", "ionice"}
	if got := restartConfigDrift(effective, requested, 10, 3); len(got) != 0 {
		t.Fatalf("matching runtime config reported as drift: %v", got)
	}
	if got := restartConfigDrift(effective, requested, 5, 3); !reflect.DeepEqual(got, []string{"nice"}) {
		t.Fatalf("drift = %v, want [nice]", got)
	}
}
