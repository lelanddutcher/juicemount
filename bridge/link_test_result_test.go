package main

import (
	"encoding/json"
	"testing"
)

func TestLinkTestResultAlwaysCarriesRequiredFields(t *testing.T) {
	raw, err := json.Marshal(linkTestResult{Addresses: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"ok", "authorized", "online", "backend_reachable", "addresses"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("required Link test field %q omitted from %s", key, raw)
		}
	}
}
