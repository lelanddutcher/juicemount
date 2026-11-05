package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// smart_folder_roots: absent or [] means none, and an invalid name is left
// out (reported), never fatal.
func TestSmartFolderRootsConfig(t *testing.T) {
	for _, tc := range []struct {
		json, want string
		bad        int
	}{
		{`{}`, "", 0},
		{`{"smart_folder_roots":[]}`, "", 0},
		{`{"smart_folder_roots":["Recents","Kollektionen","Recents"]}`, "Recents,Kollektionen", 0},
		{`{"smart_folder_roots":["Recents",".trash","a/b"]}`, "Recents", 2},
	} {
		var fc fileConfig
		dec := json.NewDecoder(strings.NewReader(tc.json))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&fc); err != nil {
			t.Fatalf("%s: %v", tc.json, err)
		}
		got, bad := fc.smartRoots()
		if strings.Join(got, ",") != tc.want || len(bad) != tc.bad {
			t.Fatalf("%s: roots %q, %d bad (%v); want %q, %d bad", tc.json, got, len(bad), bad, tc.want, tc.bad)
		}
	}
}
