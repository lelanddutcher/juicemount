package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func TestProbeEmitsSingleCompleteJSONObservation(t *testing.T) {
	snapshot := mounttable.Snapshot{Version: 1, BootID: "boot-session", Entries: []mounttable.MountEntry{{
		Path: "/Volumes/JuiceMount", Source: "127.0.0.1:/jm-0123456789abcdef0123456789abcdef",
		Type: "nfs", FSID: [2]int32{12, -34},
	}}}
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr, func() (mounttable.Snapshot, error) { return snapshot, nil }); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, stderr.String())
	}
	const want = `{"version":1,"boot_id":"boot-session","entries":[{"path":"/Volumes/JuiceMount","source":"127.0.0.1:/jm-0123456789abcdef0123456789abcdef","type":"nfs","fsid":[12,-34]}]}` + "\n"
	if stdout.String() != want || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestProbeRefusesIncompleteAndOversizedObservations(t *testing.T) {
	for name, capture := range map[string]func() (mounttable.Snapshot, error){
		"error": func() (mounttable.Snapshot, error) { return mounttable.Snapshot{}, errors.New("failed") },
		"missing-boot": func() (mounttable.Snapshot, error) {
			return mounttable.Snapshot{Version: 1, Entries: []mounttable.MountEntry{}}, nil
		},
		"nil-entries": func() (mounttable.Snapshot, error) {
			return mounttable.Snapshot{Version: 1, BootID: "boot"}, nil
		},
		"incomplete-entry": func() (mounttable.Snapshot, error) {
			return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{{Path: "/p"}}}, nil
		},
		"oversized": func() (mounttable.Snapshot, error) {
			return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{{
				Path: "/" + strings.Repeat("x", maxHelperJSONBytes), Source: "source", Type: "nfs",
			}}}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(nil, &stdout, &stderr, capture); code == 0 || stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("exit=%d stdout=%d stderr=%q", code, stdout.Len(), stderr.String())
			}
		})
	}
}

func TestProbeRejectsArgumentsBeforeCapture(t *testing.T) {
	called := false
	var stdout, stderr bytes.Buffer
	code := run([]string{"--unsafe"}, &stdout, &stderr, func() (mounttable.Snapshot, error) {
		called = true
		return mounttable.Snapshot{}, nil
	})
	if code != 2 || called || stdout.Len() != 0 || stderr.Len() == 0 {
		t.Fatalf("exit=%d called=%t stdout=%q stderr=%q", code, called, stdout.String(), stderr.String())
	}
}
