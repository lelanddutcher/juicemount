package nfs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestReplacementHistoryStrictReply(t *testing.T) {
	valid := []any{"FOREIGN", int64(4), int64(5), int64(3)}
	if epoch, err := decodeReplacementHistory(valid, 4, 1, 5); err != nil || epoch != 3 {
		t.Fatalf("foreign: %d %v", epoch, err)
	}
	if epoch, err := decodeReplacementHistory([]any{"CLEAN", int64(4), int64(5), int64(3)}, 4, 5, 5); err != nil || epoch != 3 {
		t.Fatalf("historical clean: %d %v", epoch, err)
	}
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"null", nil}, {"short", []any{"CLEAN"}}, {"wrong-project", []any{"FOREIGN", int64(9), int64(5), int64(3)}},
		{"wrong-epoch", []any{"FOREIGN", int64(4), int64(6), int64(3)}}, {"negative", []any{"FOREIGN", int64(4), int64(5), int64(-1)}},
		{"future", []any{"FOREIGN", int64(4), int64(5), int64(6)}}, {"false-clean", []any{"CLEAN", int64(4), int64(5), int64(3)}},
		{"false-foreign", []any{"FOREIGN", int64(4), int64(5), int64(0)}}, {"unknown", []any{"OK", int64(4), int64(5), int64(3)}},
		{"string-number", []any{"FOREIGN", "4", int64(5), int64(3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeReplacementHistory(tc.value, 4, 1, 5); !errors.Is(err, metadata.ErrMirrorConflict) {
				t.Fatalf("malformed reply accepted: %v", err)
			}
		})
	}
}

func TestReplacementHistoryUnsupportedDoesNotDisableOrdinaryLeases(t *testing.T) {
	f := newFakeLeaseServer(t)
	c := newTestLeaseClient(t, f, nil, time.Now)
	w := metadata.SpoolReplacementWitness{AcceptedEpoch: 0, Destination: metadata.CachePathObservation{Path: "Film/ProjA/clip.mov", Chain: []metadata.CachePathNode{{Path: "Film/ProjA", Inode: 4}}}}
	if _, err := c.replacementHistory(context.Background(), w, 1); err == nil {
		t.Fatal("old server preflight accepted")
	}
	if c.isUnsupported() {
		t.Fatal("replacement-only capability failure disabled ordinary leases")
	}
	if decision, err := c.EnsureForDrain("Film/ProjA"); err != nil || !decision.held || decision.epoch != 1 {
		t.Fatalf("ordinary lease after unsupported preflight: %+v %v", decision, err)
	}
}
