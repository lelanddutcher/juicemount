package farmqueue

import (
	"encoding/json"
	"testing"
)

// A legacy directory parent that crashes after publishing its shards but before
// MarkDispatched is replayed, re-walks the directory and publishes only the
// shards whose status hash is missing. Index-named shards let a file added
// between the walks shift later targets into IDs that already existed, so the
// new file got no execution child.
func TestShardReplayQueuesShiftedTargets(t *testing.T) {
	q, ctx := claimScanTestQueue(t)
	parent := NewJob("/jfs/dir", []string{KindDerivatives}, "manager")
	parent.QueueClass = QueueClassServer
	parent.RequiredCapabilities = []string{"cpu"}

	first := []string{"/jfs/dir/a", "/jfs/dir/b", "/jfs/dir/c", "/jfs/dir/d"}
	if _, created, err := q.EnqueueTargetShards(ctx, parent, first, 2); err != nil || created != 2 {
		t.Fatalf("first plan created=%d err=%v", created, err)
	}
	// The directory gained a2 before the replay.
	second := []string{"/jfs/dir/a", "/jfs/dir/a2", "/jfs/dir/b", "/jfs/dir/c", "/jfs/dir/d"}
	children, created, err := q.EnqueueTargetShards(ctx, parent, second, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("replay planned %d children, created %d", len(children), created)

	queued := map[string]bool{}
	raws, err := q.rdb.LRange(ctx, QueueKeyFor(&children[0]), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range raws {
		var job Job
		if err := json.Unmarshal([]byte(raw), &job); err != nil {
			t.Fatal(err)
		}
		for _, target := range job.RetryTargets {
			queued[target] = true
		}
	}
	for _, target := range second {
		if !queued[target] {
			t.Errorf("replayed plan left %s with no queued execution child (queued %v)", target, queued)
		}
	}
	for _, child := range children {
		if n, err := q.rdb.Exists(ctx, JobHashPrefix+child.ID).Result(); err != nil || n != 1 {
			t.Errorf("replayed child %s has no status record: %d %v", child.ID, n, err)
		}
	}

	// A second replay of the same walk publishes nothing new.
	if _, created, err := q.EnqueueTargetShards(ctx, parent, second, 2); err != nil || created != 0 {
		t.Fatalf("identical replay created=%d err=%v; want 0", created, err)
	}
}

// Shard names are stable for the same targets and distinct within a plan, even
// for a degenerate target list that repeats a shard.
func TestLegacyTargetShardIDsFollowTargets(t *testing.T) {
	parent := Job{ID: "p", Path: "/jfs/dir", Kinds: []string{KindDerivatives}, QueueClass: QueueClassServer,
		RequiredCapabilities: []string{"cpu"}}
	a, err := newTargetShards(parent, []string{"/x", "/y", "/z"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newTargetShards(parent, []string{"/w", "/x", "/y", "/z"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if a[0].ID == b[1].ID || a[0].ID == b[0].ID {
		t.Fatalf("different target sets share an ID: %s %s %s", a[0].ID, b[0].ID, b[1].ID)
	}
	again, _ := newTargetShards(parent, []string{"/x", "/y", "/z"}, 2)
	if again[0].ID != a[0].ID || again[1].ID != a[1].ID {
		t.Fatalf("same targets renamed: %s/%s vs %s/%s", a[0].ID, a[1].ID, again[0].ID, again[1].ID)
	}
	dup, err := newTargetShards(parent, []string{"/x", "/x", "/x", "/x"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if dup[0].ID == dup[1].ID {
		t.Fatalf("repeated shard reused ID %s", dup[0].ID)
	}
}
