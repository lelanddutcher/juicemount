package smartfolders_test

import (
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/metadata"
)

// TestEventMatchesPublishedPayload pins the tracker's Event to the payload
// the Mac clients publish (metadata.MetadataEvent, via PublishEvent). The
// production package does not import metadata; this test is the contract.
func TestEventMatchesPublishedPayload(t *testing.T) {
	cases := []struct {
		in   metadata.MetadataEvent
		want smartfolders.Event
	}{
		{metadata.MetadataEvent{Op: "create", Path: "A/clip.mov", Size: 10, Mtime: 5, Inode: 9},
			smartfolders.Event{Op: "create", Path: "A/clip.mov"}},
		{metadata.MetadataEvent{Op: "create", Path: "A/dir", IsDir: true},
			smartfolders.Event{Op: "create", Path: "A/dir", IsDir: true}},
		{metadata.MetadataEvent{Op: "create", Path: "A/l", IsSymlink: true},
			smartfolders.Event{Op: "create", Path: "A/l", IsSymlink: true}},
		{metadata.MetadataEvent{Op: "rename", Path: "B/n.mov", OldPath: "A/o.mov", IsDir: true},
			smartfolders.Event{Op: "rename", Path: "B/n.mov", OldPath: "A/o.mov", IsDir: true}},
		{metadata.MetadataEvent{Op: "delete", Path: "A/x.mov", Inode: 3},
			smartfolders.Event{Op: "delete", Path: "A/x.mov"}},
		{metadata.MetadataEvent{Op: "reconcile", Path: "A/y.mov"},
			smartfolders.Event{Op: "reconcile", Path: "A/y.mov"}},
	}
	for _, c := range cases {
		payload, err := json.Marshal(c.in)
		if err != nil {
			t.Fatal(err)
		}
		got, err := smartfolders.ParseEvent(payload)
		if err != nil || got != c.want {
			t.Errorf("%s: ParseEvent = %+v, %v; want %+v", payload, got, err, c.want)
		}
	}
	if metadata.SubscribeChannel != "juicemount:metadata" {
		t.Fatalf("channel renamed to %q", metadata.SubscribeChannel)
	}
}
