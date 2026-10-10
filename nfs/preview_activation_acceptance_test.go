//go:build redisoptionsacceptance

package nfs

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

func p94ObservePreviewAdmission(t *testing.T, x *boundCacheFixture, want bool) {
	t.Helper()
	h := x.jfs.handler
	profile := netprofile.New()
	profile.ObserveRTT(5 * time.Millisecond)
	h.readahead.profile = profile
	jobs := make(chan previewJob, 1)
	h.readahead.previewRun = func(job previewJob) { jobs <- job }
	if !x.session.Qualified() {
		t.Fatal("selected cache session not qualified")
	}
	infos := p86ReadDirPrompt(t, x)
	if len(infos) != 1 || infos[0].Name() != "clip.mov" {
		t.Fatalf("actual ReadDir listing: %v", infos)
	}
	rm := h.readahead
	rm.mu.Lock()
	seen, queued := len(rm.previewSeen), len(rm.previewQueue)
	rm.mu.Unlock()
	if want {
		select {
		case job := <-jobs:
			if job.key.identity.Path != "media/clip.mov" {
				t.Fatalf("wrong admission: %s", job.key.identity.Path)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("actual ReadDir did not admit target")
		}
		if seen != 1 {
			t.Fatalf("admitted target not recorded: seen=%d queued=%d", seen, queued)
		}
	} else if seen != 0 || queued != 0 || len(jobs) != 0 {
		t.Fatalf("disabled actual ReadDir admitted work: seen=%d queued=%d observed=%d", seen, queued, len(jobs))
	}
}

func TestP94NativePreviewConfigOverridesAmbientEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name, ambient string
		explicit      *bool
		want          bool
	}{
		{name: "explicit-on-env-off", ambient: "0", explicit: p94Bool(true), want: true},
		{name: "explicit-off-env-on", ambient: "1", explicit: p94Bool(false), want: false},
		{name: "legacy-env-on", ambient: "1", want: true},
		{name: "legacy-env-off", ambient: "0", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("JM_NATIVE_PREVIEW_WARM", tc.ambient)
			t.Setenv("JM_JFS_MAX_READAHEAD", "1M")
			x := newBoundCacheFixture(t, WithNativePreviewWarm(tc.explicit))
			if tc.explicit != nil {
				*tc.explicit = !*tc.explicit // caller mutation cannot alter this server
			}
			p94ObservePreviewAdmission(t, x, tc.want)
		})
	}
}

func p94Bool(v bool) *bool { return &v }

func TestP94StoppedPreviewAdmissionDoesNotLeakToNewServer(t *testing.T) {
	t.Setenv("JM_NATIVE_PREVIEW_WARM", "1")
	t.Setenv("JM_JFS_MAX_READAHEAD", "1M")
	on := true
	x := newBoundCacheFixture(t, WithNativePreviewWarm(&on))
	p94ObservePreviewAdmission(t, x, true)
	rm := x.jfs.handler.readahead
	select {
	case <-rm.beginPreviewStop():
	case <-time.After(3 * time.Second):
		t.Fatal("preview worker did not stop")
	}
	// A new eligible identity defeats ordinary cooldown dedup. If admission
	// were still open, this second ReadDir would enqueue new.mov.
	entry := metadata.MakeEntry("media/new.mov", false, 65536, time.Unix(1700000000, 123), 1234)
	entry.MtimeNsecKnown = true
	if err := x.store.Insert(entry); err != nil {
		t.Fatal(err)
	}
	infos, err := x.jfs.ReadDir("media")
	if err != nil {
		t.Fatalf("post-stop ReadDir: %v", err)
	}
	if len(infos) != 2 {
		t.Fatalf("post-stop listing incomplete: %v", infos)
	}
	rm.mu.Lock()
	seen, queued := len(rm.previewSeen), len(rm.previewQueue)
	rm.mu.Unlock()
	if seen != 1 || queued != 0 {
		t.Fatalf("stopped manager admitted again: seen=%d queued=%d", seen, queued)
	}
	off := false
	fresh := newBoundCacheFixture(t, WithNativePreviewWarm(&off))
	p94ObservePreviewAdmission(t, fresh, false)
}
