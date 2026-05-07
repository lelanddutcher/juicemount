package nfs

import "testing"

// TestCanonicalize locks in the path-translation contract used by the
// offline-mode open gate. The pin store keys on the user-facing mount
// path; go-nfs hands us in-mount filenames in a few different shapes,
// and a silent miss in this translation would cause the gate to refuse
// pinned files (UX cliff: "media offline" while on cellular).
func TestCanonicalize(t *testing.T) {
	cases := []struct {
		name     string
		mount    string
		filename string
		want     string
	}{
		{
			name:     "plain relative",
			mount:    "/Volumes/zpool",
			filename: "Film Projects/foo.mov",
			want:     "/Volumes/zpool/Film Projects/foo.mov",
		},
		{
			name:     "leading-slash relative",
			mount:    "/Volumes/zpool",
			filename: "/Film Projects/foo.mov",
			want:     "/Volumes/zpool/Film Projects/foo.mov",
		},
		{
			name:     "already absolute under mount",
			mount:    "/Volumes/zpool",
			filename: "/Volumes/zpool/Film Projects/foo.mov",
			want:     "/Volumes/zpool/Film Projects/foo.mov",
		},
		{
			name:     "trailing slash on mount point",
			mount:    "/Volumes/zpool/",
			filename: "Film Projects/foo.mov",
			want:     "/Volumes/zpool/Film Projects/foo.mov",
		},
		{
			name:     "non-default mount point",
			mount:    "/Volumes/storage",
			filename: "media/clip.mov",
			want:     "/Volumes/storage/media/clip.mov",
		},
		{
			name:     "empty mount point falls back to legacy default",
			mount:    "",
			filename: "media/clip.mov",
			want:     "/Volumes/zpool/media/clip.mov",
		},
		{
			name:     "root file",
			mount:    "/Volumes/zpool",
			filename: "README.txt",
			want:     "/Volumes/zpool/README.txt",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &JuiceMountHandler{mountPoint: tc.mount}
			got := h.canonicalize(tc.filename)
			if got != tc.want {
				t.Errorf("canonicalize(mount=%q, file=%q) = %q, want %q",
					tc.mount, tc.filename, got, tc.want)
			}
		})
	}
}
