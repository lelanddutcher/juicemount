package test

import "testing"

func TestMountLineForPath(t *testing.T) {
	output := "//user@nas/zpool on /Volumes/zpool (smbfs, nodev)\n" +
		"127.0.0.1:/ on /Volumes/JuiceMount-RC (nfs, nodev, nosuid)\n"

	line, ok := mountLineForPath(output, "/Volumes/JuiceMount-RC")
	if !ok {
		t.Fatal("loopback NFS mount line was not found")
	}
	if want := "127.0.0.1:/ on /Volumes/JuiceMount-RC (nfs, nodev, nosuid)"; line != want {
		t.Fatalf("mount line = %q, want %q", line, want)
	}
}

func TestJuiceMountNFSMountGuard(t *testing.T) {
	tests := []struct {
		name string
		line string
		path string
		want bool
	}{
		{
			name: "JuiceMount loopback NFS",
			line: "127.0.0.1:/ on /Volumes/JuiceMount-RC (nfs, nodev, nosuid)",
			path: "/Volumes/JuiceMount-RC",
			want: true,
		},
		{
			name: "unrelated SMB share",
			line: "//user@nas/zpool on /Volumes/zpool (smbfs, nodev, nosuid)",
			path: "/Volumes/zpool",
			want: false,
		},
		{
			name: "remote NFS server",
			line: "192.168.0.197:/mnt/zpool on /Volumes/JuiceMount-RC (nfs, nodev)",
			path: "/Volumes/JuiceMount-RC",
			want: false,
		},
		{
			name: "wrong mountpoint",
			line: "127.0.0.1:/ on /Volumes/zpool (nfs, nodev)",
			path: "/Volumes/JuiceMount-RC",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isJuiceMountNFSLine(tt.line, tt.path); got != tt.want {
				t.Fatalf("isJuiceMountNFSLine(%q, %q) = %v, want %v", tt.line, tt.path, got, tt.want)
			}
		})
	}
}
