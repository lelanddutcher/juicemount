package main

import (
	"errors"
	"testing"
	"time"
)

func TestClassifyConfiguredNFSMount(t *testing.T) {
	const path = "/Volumes/JuiceMount"
	for _, tc := range []struct {
		name   string
		output string
		err    error
		want   nfsMountOwnership
	}{
		{name: "absent", output: "disk1s1 on / (apfs, local)\n", want: nfsMountAbsent},
		{name: "unverified legacy loopback", output: "127.0.0.1:/ on " + path + " (nfs, nodev, nosuid)\n", want: nfsMountForeignOrUnknown},
		{name: "APFS", output: "disk3s1 on " + path + " (apfs, local)\n", want: nfsMountForeignOrUnknown},
		{name: "SMB", output: "//host/share on " + path + " (smbfs, nodev)\n", want: nfsMountForeignOrUnknown},
		{name: "other NFS", output: "192.0.2.1:/ on " + path + " (nfs, nodev)\n", want: nfsMountForeignOrUnknown},
		{name: "lookalike loopback", output: "127.0.0.1:/other on " + path + " (nfs, nodev)\n", want: nfsMountForeignOrUnknown},
		{name: "mounttable timeout", err: errors.New("deadline exceeded"), want: nfsMountForeignOrUnknown},
		{name: "malformed entry", output: "127.0.0.1:/ on " + path + "\n", want: nfsMountForeignOrUnknown},
		{name: "duplicate entry", output: "127.0.0.1:/ on " + path + " (nfs, nodev)\ndisk3s1 on " + path + " (apfs, local)\n", want: nfsMountForeignOrUnknown},
		{name: "partial path", output: "disk3s1 on " + path + "Other (apfs, local)\n", want: nfsMountAbsent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyConfiguredNFSMount(path, []byte(tc.output), tc.err); got != tc.want {
				t.Fatalf("classifyConfiguredNFSMount = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestForeignOrUnknownNFSMountNeverRunsUnmount(t *testing.T) {
	for _, state := range []nfsMountOwnership{nfsMountForeignOrUnknown, nfsMountOurs} {
		calls := 0
		if unmountNFSWith("/Volumes/JuiceMount", func(string) nfsMountOwnership { return state }, func(time.Duration, string, ...string) error {
			calls++
			return nil
		}) {
			t.Fatal("foreign or unknown mount reported unmounted")
		}
		if calls != 0 {
			t.Fatalf("ran %d unmount commands against foreign or unknown mount", calls)
		}
	}
}

func TestNFSMountOwnershipChangeStopsUnmountEscalation(t *testing.T) {
	checks, calls := 0, 0
	inspect := func(string) nfsMountOwnership {
		checks++
		if checks <= 3 {
			return nfsMountOurs
		}
		return nfsMountForeignOrUnknown
	}
	if unmountNFSWith("/Volumes/JuiceMount", inspect, func(time.Duration, string, ...string) error {
		calls++
		return nil
	}) {
		t.Fatal("ownership changed to foreign but unmount reported success")
	}
	if calls != 0 {
		t.Fatalf("ran %d unmount commands without exact instance authority, want 0", calls)
	}
}
