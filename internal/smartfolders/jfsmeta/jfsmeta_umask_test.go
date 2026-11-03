//go:build linux || darwin

package jfsmeta_test

import (
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/smartfolders"
	"github.com/lelanddutcher/juicemount/internal/smartfolders/jfsmeta"
)

// Mkdir identifies the new directory from the create call itself (no
// second lookup) and gets exactly the requested mode whatever the process
// umask: the JuiceFS client applies only the cumask it is given (0).
func TestMetaFSMkdirIdentityAndModeFromCreate(t *testing.T) {
	url := metaTestURL(t)
	formatThrowawayVolume(t, url)
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)
	fsys, err := jfsmeta.Open(url)
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()
	info, err := fsys.Mkdir("D", smartfolders.OwnedDirPerm)
	if err != nil {
		t.Fatal(err)
	}
	if info.Type != smartfolders.TypeDir || info.Perm != smartfolders.OwnedDirPerm || info.Ino == 0 {
		t.Fatalf("Mkdir = %+v", info)
	}
	conf := meta.DefaultConf()
	conf.NoBGJob = true
	probe := meta.NewClient(url, conf)
	if _, err := probe.Load(true); err != nil {
		t.Fatal(err)
	}
	defer probe.Shutdown()
	var ino meta.Ino
	var attr meta.Attr
	if st := probe.Lookup(meta.Background(), meta.RootInode, "D", &ino, &attr, false); st != 0 {
		t.Fatal(st)
	}
	if uint64(ino) != info.Ino || attr.Mode&0o777 != uint16(smartfolders.OwnedDirPerm) {
		t.Fatalf("on the volume: ino %d mode %o; Mkdir reported %+v", ino, attr.Mode&0o777, info)
	}
}
