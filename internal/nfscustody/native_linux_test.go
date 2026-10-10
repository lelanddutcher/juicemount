package nfscustody

import (
	"errors"
	"testing"
)

func TestLinuxNativeCustodyFailsClosed(t *testing.T) {
	if nativeCapability() {
		t.Fatal("Linux reported native descriptor NFS custody")
	}
	if root, err := openNativeRoot("/unused", "unused"); root != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Linux native root admission: root=%v err=%v", root, err)
	}
	if err := provisionACL(-1, true); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Linux ACL admission: %v", err)
	}
	if receipt, effect, err := nativeProvision(provisionRequest{}); effect || !errors.Is(err, ErrUnsupported) || receipt.State != "" {
		t.Fatalf("Linux privileged provision unexpectedly attempted: receipt=%+v effect=%t err=%v", receipt, effect, err)
	}
}
