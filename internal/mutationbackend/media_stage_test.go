package mutationbackend

import (
	"errors"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

func TestMediaObjectKeyRejectsAliasesAndMalformedLayout(t *testing.T) {
	for _, hashPrefix := range []bool{false, true} {
		m := &sdkMedia{format: meta.Format{Name: "fixture", BlockSize: 64, HashPrefix: hashPrefix}}
		key := m.key(1234567, 2, 43)
		id, block, size, err := m.parseKey(key)
		if err != nil || id != 1234567 || block != 2 || size != 43 {
			t.Fatal(key, err)
		}
		for _, bad := range []string{key + "/", "other/" + key, strings.TrimSuffix(key, "_43") + "_043", "fixture/chunks/00/0/1234567_2_43", m.key(0, 0, 1), m.key(1, 1024, 1), m.key(1, 0, 65537), m.key(1, -1, 1)} {
			if _, _, _, err := m.parseKey(bad); err == nil {
				t.Fatal("noncanonical key accepted", bad)
			}
		}
	}
}

func TestRawObjectGateDeniesPromotedMutationMethods(t *testing.T) {
	// A nil embedded provider would panic if any method forwarded around Put.
	v := &verifiedObjects{}
	if !errors.Is(v.Create(), errObjectReclamation) || !errors.Is(v.Copy("to", "from"), errObjectReclamation) || !errors.Is(v.CompleteUpload("key", "id", nil), errObjectReclamation) {
		t.Fatal("mutation bypass")
	}
	if _, err := v.CreateMultipartUpload("key"); !errors.Is(err, errObjectReclamation) {
		t.Fatal(err)
	}
	if _, err := v.UploadPart("key", "id", 1, nil); !errors.Is(err, errObjectReclamation) {
		t.Fatal(err)
	}
	if _, err := v.UploadPartCopy("key", "id", 1, "from", 0, 1); !errors.Is(err, errObjectReclamation) {
		t.Fatal(err)
	}
	v.AbortUpload("key", "id")
}
