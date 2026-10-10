package mutationbackend

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func TestNewNameTargetRequiresCanonicalOriginalChain(t *testing.T) {
	valid := NewNameStageTarget{Path: "/parent/child/file.mov", Parents: []DirectoryIdentity{{Inode: 1}, {Name: "parent", Inode: 2}, {Name: "child", Inode: 3}}}
	for _, tc := range []struct {
		name   string
		change func(*NewNameStageTarget)
	}{
		{"relative", func(v *NewNameStageTarget) { v.Path = v.Path[1:] }},
		{"root", func(v *NewNameStageTarget) { v.Path = "/" }},
		{"empty", func(v *NewNameStageTarget) { v.Path = "" }},
		{"double-slash", func(v *NewNameStageTarget) { v.Path = "/parent//child/file.mov" }},
		{"dot", func(v *NewNameStageTarget) { v.Path = "/parent/./file.mov" }},
		{"dotdot", func(v *NewNameStageTarget) { v.Path = "/parent/child/../file.mov" }},
		{"trailing-slash", func(v *NewNameStageTarget) { v.Path += "/" }},
		{"nul", func(v *NewNameStageTarget) { v.Path += "\x00" }},
		{"invalid-utf8", func(v *NewNameStageTarget) { v.Path += "\xff" }},
		{"reserved-stage", func(v *NewNameStageTarget) { v.Path = "/parent/child/.juicemount-stage-fake" }},
		{"trash", func(v *NewNameStageTarget) { v.Path = "/parent/child/.trash" }},
		{"long-name", func(v *NewNameStageTarget) { v.Path = "/parent/child/" + strings.Repeat("x", 256) }},
		{"no-root", func(v *NewNameStageTarget) { v.Parents = nil }},
		{"wrong-root", func(v *NewNameStageTarget) { v.Parents[0].Inode = 9 }},
		{"named-root", func(v *NewNameStageTarget) { v.Parents[0].Name = "/" }},
		{"missing-parent", func(v *NewNameStageTarget) { v.Parents = v.Parents[:2] }},
		{"renamed-parent", func(v *NewNameStageTarget) { v.Parents[1].Name = "replacement" }},
		{"unknown-inode", func(v *NewNameStageTarget) { v.Parents[2].Inode = 0 }},
		{"duplicate-inode", func(v *NewNameStageTarget) { v.Parents[2].Inode = 2 }},
		{"inode-overflow", func(v *NewNameStageTarget) { v.Parents[2].Inode = math.MaxUint64 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := valid
			v.Parents = append([]DirectoryIdentity(nil), valid.Parents...)
			tc.change(&v)
			if validNewNameTarget(v) {
				t.Fatal("invalid target accepted")
			}
		})
	}
	if !validNewNameTarget(valid) || !validNewNameTarget(NewNameStageTarget{Path: "/file.mov", Parents: []DirectoryIdentity{{Inode: 1}}}) {
		t.Fatal("valid target refused")
	}
}

func TestNewNameStageEnvelopePreservesLegacyAndBindsTarget(t *testing.T) {
	spec := MediaStageRequest{Volume: "volume", Session: "session", Epoch: 1, ID: "operation", FileID: "file", ExpectedRevision: "absent", Size: 7, SHA256: digest([]byte("payload")), CustodyArtifactSHA256: digest([]byte("custody"))}
	legacy, _ := json.Marshal(struct {
		Version string
		Request MediaStageRequest
	}{"media-stage-v1", spec})
	current, _ := json.Marshal(mediaStageEnvelope{Version: "media-stage-v1", Request: spec})
	if string(current) != string(legacy) {
		t.Fatal("legacy canonical identity changed")
	}
	target := NewNameStageTarget{Path: "/folder/file.mov", Parents: []DirectoryIdentity{{Inode: 1}, {Name: "folder", Inode: 2}}}
	for _, version := range []string{"media-stage-v1", "media-stage-v2-new-name"} {
		body := mediaStageEnvelope{Version: version, Request: spec}
		if version != "media-stage-v1" {
			body.Target = &target
		}
		raw, _ := json.Marshal(body)
		s := mutationauthority.MediaStage{Request: mutationauthority.Request{Lease: mutationauthority.Lease{Volume: spec.Volume, Session: spec.Session, Epoch: spec.Epoch}, ID: spec.ID, Digest: digest(raw), Kind: mutationauthority.MediaPublishOperation, FileID: spec.FileID, ExpectedRevision: spec.ExpectedRevision}, Intent: mutationauthority.StageIntent{Body: raw, Path: "/.juicemount-stage-" + digest([]byte(spec.Volume+"\x00"+spec.ID)), Backend: digest([]byte("binding")), Size: spec.Size, SHA256: spec.SHA256}}
		if err := validateStageIntent(s, "volume", s.Intent.Backend); err != nil {
			t.Fatal(version, err)
		}
		body.Target = &NewNameStageTarget{Path: "/other.mov", Parents: []DirectoryIdentity{{Inode: 1}}}
		changed, _ := json.Marshal(body)
		s.Intent.Body = changed
		if validateStageIntent(s, "volume", s.Intent.Backend) == nil {
			t.Fatal("retargeted stage accepted with old operation digest")
		}
		if version == "media-stage-v1" {
			s.Request.Digest = digest(changed)
			if validateStageIntent(s, "volume", s.Intent.Backend) == nil {
				t.Fatal("v1 accepted target extension")
			}
		}
	}
}
