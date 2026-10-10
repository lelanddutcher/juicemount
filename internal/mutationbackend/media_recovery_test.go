package mutationbackend

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func recoveryLayoutFixture(t *testing.T) (*sdkMedia, mutationauthority.MediaStage) {
	t.Helper()
	m := &sdkMedia{volume: "volume", binding: strings.Repeat("a", 64), format: meta.Format{Name: "fixture", Storage: "file", Bucket: "/owned/", BlockSize: 64}}
	p := MediaStageRequest{Volume: "volume", Session: "session", Epoch: 1, ID: "stage", FileID: "file", ExpectedRevision: "parent@3,file@5", Size: 4, SHA256: digest([]byte("data")), CustodyArtifactSHA256: digest([]byte("custody"))}
	body, _ := json.Marshal(struct {
		Version string
		Request MediaStageRequest
	}{"media-stage-v1", p})
	r := mutationauthority.Request{Lease: mutationauthority.Lease{Volume: p.Volume, Session: p.Session, Epoch: p.Epoch}, ID: p.ID, Digest: digest(body), Kind: mutationauthority.MediaPublishOperation, FileID: p.FileID, ExpectedRevision: p.ExpectedRevision}
	s := mutationauthority.MediaStage{Request: r, Intent: mutationauthority.StageIntent{Body: body, Path: "/.juicemount-stage-" + digest([]byte("volume\x00stage")), Backend: m.binding, Size: 4, SHA256: p.SHA256}, Inode: 9, Marker: r.Digest,
		Slices: []mutationauthority.StageSlice{{ID: 11, Inode: 9, Written: true, Size: 4, Length: 4}}, Objects: []mutationauthority.ObjectAttempt{{Attempt: 1, Slice: 11, Provider: "file", Bucket: "/owned/", Key: m.key(11, 0, 4), Size: 4, SHA256: p.SHA256, Verified: true}}}
	return m, s
}

func TestRetainedStageLayoutRejectsCorruption(t *testing.T) {
	mutations := map[string]func(*mutationauthority.MediaStage){
		"noncanonical-json": func(s *mutationauthority.MediaStage) { s.Intent.Body = append(s.Intent.Body, ' ') },
		"version": func(s *mutationauthority.MediaStage) {
			s.Intent.Body = []byte(strings.Replace(string(s.Intent.Body), "media-stage-v1", "media-stage-v2", 1))
		},
		"operation":            func(s *mutationauthority.MediaStage) { s.Request.FileID = "other" },
		"backend":              func(s *mutationauthority.MediaStage) { s.Intent.Backend = strings.Repeat("b", 64) },
		"path":                 func(s *mutationauthority.MediaStage) { s.Intent.Path = "/public" },
		"marker":               func(s *mutationauthority.MediaStage) { s.Marker = strings.Repeat("b", 64) },
		"unbound-with-effect":  func(s *mutationauthority.MediaStage) { s.Inode = 0; s.Marker = "" },
		"slice-inode":          func(s *mutationauthority.MediaStage) { s.Slices[0].Inode = 10 },
		"duplicate-slice":      func(s *mutationauthority.MediaStage) { s.Slices = append(s.Slices, s.Slices[0]) },
		"slice-range":          func(s *mutationauthority.MediaStage) { s.Slices[0].Offset = 2 },
		"unwritten-with-range": func(s *mutationauthority.MediaStage) { s.Slices[0].Written = false },
		"slice-owner":          func(s *mutationauthority.MediaStage) { s.Objects[0].Slice = 12 },
		"key-alias": func(s *mutationauthority.MediaStage) {
			s.Objects[0].Key = strings.Replace(s.Objects[0].Key, "11_0_4", "011_0_4", 1)
		},
		"provider":        func(s *mutationauthority.MediaStage) { s.Objects[0].Provider = "s3" },
		"bucket":          func(s *mutationauthority.MediaStage) { s.Objects[0].Bucket = "/other/" },
		"shard":           func(s *mutationauthority.MediaStage) { s.Objects[0].Shard = 1 },
		"attempt":         func(s *mutationauthority.MediaStage) { s.Objects[0].Attempt = 0 },
		"hash":            func(s *mutationauthority.MediaStage) { s.Objects[0].SHA256 = "bad" },
		"size":            func(s *mutationauthority.MediaStage) { s.Objects[0].Size = 33 << 20 },
		"open-manifest":   func(s *mutationauthority.MediaStage) { s.Manifest = strings.Repeat("b", 64) },
		"closed-manifest": func(s *mutationauthority.MediaStage) { s.Closed = true; s.Manifest = strings.Repeat("b", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			m, s := recoveryLayoutFixture(t)
			mutate(&s)
			if err := m.validateRetainedLayout(s); err == nil {
				t.Fatal("corrupt stage accepted")
			}
		})
	}
}

func TestRetainedStageLayoutPreservesOpenAndClosedOutcomes(t *testing.T) {
	m, s := recoveryLayoutFixture(t)
	if err := m.validateRetainedLayout(s); err != nil {
		t.Fatal(err)
	}
	s.Manifest = mutationauthority.StageSnapshotDigest(s)
	s.Closed = true
	if err := m.validateRetainedLayout(s); err != nil {
		t.Fatal("valid closed stage refused", err)
	}
	s.Objects[0].Verified = false
	if err := m.validateRetainedLayout(s); err == nil {
		t.Fatal("closed stage with pending object accepted")
	}
	s.Closed = false
	s.Manifest = ""
	if err := m.validateRetainedLayout(s); err != nil {
		t.Fatal("pending raw attempt could not be retained", err)
	}
	s.Slices[0] = mutationauthority.StageSlice{ID: 11, Inode: 9}
	if err := m.validateRetainedLayout(s); err != nil {
		t.Fatal("allocated unwritten slice could not be retained", err)
	}
	s.Inode = 0
	s.Marker = ""
	s.Slices = nil
	s.Objects = nil
	if err := m.validateRetainedLayout(s); err != nil {
		t.Fatal("unbound crash gap could not be retained", err)
	}
}
