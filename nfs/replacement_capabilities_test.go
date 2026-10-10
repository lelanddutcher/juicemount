package nfs

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type capabilityDoer struct {
	gets  []string
	reply any
	err   error
}

func (d *capabilityDoer) Do(ctx context.Context, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx, args...)
	if len(args) > 0 && args[0] == "GET" {
		d.gets = append(d.gets, args[1].(string))
		cmd.SetErr(redis.Nil)
	} else if d.err != nil {
		cmd.SetErr(d.err)
	} else {
		cmd.SetVal(d.reply)
	}
	return cmd
}

func capabilityClient(d *capabilityDoer) *ProjectLeaseClient {
	c := newProjectLeaseClient(d, ProjectLeaseConfig{Timeout: time.Second})
	c.database, c.principal = 3, "mac1"
	return c
}

func TestReplacementCapabilityStrictReplyAndLeaseIsolation(t *testing.T) {
	good := []any{int64(1), int64(3), "mac1", "proxy-incarnation", int64(1)}
	for _, tc := range []struct {
		name  string
		reply any
		err   error
		valid bool
	}{
		{"ready", good, nil, true},
		{"old provider", nil, errors.New("ERR unknown JM.LEASE subcommand"), false},
		{"canceled", nil, context.Canceled, false},
		{"deadline", nil, context.DeadlineExceeded, false},
		{"truncated", good[:4], nil, false},
		{"non-boolean readiness", []any{int64(1), int64(3), "mac1", "proxy-incarnation", int64(2)}, nil, false},
		{"off", []any{int64(1), int64(3), "mac1", "proxy-incarnation", int64(0)}, nil, false},
		{"wrong version", []any{int64(2), int64(3), "mac1", "proxy-incarnation", int64(1)}, nil, false},
		{"wrong DB", []any{int64(1), int64(4), "mac1", "proxy-incarnation", int64(1)}, nil, false},
		{"wrong principal", []any{int64(1), int64(3), "mac2", "proxy-incarnation", int64(1)}, nil, false},
		{"oversized principal", []any{int64(1), int64(3), strings.Repeat("x", 257), "proxy-incarnation", int64(1)}, nil, false},
		{"missing incarnation", []any{int64(1), int64(3), "mac1", "", int64(1)}, nil, false},
		{"oversized incarnation", []any{int64(1), int64(3), "mac1", strings.Repeat("x", 257), int64(1)}, nil, false},
		{"wrong type", []any{"1", int64(3), "mac1", "proxy-incarnation", int64(1)}, nil, false},
		{"wrong readiness type", []any{int64(1), int64(3), "mac1", "proxy-incarnation", "1"}, nil, false},
		{"trailing element", append(append([]any{}, good...), "extra"), nil, false},
		{"not array", "ready", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := capabilityClient(&capabilityDoer{reply: tc.reply, err: tc.err})
			incarnation, err := c.replacementStageCapability(context.Background())
			if tc.valid {
				if err != nil || incarnation != "proxy-incarnation" {
					t.Fatalf("valid capability: %q %v", incarnation, err)
				}
			} else if !errors.Is(err, errReplacementCapabilityUnavailable) || incarnation != "" {
				t.Fatalf("invalid capability: %q %v", incarnation, err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("lost underlying capability error: %v", err)
			}
			if !c.unsupported.IsZero() || len(c.projects) != 0 {
				t.Fatal("capability probe changed ordinary lease state")
			}
		})
	}
}

type capabilityStageIO struct {
	created int
	file    *capabilityStageFile
}

func (b *capabilityStageIO) Check() error { return nil }
func (b *capabilityStageIO) Open(string, bool) (replacementPublishFile, error) {
	return nil, os.ErrNotExist
}
func (b *capabilityStageIO) OpenOwned(string, replaceguard.File) (replacementPublishFile, error) {
	return nil, os.ErrNotExist
}
func (b *capabilityStageIO) Create(name string, _ os.FileMode) (replacementPublishFile, error) {
	b.created++
	b.file = &capabilityStageFile{record: replaceguard.File{Name: name, Parent: 3, Inode: 90}}
	return b.file, nil
}
func (b *capabilityStageIO) Stat(string) (replaceguard.File, error) {
	if b.file == nil {
		return replaceguard.File{}, os.ErrNotExist
	}
	return b.file.record, nil
}
func (b *capabilityStageIO) Rename(string, string) error { return errors.New("unexpected rename") }

type capabilityStageFile struct {
	record         replaceguard.File
	closed, writes int
}

func (f *capabilityStageFile) Stat() (replaceguard.File, error) { return f.record, nil }
func (f *capabilityStageFile) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("unexpected read")
}
func (f *capabilityStageFile) WriteAt([]byte, int64) (int, error) {
	f.writes++
	return 0, errors.New("unexpected write")
}
func (f *capabilityStageFile) SetMtime(int64) error      { return errors.New("unexpected mtime") }
func (f *capabilityStageFile) SetMode(os.FileMode) error { return errors.New("unexpected mode") }
func (f *capabilityStageFile) Sync() error               { return errors.New("unexpected sync") }
func (f *capabilityStageFile) Close() error              { f.closed++; return nil }

func TestReplacementCapabilityGatesFreshCreateAndBornUpload(t *testing.T) {
	op := strings.Repeat("a", 32)
	w := metadata.SpoolReplacementWitness{OperationID: op,
		Image: metadata.SpoolReplacementImage{Size: 1},
		Destination: metadata.CachePathObservation{Path: "media/project/clip.mov",
			Chain: []metadata.CachePathNode{{Path: "media", Inode: 2}, {Path: "media/project", Inode: 3}, {Path: "media/project/clip.mov", Inode: 4}}}}
	for _, tc := range []struct {
		name       string
		reply      any
		err        error
		wantCreate int
	}{
		{"old provider", nil, errors.New("ERR unknown JM.LEASE subcommand"), 0},
		{"birth disabled", []any{int64(1), int64(3), "mac1", "proxy-incarnation", int64(0)}, nil, 0},
		{"positive then birth missing", []any{int64(1), int64(3), "mac1", "proxy-incarnation", int64(1)}, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doer := &capabilityDoer{reply: tc.reply, err: tc.err}
			c := capabilityClient(doer)
			backend := &capabilityStageIO{}
			f, err := c.openReplacementStage(context.Background(), backend, w, replaceguard.BirthStageName(op), 0600)
			if f != nil || err == nil || backend.created != tc.wantCreate {
				t.Fatalf("stage=%v error=%v creates=%d", f, err, backend.created)
			}
			if tc.wantCreate == 1 {
				want := []string{replaceguard.BirthOperationKey(op), replaceguard.BirthOperationKey(op), replaceguard.BirthInodeKey(90), replaceguard.Key(90)}
				if !slices.Equal(doer.gets, want) {
					t.Fatalf("post-create custody confirmation not reached: got=%v want=%v", doer.gets, want)
				}
			}
			if backend.file != nil && (backend.file.closed != 1 || backend.file.writes != 0) {
				t.Fatalf("unborn descriptor custody: %+v", backend.file)
			}
			if !c.unsupported.IsZero() {
				t.Fatal("ordinary lease unsupported backoff changed")
			}
		})
	}
}
