package mutationbackend

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

// DirectoryIdentity identifies one component of an operation's intended
// ancestor chain. The first component is the volume root: Name="", Inode=1.
type DirectoryIdentity struct {
	Name  string
	Inode uint64
}

// NewNameStageTarget records the originating operation's expected parent
// identities and absent destination. It is not a live namespace capability.
// The complete predicate must be checked atomically again at publication.
type NewNameStageTarget struct {
	Path    string
	Parents []DirectoryIdentity
}

type mediaStageEnvelope struct {
	Version string
	Request MediaStageRequest
	Target  *NewNameStageTarget `json:",omitempty"`
}

func validNewNameTarget(target NewNameStageTarget) bool {
	if len(target.Path) < 2 || len(target.Path) > 4096 || !strings.HasPrefix(target.Path, "/") ||
		path.Clean(target.Path) != target.Path || strings.ContainsRune(target.Path, 0) || !utf8.ValidString(target.Path) {
		return false
	}
	components := strings.Split(target.Path[1:], "/")
	if len(components) > 256 || len(target.Parents) != len(components) || target.Parents[0] != (DirectoryIdentity{Inode: uint64(meta.RootInode)}) {
		return false
	}
	seen := map[uint64]bool{uint64(meta.RootInode): true}
	for i, name := range components {
		if len(name) == 0 || len(name) > 255 || name == "." || name == ".." || name == meta.TrashName || strings.HasPrefix(name, ".juicemount-stage-") {
			return false
		}
		if i == len(components)-1 {
			break
		}
		parent := target.Parents[i+1]
		if parent.Name != name || parent.Inode <= uint64(meta.RootInode) || parent.Inode > math.MaxInt64 || seen[parent.Inode] {
			return false
		}
		seen[parent.Inode] = true
	}
	return true
}

// StageMediaForNewName binds a destination and its original ancestor identities
// before creating the private stage. Caller-supplied expectations are never
// refreshed to match replacement directories. The returned closed stage remains
// PREPARED and private; this method neither publishes nor returns a receipt.
func (r *SDK) StageMediaForNewName(ctx context.Context, spec MediaStageRequest, target NewNameStageTarget, input io.Reader) (mutationauthority.MediaStage, error) {
	var empty mutationauthority.MediaStage
	target.Parents = append([]DirectoryIdentity(nil), target.Parents...)
	if !validNewNameTarget(target) || input == nil {
		return empty, mutationauthority.ErrInvalid
	}
	// encoding/json replaces invalid UTF-8. Never bind a different operation
	// identity from the one the caller supplied.
	for _, value := range []string{spec.Volume, spec.Session, spec.ID, spec.FileID, spec.ExpectedRevision} {
		if !utf8.ValidString(value) {
			return empty, mutationauthority.ErrInvalid
		}
	}
	if err := r.Check(ctx); err != nil {
		return empty, err
	}
	if err := r.checkNewNameTarget(ctx, target); err != nil {
		return empty, err
	}
	body, err := json.Marshal(mediaStageEnvelope{Version: "media-stage-v2-new-name", Request: spec, Target: &target})
	if err != nil || len(body) > 64<<10 {
		return empty, mutationauthority.ErrInvalid
	}
	return r.stageMedia(ctx, spec, body, input)
}

func (r *SDK) checkNewNameTarget(ctx context.Context, target NewNameStageTarget) error {
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	for i, expected := range target.Parents {
		if err := ctx.Err(); err != nil {
			return err
		}
		parent, name := meta.RootInode, "."
		if i > 0 {
			parent, name = meta.Ino(target.Parents[i-1].Inode), expected.Name
		}
		var inode meta.Ino
		var attr meta.Attr
		if eno := r.metadata.Lookup(fctx, parent, name, &inode, &attr, false); eno != 0 || uint64(inode) != expected.Inode || attr.Typ != meta.TypeDirectory || (i > 0 && attr.Parent != parent) {
			return ErrUnavailable
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var inode meta.Ino
	var attr meta.Attr
	parent := meta.Ino(target.Parents[len(target.Parents)-1].Inode)
	if eno := r.metadata.Lookup(fctx, parent, path.Base(target.Path), &inode, &attr, false); eno != syscall.ENOENT {
		return ErrUnavailable
	}
	return nil
}
