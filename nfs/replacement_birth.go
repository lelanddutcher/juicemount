package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// replacementBirth reads protected custody, not publication permission. A known
// inode also checks the reverse index when the operation receipt is absent.
func (c *ProjectLeaseClient) replacementBirth(ctx context.Context, operation string, inode uint64) (*replaceguard.Birth, error) {
	if c == nil || c.rdb == nil || replaceguard.BirthOperationKey(operation) == "" {
		return nil, metadata.ErrMirrorConflict
	}
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	read := func(key string) ([]byte, error) {
		raw, err := c.rdb.Do(ctx, "GET", key).Text()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return []byte(raw), err
	}
	raw, err := read(replaceguard.BirthOperationKey(operation))
	if err != nil {
		return nil, err
	}
	var birth *replaceguard.Birth
	if raw != nil {
		decoded, err := replaceguard.DecodeBirth(raw)
		if err != nil || decoded.OperationID != operation || decoded.Database != int64(c.database) || decoded.Owner != c.principal || (inode != 0 && inode != decoded.Stage.Inode) {
			return nil, metadata.ErrMirrorConflict
		}
		birth, inode = &decoded, decoded.Stage.Inode
	}
	if inode == 0 {
		return nil, nil
	}
	key := replaceguard.BirthInodeKey(inode)
	if key == "" {
		return nil, metadata.ErrMirrorConflict
	}
	reverse, err := read(key)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(raw, reverse) || (raw == nil) != (reverse == nil) {
		return nil, metadata.ErrMirrorConflict
	}
	// Neither a prepared nor a committed stage may reopen for upload.
	final, err := read(replaceguard.Key(inode))
	if err != nil {
		return nil, err
	}
	if final != nil {
		return nil, metadata.ErrMirrorConflict
	}
	return birth, nil
}

func matchesReplacementBirth(b replaceguard.Birth, w metadata.SpoolReplacementWitness, name string, mode os.FileMode) bool {
	if b.Validate() != nil || b.OperationID != w.OperationID || b.BackendUUID != w.Binding.FormatUUID || b.Stage.Name != name || b.Mode != uint16(mode.Perm()) || len(w.Destination.Chain) < 2 {
		return false
	}
	parents := []replaceguard.Parent{{Inode: 1}}
	var project uint64
	for _, node := range w.Destination.Chain[:len(w.Destination.Chain)-1] {
		parents = append(parents, replaceguard.Parent{Name: path.Base(node.Path), Inode: node.Inode})
		if node.Path == projectOf(w.Destination.Path) {
			project = node.Inode
		}
	}
	if b.Project != project || b.Stage.Parent != parents[len(parents)-1].Inode || len(b.Parents) != len(parents) {
		return false
	}
	for i := range parents {
		if b.Parents[i] != parents[i] {
			return false
		}
	}
	return true
}

// openReplacementStage never adopts by filename. Only a protected reciprocal
// birth receipt permits a writable reopen; legacy existing stages remain held.
func (c *ProjectLeaseClient) openReplacementStage(ctx context.Context, backend replacementPublishIO, w metadata.SpoolReplacementWitness, name string, mode os.FileMode) (replacementPublishFile, error) {
	if name != replaceguard.BirthStageName(w.OperationID) || w.Image.Size < 0 || len(w.Destination.Chain) < 2 {
		return nil, metadata.ErrMirrorConflict
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := c.replacementBirth(ctx, w.OperationID, 0)
	if err != nil {
		return nil, err
	}
	if b != nil && !matchesReplacementBirth(*b, w, name, mode) {
		return nil, metadata.ErrMirrorConflict
	}
	named, statErr := backend.Stat(name)
	if b == nil {
		if statErr == nil {
			return nil, os.ErrExist
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			return nil, statErr
		}
	} else {
		if statErr != nil || named.Inode != b.Stage.Inode || named.Parent != b.Stage.Parent || named.Name != name || named.Length > uint64(w.Image.Size) {
			return nil, errors.Join(metadata.ErrMirrorConflict, statErr)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var f replacementPublishFile
	var incarnation string
	if b == nil {
		incarnation, err = c.replacementStageCapability(ctx)
		if err != nil {
			return nil, err
		}
		f, err = backend.Create(name, mode.Perm())
	} else {
		f, err = backend.OpenOwned(name, named)
	}
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, metadata.ErrMirrorConflict
	}
	refuse := func(err error) (replacementPublishFile, error) { return nil, errors.Join(err, f.Close()) }
	actual, err := f.Stat()
	parent := w.Destination.Chain[len(w.Destination.Chain)-2].Inode
	if err != nil || actual.Inode <= 1 || actual.Parent != parent || actual.Name != name || actual.Length > uint64(w.Image.Size) || (b == nil && actual.Length != 0) || (b != nil && actual != named) {
		return refuse(errors.Join(metadata.ErrMirrorConflict, err))
	}
	current, err := backend.Stat(name)
	if err != nil || current != actual {
		return refuse(errors.Join(metadata.ErrMirrorConflict, err))
	}
	confirmed, err := c.replacementBirth(ctx, w.OperationID, actual.Inode)
	if err != nil {
		return refuse(err)
	}
	if confirmed == nil || !matchesReplacementBirth(*confirmed, w, name, mode) || (b == nil && confirmed.Proxy != incarnation) {
		return refuse(metadata.ErrMirrorConflict)
	}
	if b != nil {
		before, _ := replaceguard.EncodeBirth(*b)
		after, _ := replaceguard.EncodeBirth(*confirmed)
		if !bytes.Equal(before, after) {
			return refuse(metadata.ErrMirrorConflict)
		}
	}
	if err := ctx.Err(); err != nil {
		return refuse(err)
	}
	if err := backend.Check(); err != nil {
		return refuse(err)
	}
	return f, nil
}
