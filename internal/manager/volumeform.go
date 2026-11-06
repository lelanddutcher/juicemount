package manager

// volumeform.go: the byte form of a folder name that the volume holds. A
// name can be composed (NFC) or decomposed (NFD, as a macOS listing gives
// it), and JuiceFS compares names byte for byte: two folders whose names
// differ only in form are two folders, and the proxy resolves a grant to
// exactly the directory whose name has the grant's bytes. So a grant, or a
// query folder, given in the other form is stored in the form the volume
// holds, and every later comparison with it stays byte for byte.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/redis/go-redis/v9"
	"golang.org/x/text/unicode/norm"
)

// dirExistsFunc reports whether the volume holds a directory at exactly p
// (volume-relative, no leading slash), byte for byte.
type dirExistsFunc func(ctx context.Context, p string) (bool, error)

// errFolderFormAmbiguous: the volume holds a folder name in both forms and
// the path given is neither exactly.
var errFolderFormAmbiguous = errors.New("the volume holds this folder name both composed and decomposed")

// errVolumeUnread: the volume could not be read to find a folder's form.
var errVolumeUnread = errors.New("cannot read the volume")

// volumeForm returns p (volume-relative, "A/B") in the form the volume
// holds, one component at a time: the exact name when that directory
// exists, otherwise the one NFC or NFD form of it that does. found is false
// when some component exists in no form; p is then returned as given.
func volumeForm(ctx context.Context, exists dirExistsFunc, p string) (form string, found bool, err error) {
	var parts []string
	for _, c := range strings.Split(p, "/") {
		var cands []string
		for _, v := range []string{c, norm.NFC.String(c), norm.NFD.String(c)} {
			if !slices.Contains(cands, v) {
				cands = append(cands, v)
			}
		}
		var held []string
		for i, v := range cands {
			ok, err := exists(ctx, strings.Join(append(parts, v), "/"))
			if err != nil {
				return "", false, fmt.Errorf("%w: %v", errVolumeUnread, err)
			}
			if ok {
				held = append(held, v)
				if i == 0 {
					break // the exact name
				}
			}
		}
		switch {
		case len(held) == 0:
			return p, false, nil
		case len(held) > 1:
			return "", false, fmt.Errorf("%w: /%s; give the exact name", errFolderFormAmbiguous, strings.Join(append(parts, c), "/"))
		}
		parts = append(parts, held[0])
	}
	return strings.Join(parts, "/"), true, nil
}

// redisDirExists looks p up in the volume's metadata as the proxy does
// (respproxy lookupPath): from the root, one directory entry per component,
// by exact name. An entry's value is its type (2: directory) and then its
// inode, big-endian (pkg/meta).
func redisDirExists(rdb redis.Cmdable) dirExistsFunc {
	return func(ctx context.Context, p string) (bool, error) {
		dir := uint64(1)
		for _, c := range strings.Split(p, "/") {
			v, err := rdb.HGet(ctx, "d"+strconv.FormatUint(dir, 10), c).Result()
			if err == redis.Nil {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if len(v) < 9 {
				return false, fmt.Errorf("directory entry %q of inode %d: %d bytes", c, dir, len(v))
			}
			if v[0] != 2 {
				return false, nil
			}
			dir = binary.BigEndian.Uint64([]byte(v[1:9]))
		}
		return true, nil
	}
}

// folderFormFunc is volumeForm over some view of the volume.
type folderFormFunc func(ctx context.Context, p string) (form string, found bool, err error)

// redisFolderForm is volumeForm over the volume's metadata, with a client
// of its own per call (the people routes hold no connection to the volume).
func redisFolderForm(metaURL string) folderFormFunc {
	return func(ctx context.Context, p string) (string, bool, error) {
		opts, err := redisurl.Parse(metaURL)
		if err != nil {
			return "", false, fmt.Errorf("%w: %v", errVolumeUnread, err)
		}
		rdb := redis.NewClient(opts)
		defer rdb.Close()
		return volumeForm(ctx, redisDirExists(rdb), p)
	}
}
