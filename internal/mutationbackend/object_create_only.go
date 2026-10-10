package mutationbackend

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

const createOnlyProbePrefix = ".juicemount-create-only/"

// Probe each raw shard before admitting a filesystem. This rejects providers
// that ignore the conditional request; it cannot prove future behavior, power
// loss durability, or exclusion of external overwrites/deletes/delete markers.
// Probes are retained, since uncertain writes never authorize reclamation.
func qualifyCreateOnly(parent context.Context, raw object.ObjectStorage, volume string, shards int) error {
	creator, ok := raw.(object.SupportCreateOnly)
	if !ok || shards < 1 || shards > 256 || !sdkVolumeName.MatchString(volume) {
		return errObjectIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	keys, err := createOnlyProbeKeys(volume, shards)
	if err != nil {
		return errObjectIntegrity
	}
	finished := make(chan error, 1)
	go func() {
		first := []byte("juicemount create-only probe v1")
		second := []byte("conflicting create-only probe")
		verified := &verifiedObjects{ObjectStorage: raw}
		for _, key := range keys {
			if ctx.Err() != nil || verified.Put(key, bytes.NewReader(first)) != nil {
				finished <- errObjectIntegrity
				return
			}
			if ctx.Err() != nil || !errors.Is(creator.PutIfAbsent(key, bytes.NewReader(second)), os.ErrExist) {
				finished <- errObjectIntegrity
				return
			}
			reader, err := raw.Get(key, 0, -1)
			if err != nil || reader == nil {
				if reader != nil {
					_ = reader.Close()
				}
				finished <- errObjectIntegrity
				return
			}
			body, readErr := io.ReadAll(io.LimitReader(reader, int64(len(first)+1)))
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil || !bytes.Equal(body, first) {
				finished <- errObjectIntegrity
				return
			}
		}
		finished <- nil
	}()
	select {
	case err := <-finished:
		if ctx.Err() != nil {
			return errObjectIntegrity
		}
		return err
	case <-ctx.Done():
		// ObjectStorage has no cancellation/join contract. The worker may still
		// finish a reserved probe; OpenSDK keeps its process pin on this failure.
		return errObjectIntegrity
	}
}

func createOnlyProbeKeys(volume string, shards int) ([]string, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	prefix := volume + "/" + createOnlyProbePrefix + hex.EncodeToString(nonce[:]) + "-"
	keys := make([]string, shards)
	remaining := shards
	for i := 0; i < 1_000_000 && remaining > 0; i++ {
		key := prefix + strconv.Itoa(i)
		// Exact raw-key FNV-1a routing used by object.sharded.pick.
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		index := h.Sum32() % uint32(shards)
		if keys[index] == "" {
			keys[index] = key
			remaining--
		}
	}
	if remaining != 0 {
		return nil, errObjectIntegrity
	}
	return keys, nil
}
