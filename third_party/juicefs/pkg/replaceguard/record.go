// Package replaceguard defines the proxy-owned, immutable Redis evidence for
// one protected replacement. The complete old graph stays in local custody.
package replaceguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	MaxBytes    = 64 << 10
	maxChunks   = 4096
	stagePrefix = ".juicemount-streaming-drain-"
)

var ErrInvalid = errors.New("invalid replacement guard")

type Parent struct {
	Name  string `json:"name"`
	Inode uint64 `json:"inode"`
}

type File struct {
	Name    string `json:"name"`
	Parent  uint64 `json:"parent"`
	Inode   uint64 `json:"inode"`
	Length  uint64 `json:"length"`
	MtimeNS int64  `json:"mtime_ns"`
	CtimeNS int64  `json:"ctime_ns"`
}

type Record struct {
	Version       uint32   `json:"version"`
	OperationID   string   `json:"operation_id"`
	Owner         string   `json:"owner"`
	Proxy         string   `json:"proxy"`
	BackendUUID   string   `json:"backend_uuid"`
	BackendDigest string   `json:"backend_digest"`
	Project       uint64   `json:"project"`
	AcceptedEpoch uint64   `json:"accepted_epoch"`
	Epoch         uint64   `json:"epoch"`
	Parents       []Parent `json:"parents"`
	Stage         File     `json:"stage"`
	Destination   File     `json:"destination"`
	GraphDigest   string   `json:"graph_digest"`
	ImageSHA256   string   `json:"image_sha256"`
	Phase         string   `json:"phase"`
}

func Key(stage uint64) string { return "jmlease:v1:replace:" + strconv.FormatUint(stage, 10) }

func bounded(s string) bool {
	return len(s) > 0 && len(s) <= 256 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func nameOK(s string) bool {
	return bounded(s) && len(s) <= 255 && s != "." && s != ".." && !strings.Contains(s, "/")
}
func hexOK(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

func (r Record) Validate() error {
	if r.Version != 1 || !hexOK(r.OperationID, 32) || !hexOK(r.BackendDigest, 64) ||
		!hexOK(r.GraphDigest, 64) || !hexOK(r.ImageSHA256, 64) ||
		!bounded(r.Owner) || !bounded(r.Proxy) || !bounded(r.BackendUUID) ||
		r.Epoch == 0 || r.Epoch > math.MaxInt64 || r.AcceptedEpoch > r.Epoch ||
		r.Project == 0 || r.Project > math.MaxInt64 ||
		(r.Phase != "prepared" && r.Phase != "committed") ||
		len(r.Parents) == 0 || len(r.Parents) > 256 || r.Parents[0] != (Parent{Inode: 1}) {
		return ErrInvalid
	}
	seen := map[uint64]bool{}
	project := false
	for i, p := range r.Parents {
		if p.Inode == 0 || p.Inode > math.MaxInt64 || seen[p.Inode] || i > 0 && !nameOK(p.Name) {
			return ErrInvalid
		}
		seen[p.Inode] = true
		project = project || p.Inode == r.Project
	}
	parent := r.Parents[len(r.Parents)-1].Inode
	if !project || r.Stage.Parent != parent || r.Destination.Parent != parent ||
		r.Stage.Inode <= 1 || r.Stage.Inode > math.MaxInt64 ||
		r.Destination.Inode <= 1 || r.Destination.Inode > math.MaxInt64 ||
		r.Stage.Inode == r.Destination.Inode || seen[r.Stage.Inode] || seen[r.Destination.Inode] ||
		r.Stage.Length > math.MaxInt64 || r.Destination.Length > math.MaxInt64 ||
		r.Stage.MtimeNS < 0 || r.Stage.CtimeNS < 0 ||
		r.Destination.MtimeNS < 0 || r.Destination.CtimeNS < 0 ||
		uint64((r.Destination.Length+((64<<20)-1))/(64<<20)) > maxChunks ||
		!nameOK(r.Stage.Name) || !strings.HasPrefix(r.Stage.Name, stagePrefix) ||
		!nameOK(r.Destination.Name) || r.Stage.Name == r.Destination.Name {
		return ErrInvalid
	}
	return nil
}

func Encode(r Record) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxBytes {
		return nil, ErrInvalid
	}
	return b, nil
}

func Decode(raw []byte) (Record, error) {
	var r Record
	if len(raw) == 0 || len(raw) > MaxBytes {
		return r, ErrInvalid
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	canon, err := Encode(r)
	if err != nil || !bytes.Equal(raw, canon) {
		return Record{}, ErrInvalid
	}
	return r, nil
}

// RequiredKeys enumerates the immutable record, proxy/lease authority, both
// names' ancestry, and every old content chunk, including empty mappings.
func RequiredKeys(r Record) []string {
	if r.Validate() != nil {
		return nil
	}
	keys := []string{Key(r.Stage.Inode), "setting", "jmlease:v1:proxy",
		"jmlease:v1:p:" + strconv.FormatUint(r.Project, 10),
		"jmlease:v1:epoch:" + strconv.FormatUint(r.Project, 10)}
	for _, p := range r.Parents {
		id := strconv.FormatUint(p.Inode, 10)
		keys = append(keys, "i"+id, "d"+id)
	}
	for _, f := range []File{r.Stage, r.Destination} {
		id := strconv.FormatUint(f.Inode, 10)
		keys = append(keys, "i"+id, "d"+id)
	}
	count := (r.Destination.Length + (64 << 20) - 1) / (64 << 20)
	for i := uint64(0); i < count; i++ {
		keys = append(keys, "c"+strconv.FormatUint(r.Destination.Inode, 10)+"_"+strconv.FormatUint(i, 10))
	}
	return keys
}
