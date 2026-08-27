package nfs

import (
	"bytes"
	"context"
	"path"
	"strings"

	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type readDirPlusArgs struct {
	Handle      []byte
	Cookie      uint64
	CookieVerif uint64
	DirCount    uint32
	MaxCount    uint32
}

type readDirPlusEntity struct {
	FileID     uint64
	Name       []byte
	Cookie     uint64
	Attributes *FileAttribute `xdr:"optional"`
	Handle     *[]byte        `xdr:"optional"`
	Next       bool
}

func joinPath(parent []string, elements ...string) []string {
	joinedPath := make([]string, 0, len(parent)+len(elements))
	joinedPath = append(joinedPath, parent...)
	joinedPath = append(joinedPath, elements...)
	return joinedPath
}

func onReadDirPlus(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = opAttrErrorFormatter
	obj := readDirPlusArgs{}
	if err := xdr.Read(w.req.Body, &obj); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	// in case of test, nfs-client send:
	// DirCount = 512
	// MaxCount = 4096
	if obj.DirCount < 512 || obj.MaxCount < 4096 {
		return &NFSStatusError{NFSStatusTooSmall, nil}
	}

	fs, p, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	contents, verifier, err := getDirListingWithVerifier(userHandle, obj.Handle, obj.CookieVerif)
	if err != nil {
		return err
	}
	if obj.Cookie > 0 && obj.CookieVerif > 0 && verifier != obj.CookieVerif {
		return &NFSStatusError{NFSStatusBadCookie, nil}
	}

	entities := make([]readDirPlusEntity, 0)
	dirBytes := uint32(0)
	maxBytes := uint32(100) // conservative overhead measure

	if obj.Cookie == 0 {
		// add '.' and '..' to entities
		dotdotFileID := uint64(0)
		if len(p) > 0 {
			dda := tryStat(fs, p[0:len(p)-1])
			if dda != nil {
				dotdotFileID = dda.Fileid
			}
		}
		dotFileID := uint64(0)
		da := tryStat(fs, p)
		if da != nil {
			dotFileID = da.Fileid
		}
		entities = append(entities,
			readDirPlusEntity{Name: []byte("."), Cookie: 0, Next: true, FileID: dotFileID, Attributes: da},
			readDirPlusEntity{Name: []byte(".."), Cookie: 1, Next: true, FileID: dotdotFileID},
		)
	}

	eof := true
	maxEntities := userHandle.HandleLimit() / 2
	fb := 0
	fss := 0
	for i := readDirStartIndex(obj.Cookie, len(contents)); i < len(contents); i++ {
		c := contents[i]
		// cookie equates to index within contents + 2 (for '.' and '..')
		cookie := uint64(i + 2)
		fb++
		fss++
		// AppleDouble companions must remain visible so rsync/backup tools can
		// round-trip them, but macOS does not display them in Finder. Eagerly
		// attaching attributes and a file handle for every `._` entry makes the
		// kernel instantiate thousands of vnodes it will never use. Both fields
		// are optional in NFSv3 READDIRPLUS; omit them only for hidden sidecars.
		// A client that actually accesses one performs the normal LOOKUP and gets
		// the same attributes/handle there. User files keep the full fast path.
		includePostOps := !strings.HasPrefix(c.Name(), "._")
		nameLen := uint32(len(c.Name()))
		entryDirBytes := nameLen + 20 // dir overhead
		filePath := joinPath(p, c.Name())
		attrs := ToFileAttribute(c, path.Join(filePath...))
		var responseAttrs *FileAttribute
		var responseHandle *[]byte
		handleLen := -1
		if includePostOps {
			handle := userHandle.ToHandle(fs, filePath)
			responseAttrs = attrs
			responseHandle = &handle
			handleLen = len(handle)
		}
		entryMaxBytes := readDirPlusEntryMaxBytes(c.Name(), includePostOps, handleLen)
		dirBytes += entryDirBytes
		maxBytes += entryMaxBytes
		if dirBytes > obj.DirCount || maxBytes > obj.MaxCount || len(entities) > maxEntities {
			eof = false
			break
		}
		entities = append(entities, readDirPlusEntity{
			FileID:     attrs.Fileid,
			Name:       []byte(c.Name()),
			Cookie:     cookie,
			Attributes: responseAttrs,
			Handle:     responseHandle,
			Next:       true,
		})
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, p)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, verifier); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, len(entities) > 0); err != nil { // next
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if len(entities) > 0 {
		entities[len(entities)-1].Next = false
		// no next for last entity

		for _, e := range entities {
			if err := xdr.Write(writer, e); err != nil {
				return &NFSStatusError{NFSStatusServerFault, err}
			}
		}
	}
	if err := xdr.Write(writer, eof); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	// TODO: track writer size at this point to validate maxcount estimation and stop early if needed.

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}

// readDirPlusEntryMaxBytes returns the exact XDR payload size, including each
// optional-field presence word and the linked-list continuation word. A
// negative handleLen means the optional handle is absent. Keeping this helper
// in sync with readDirPlusEntity is a correctness boundary: under-counting can
// overrun the client's MaxCount; over-counting needlessly fragments listings.
func readDirPlusEntryMaxBytes(name string, includeAttrs bool, handleLen int) uint32 {
	nameLen := uint32(len(name))
	namePadded := (nameLen + 3) &^ 3
	size := uint32(8 + 4 + namePadded + 8 + 4 + 4 + 4) // fileid, name, cookie, two option flags, next
	if includeAttrs {
		size += 84 // fattr3
	}
	if handleLen >= 0 {
		handlePadded := (uint32(handleLen) + 3) &^ 3
		size += 4 + handlePadded // opaque length + padded handle bytes
	}
	return size
}
