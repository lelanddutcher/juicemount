package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// replacementImage owns the directory chain and exact private file for a local
// acceptance. The caller retains it through SQL settlement; no backend I/O is
// performed here. This is private-image custody, not a spool birth binding.
type replacementImage struct {
	root, files, file                         *os.File
	rootPath, name, rootID, filesID, identity string
	ownedFile                                 bool
}

func (p *replacementImage) Close() { _ = p.closeChecked() }

func (p *replacementImage) closeChecked() error {
	var err error
	if p.ownedFile && p.file != nil {
		err = errors.Join(err, p.file.Close())
		p.file = nil
	}
	if p.files != nil {
		err = errors.Join(err, p.files.Close())
		p.files = nil
	}
	if p.root != nil {
		err = errors.Join(err, p.root.Close())
		p.root = nil
	}
	return err
}

func (s *SpoolStore) openReplacementDirectories(name string) (_ *replacementImage, retErr error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name || filepath.Dir(name) != filepath.Join(s.root, SpoolFilesSubdir) {
		return nil, metadata.ErrMirrorConflict
	}
	p := &replacementImage{rootPath: s.root, name: filepath.Base(name)}
	defer func() {
		if retErr != nil {
			p.Close()
		}
	}()
	fd, err := unix.Open(s.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	p.root = os.NewFile(uintptr(fd), s.root)
	p.rootID, _, err = birthFDIdentity(p.root)
	if err != nil {
		return nil, err
	}
	p.files, err = birthOpenDirectoryAt(p.root, SpoolFilesSubdir)
	if err != nil {
		return nil, err
	}
	p.filesID, _, err = birthFDIdentity(p.files)
	if err != nil {
		return nil, err
	}
	return p, p.verifyDirectories()
}

func (s *SpoolStore) openReplacementImage(name string, current *os.File) (_ *replacementImage, retErr error) {
	p, err := s.openReplacementDirectories(name)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			p.Close()
		}
	}()
	p.file = current
	if p.file == nil {
		fd, err := unix.Openat(int(p.files.Fd()), p.name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		p.file = os.NewFile(uintptr(fd), name)
		p.ownedFile = true
	}
	p.identity, _, _, err = fdPhaseIdentity(p.file)
	if err != nil {
		return nil, err
	}
	if err = p.Verify(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *replacementImage) Verify() error {
	id, _, _, err := fdPhaseIdentity(p.file)
	if err != nil || id != p.identity {
		return metadata.ErrMirrorConflict
	}
	leaf, _, err := birthEntryIdentity(p.files, p.name)
	if err != nil || leaf != id {
		return metadata.ErrMirrorConflict
	}
	if err := p.verifyDirectories(); err != nil {
		return err
	}
	// Retain the final leaf recheck after directory validation: a replacement
	// during the named-chain check must not inherit this descriptor's proof.
	leaf, _, err = birthEntryIdentity(p.files, p.name)
	if err != nil || leaf != id {
		return metadata.ErrMirrorConflict
	}
	return nil
}

func (p *replacementImage) verifyDirectories() error {
	fd, err := unix.Open(p.rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	root := os.NewFile(uintptr(fd), p.rootPath)
	defer root.Close()
	rootID, _, err := birthFDIdentity(root)
	if err != nil || rootID != p.rootID {
		return metadata.ErrMirrorConflict
	}
	files, err := birthOpenDirectoryAt(root, SpoolFilesSubdir)
	if err != nil {
		return err
	}
	defer files.Close()
	filesID, _, err := birthFDIdentity(files)
	if err != nil || filesID != p.filesID {
		return metadata.ErrMirrorConflict
	}
	return nil
}

func (p *replacementImage) Seal() (metadata.SpoolReplacementImage, error) {
	var image metadata.SpoolReplacementImage
	if err := p.file.Sync(); err != nil {
		return image, err
	}
	if err := p.files.Sync(); err != nil {
		return image, err
	}
	if err := p.root.Sync(); err != nil {
		return image, err
	}
	return p.HashImage()
}

func (p *replacementImage) HashImage() (metadata.SpoolReplacementImage, error) {
	var image metadata.SpoolReplacementImage
	info, err := p.file.Stat()
	if err != nil {
		return image, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 {
		return image, metadata.ErrMirrorConflict
	}
	image.Size, image.MtimeNS = info.Size(), info.ModTime().UnixNano()
	hash := sha256.New()
	n, err := io.Copy(hash, io.NewSectionReader(p.file, 0, image.Size))
	if err != nil || n != image.Size {
		return image, fmt.Errorf("seal replacement source: %w", errors.Join(err, io.ErrUnexpectedEOF))
	}
	image.SHA256 = hash.Sum(nil)
	return image, p.VerifyImage(image)
}

func (p *replacementImage) VerifyImage(image metadata.SpoolReplacementImage) error {
	if err := p.Verify(); err != nil {
		return err
	}
	info, err := p.file.Stat()
	if err != nil || info.Size() != image.Size || info.ModTime().UnixNano() != image.MtimeNS {
		return metadata.ErrMirrorConflict
	}
	return nil
}

func (s *SpoolStore) openAcceptedReplacement(row *metadata.SpoolRow) (*replacementImage, *metadata.SpoolReplacementWitness, error) {
	w, err := s.meta.ReplacementWitness(row)
	if err != nil || w == nil {
		return nil, w, err
	}
	if row.RetiredBy != 0 || row.PunchedEnd != 0 || row.QualificationHold != "" || row.Size != w.Image.Size || !bytes.Equal(row.SHA256, w.Image.SHA256) {
		return nil, w, metadata.ErrMirrorConflict
	}
	p, err := s.openReplacementImage(row.SpoolFile, nil)
	if err != nil {
		return nil, w, err
	}
	if p.identity != w.SourceIdentity {
		p.Close()
		return nil, w, metadata.ErrMirrorConflict
	}
	if err = p.VerifyImage(w.Image); err != nil {
		p.Close()
		return nil, w, err
	}
	return p, w, nil
}

// Qualify active images and reconcile durable cleanup before capacity reset.
func (s *SpoolStore) checkReplacementRecovery(ctx context.Context, rows []*metadata.SpoolRow) error {
	for _, row := range rows {
		if row.RetiredBy != 0 {
			continue
		}
		w, err := s.meta.ReplacementWitness(row)
		if err != nil {
			return err
		}
		if w == nil {
			continue
		}
		journal, err := s.meta.ReplacementPublication(row)
		if err != nil {
			return fmt.Errorf("recover replacement publication %d: %w", row.ID, err)
		}
		phase := ""
		if journal != nil {
			phase = journal.Phase
		} // a legacy witness may await the supported lazy journal migration
		switch phase {
		case metadata.ReplacementPublicationCleaned:
			continue // historical evidence never authorizes pathname deletion
		case metadata.ReplacementPublicationCleaning:
			if err := s.finishReplacementCleanup(ctx, row, *journal); err != nil {
				return fmt.Errorf("recover replacement cleanup %d: %w", row.ID, err)
			}
			continue
		case metadata.ReplacementPublicationSettled:
			if row.DrainState != metadata.DrainDone {
				return metadata.ErrMirrorConflict
			}
		default:
			if row.DrainState != metadata.DrainReady {
				return fmt.Errorf("replacement row %d cannot leave queued state before conditional publication: %w", row.ID, syscall.EIO)
			}
		}
		p, _, err := s.openAcceptedReplacement(row)
		if err != nil {
			return fmt.Errorf("recover replacement %d: %w", row.ID, err)
		}
		image, sealErr := p.HashImage()
		p.Close()
		if sealErr != nil || !bytes.Equal(image.SHA256, w.Image.SHA256) {
			return fmt.Errorf("recover replacement %d: %w", row.ID, metadata.ErrMirrorConflict)
		}
		if phase == metadata.ReplacementPublicationSettled {
			// Serving uses the independently qualified resident route. The old
			// private image must never shadow a later mirror generation.
			continue
		}
		// A current acceptance requires its journal to survive with the same
		// source and mirror. Validate it before generic recovery can reset
		// capacity or dispose any files; this never contacts the backend.
		if _, err := s.meta.ReplacementPublication(row); err != nil {
			return fmt.Errorf("recover replacement publication %d: %w", row.ID, err)
		}
		owner := s.meta.Owner()
		if owner == nil {
			return metadata.ErrMirrorConflict
		}
		e := owner.LookupByPath(row.NFSPath)
		if e == nil || !e.LocalOnly || e.IsDir || e.Version.Incarnation != w.Source.Incarnation || e.Version.Revision < w.Source.Revision+1 || e.Size != w.Image.Size {
			return metadata.ErrMirrorConflict
		}
		if row.DrainState == metadata.DrainWriting {
			return fmt.Errorf("replacement checkpoint reopened without a successor: %w", syscall.EIO)
		}
	}
	return nil
}
