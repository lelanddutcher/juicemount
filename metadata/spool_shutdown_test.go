package metadata

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newQuiescentMetadataFixture(t *testing.T) (*Store, *SpoolStore, QuiescentSpoolSnapshot, []byte) {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	sp := NewSpoolStore(s.DB())
	s.SetSpoolStore(sp)
	body := []byte("all received bytes")
	file := filepath.Join(t.TempDir(), "image")
	if err := os.WriteFile(file, body, 0600); err != nil {
		t.Fatal(err)
	}
	id, err := sp.Insert("image.bin", file)
	if err != nil {
		t.Fatal(err)
	}
	const inode = uint64(1<<63 | 91)
	if err := s.Insert(MakeEntry("image.bin", false, 0, time.Now(), inode)); err != nil {
		t.Fatal(err)
	}
	q, err := sp.InspectQuiescentSpool(id, "image.bin", file, inode)
	if err != nil {
		t.Fatal(err)
	}
	return s, sp, q, body
}

func TestQuiescentReadyCASPreservesCustody(t *testing.T) {
	cases := map[string]string{
		"ready": "", "postcommit": "",
		"binding":              `INSERT INTO spool_bindings(spool_id,incarnation) SELECT id,'other' FROM spool_entries WHERE id=?`,
		"birth_tombstone":      `INSERT INTO spool_bindings(spool_id,incarnation,birth_id,birth_path,birth_file,birth_identity) SELECT id+100,'other','old-birth',nfs_path,spool_file,'1:2' FROM spool_entries WHERE id=?`,
		"retired":              `UPDATE spool_entries SET retired_by=id+100 WHERE id=?`,
		"clone":                `UPDATE spool_entries SET clone_hold='copied' WHERE id=?`,
		"qualification":        `UPDATE spool_entries SET qualification_hold='unknown' WHERE id=?`,
		"replacement":          `UPDATE spool_entries SET replacement_witness=x'01' WHERE id=?`,
		"publication_required": `UPDATE spool_entries SET replacement_publication_required=1 WHERE id=?`,
		"publication":          `INSERT INTO spool_replacement_publications(spool_id,operation_id,phase,payload) VALUES (?,'operation','queued',x'01')`,
		"completion":           `INSERT INTO spool_completion_holds(completion_id,spool_id,nfs_path,spool_file,size) SELECT 'completion',id,nfs_path,spool_file,size FROM spool_entries WHERE id=?`,
		"remote_custody":       `INSERT INTO spool_custody(operation_id,source_id,spool_id,spool_file,source_identity,store_id,incarnation,target_path,target_revision,epoch,phase,stage_path,target_eof) SELECT 'op','source',id,spool_file,'1:2','store','inc',nfs_path,1,1,'staged','stage',0 FROM spool_entries WHERE id=?`,
		"successor":            `INSERT INTO spool_entries(nfs_path,spool_file,size,drain_state,created_at,updated_at) SELECT nfs_path,spool_file||'.next',0,'writing',created_at,updated_at FROM spool_entries WHERE id=?`,
		"path":                 `UPDATE spool_entries SET nfs_path='moved.bin' WHERE id=?`,
		"file":                 `UPDATE spool_entries SET spool_file=spool_file||'.moved' WHERE id=?`,
		"size":                 `UPDATE spool_entries SET size=12 WHERE id=?`,
		"punched":              `UPDATE spool_entries SET punched_end=1 WHERE id=?`,
		"state":                `UPDATE spool_entries SET drain_state='failed' WHERE id=?`,
		"revision":             `UPDATE entries SET revision=revision+1 WHERE path=(SELECT nfs_path FROM spool_entries WHERE id=?)`,
		"inode":                `UPDATE entries SET inode=inode+1 WHERE path=(SELECT nfs_path FROM spool_entries WHERE id=?)`,
		"directory":            `UPDATE entries SET is_dir=1 WHERE path=(SELECT nfs_path FROM spool_entries WHERE id=?)`,
		"trigger_abort":        `CREATE TRIGGER refuse_quiescent BEFORE UPDATE OF drain_state ON spool_entries WHEN NEW.drain_state='ready' BEGIN SELECT RAISE(ABORT,'injected'); END`,
		"ram_inode":            "", "ram_version": "", "prepared": "", "bad_hash": "",
	}
	for kind, stmt := range cases {
		t.Run(kind, func(t *testing.T) {
			s, sp, q, body := newQuiescentMetadataFixture(t)
			if stmt != "" {
				var err error
				if kind == "trigger_abort" {
					_, err = s.DB().Exec(stmt)
				} else {
					_, err = s.DB().Exec(stmt, q.SpoolID)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "postcommit":
				sp.shutdownReadyPostCommit = func() error { return errors.New("lost reply") }
			case "ram_inode", "ram_version":
				s.mu.Lock()
				e := *s.pathCache[q.Path]
				if kind == "ram_inode" {
					e.Inode++
				} else {
					e.Version.Revision++
				}
				s.pathCache[q.Path] = &e
				s.mu.Unlock()
			case "prepared":
				s.mu.Lock()
				s.preparedCreates[q.Path] = PreparedCreate{Path: q.Path}
				s.mu.Unlock()
			}
			before, err := sp.Get(q.SpoolID)
			if err != nil {
				t.Fatal(err)
			}
			want := sha256.Sum256(body)
			digest := want[:]
			if kind == "bad_hash" {
				digest = digest[:3]
			}
			err = sp.MarkQuiescentReady(q, int64(len(body)), digest)
			row, readErr := sp.Get(q.SpoolID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if kind == "ready" || kind == "postcommit" {
				if kind == "ready" && err != nil {
					t.Fatal(err)
				}
				var pending *PendingSpoolFinalization
				if kind == "postcommit" && !errors.As(err, &pending) {
					t.Fatalf("not ambiguous: %v", err)
				}
				if row.DrainState != DrainReady || row.Size != int64(len(body)) || !bytes.Equal(row.SHA256, want[:]) {
					t.Fatalf("wrong commit: %+v", row)
				}
				// The stale snapshot cannot be replayed, even after a lost success reply.
				if err := sp.MarkQuiescentReady(q, int64(len(body)), want[:]); err == nil {
					t.Fatal("stale CAS replayed")
				}
			} else {
				if err == nil {
					t.Fatal("unsafe transition accepted")
				}
				if row.DrainState != before.DrainState || row.Size != before.Size || !bytes.Equal(row.SHA256, before.SHA256) {
					t.Fatalf("refusal mutated row: %+v", row)
				}
			}
			got, err := os.ReadFile(q.File)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatal("private image changed", err)
			}
		})
	}
}
