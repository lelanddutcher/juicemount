package metadata

import (
	"context"
	"testing"
	"time"
)

func TestCachedFileIdentityNeverWaitsForSQL(t *testing.T) {
	s := newTestStore(t)
	entry := MakeEntry("._movie", false, 4, time.Unix(123, 456), 42)
	entry.MtimeNsecKnown = true
	if err := s.Insert(entry); err != nil {
		t.Fatal(err)
	}
	s.db.SetMaxOpenConns(1)
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.writeMu.Lock()
	s.cachePublicationMu.Lock()
	done := make(chan bool, 1)
	go func() {
		id, ok := s.LookupCachedFileIdentity(entry.Path)
		_, absent := s.LookupCachedFileIdentity("absent")
		done <- ok && !absent && id.Inode == 42 && id.Publication != 0 && id.Version.Incarnation != ""
	}()
	var passed bool
	select {
	case passed = <-done:
	case <-time.After(300 * time.Millisecond):
	}
	s.cachePublicationMu.Unlock()
	s.writeMu.Unlock()
	conn.Close()
	if !passed {
		t.Fatal("RAM identity failed or waited on SQL/publication writer")
	}
}

func TestCachedFileIdentityRejectsUnversionedAndChangesOnPublication(t *testing.T) {
	s := newTestStore(t)
	e := MakeEntry("._movie", false, 4, time.Unix(123, 456), 42)
	e.MtimeNsecKnown = true
	s.InsertToCache(e)
	if _, ok := s.LookupCachedFileIdentity(e.Path); ok {
		t.Fatal("unversioned cache-only row accepted")
	}
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}
	first, ok := s.LookupCachedFileIdentity(e.Path)
	if !ok {
		t.Fatal("durable row absent")
	}
	e = s.LookupByPath(e.Path)
	e.Version = MirrorToken{}
	s.InsertToCache(e)
	if _, ok := s.LookupCachedFileIdentity(e.Path); ok {
		t.Fatal("cache-only publication retained a durable proof")
	}
	s.mu.RLock()
	secondPublication := s.pathCache[e.Path].cacheIdentity
	s.mu.RUnlock()
	if first.Publication == secondPublication {
		t.Fatal("same-value RAM publication retained identity")
	}
	// Restore a durable row before eviction, so SQL really could supply it.
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	delete(s.pathCache, e.Path)
	s.cacheEvicted.Store(true)
	s.mu.Unlock()
	if _, ok := s.LookupCachedFileIdentity(e.Path); ok {
		t.Fatal("evicted durable row fell back to SQL")
	}
}
