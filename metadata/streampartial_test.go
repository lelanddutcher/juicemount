package metadata

import "testing"

// A streamed partial must never reach the mirror.
//
// Unlike .trash / .juicemount, an in-flight streamed destination is a REAL
// JuiceFS file with a root-resolvable dentry, so the authoritative SCAN returns
// it and keyspace push reports it. Mirrored, it would appear in Finder as a real
// file and publish its half-written size as authoritative — and a derivative
// generator could key a thumbnail off half a clip.
func TestMirrorRejectsStreamedPartials(t *testing.T) {
	s := newTestStore(t)
	partial := &Entry{
		Path:       "DCIM/A001/.juicemount-streaming-42-clip.mov",
		Name:       ".juicemount-streaming-42-clip.mov",
		ParentPath: "DCIM/A001",
		Size:       12345,
		Inode:      9001,
	}
	s.InsertToCache(partial)
	if got := s.LookupByPath(partial.Path); got != nil {
		t.Error("a streamed partial entered the RAM mirror — it will list in Finder " +
			"as a real file and publish its partial size as authoritative")
	}
	if err := s.Insert(partial); err != nil {
		t.Fatalf("Insert returned an error rather than silently skipping: %v", err)
	}
	if got := s.LookupByPath(partial.Path); got != nil {
		t.Error("a streamed partial reached the mirror via Insert")
	}
}

// The filter must be NARROW. Rejecting ordinary content would hide real files
// from every listing — a quieter and worse bug than the one being fixed.
func TestMirrorAcceptsOrdinaryFilesIncludingSimilarNames(t *testing.T) {
	s := newTestStore(t)
	for _, name := range []string{
		"clip.mov",
		"._clip.mov",
		".DS_Store",
		".juicemount",                  // the derivative namespace root itself
		"juicemount-streaming-1-a.mov", // no leading dot: a real user file
		".juicemount-streaming-",       // prefix with NO payload: not a partial
	} {
		e := &Entry{Path: "DCIM/" + name, Name: name, ParentPath: "DCIM", Inode: 1}
		s.InsertToCache(e)
		if got := s.LookupByPath(e.Path); got == nil {
			t.Errorf("ordinary entry %q was rejected as a streamed partial — real "+
				"content would vanish from listings", name)
		}
	}
}

// The name predicate itself, at the boundaries.
func TestStreamPartialName(t *testing.T) {
	yes := []string{
		".juicemount-streaming-1-clip.mov",
		".juicemount-streaming-999999-a",
	}
	no := []string{
		"",
		"clip.mov",
		".juicemount",
		".juicemount-streaming-", // exactly the prefix, nothing after it
		"x.juicemount-streaming-1-clip.mov",
	}
	for _, n := range yes {
		if !StreamPartialName(n) {
			t.Errorf("StreamPartialName(%q) = false, want true", n)
		}
	}
	for _, n := range no {
		if StreamPartialName(n) {
			t.Errorf("StreamPartialName(%q) = true, want false", n)
		}
	}
}
