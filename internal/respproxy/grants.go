package respproxy

// grants.go: per-person write grants (Teams slice 1).
//
// The principals file (format version 2) names people: a stable ID, a
// display name, the Mac principals (devices) that belong to the person, an
// access mode and a list of granted folders. A person whose access is
// "granted" may write only inside granted folders; access "all" keeps the
// behaviour from before people existed. While grants are in force (any
// scoped person, or "restrict_unassigned") a Mac with no person may read
// but not write; otherwise it keeps the behaviour from before. Service
// principals (admin, bg, manager, farm, webdav) are never mapped to a
// person and keep their rights.
//
// A grant names a folder by path: a client folder (level 1, "/Clients") or
// a project (level 2, "/Clients/A"), or any folder down to the project
// depth. The proxy resolves the paths to directory inodes with side reads
// and re-resolves them after any change to the directories above the
// project depth. A write unit is admitted only if EVERY place it touches is
// covered:
//
//   - a project (by the lease engine's resolution of every anchor) is
//     covered when its root or a folder above it is granted;
//   - an entry created, renamed or removed directly in a folder above the
//     project depth (and the folder's own attributes) is covered when that
//     folder or one above it is granted, so creating a project needs a
//     grant on its client folder and creating a top-level folder is never
//     covered: only service principals and unscoped people may;
//   - /.juicemount/derivatives is shared and always covered;
//   - the trash rules of the lease engine hold (placeGrant): no write to a
//     trashed inode or inside a trashed directory, trash entries paired
//     with the removal, no purge; and a restore out of the trash also needs
//     a grant on the folder the entry was deleted from (its trash name
//     says which), so nobody takes another project's deleted files;
//   - a quota definition (a dirQuota row) needs a grant on its directory,
//     also above the project depth, and also when it rides along a write
//     that is otherwise covered; the usage counters above the project depth
//     (dir-stat flushes) stay open;
//   - a place the proxy cannot resolve is not covered (fail closed).
//
// The check runs in every proxy mode (passthrough and observe relax only
// leases) and refuses with NOT_GRANTED, which juicefs surfaces as EROFS.

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// PersonEntry is one person in a version-2 principals file:
//
//	{"id":"p-0123456789abcdef","name":"John","access":"granted",
//	 "grants":["/Clients/A"],"devices":["mac-0123456789ab"]}
type PersonEntry struct {
	ID      string   `json:"id"`
	Name    string   `json:"name,omitempty"`
	Access  string   `json:"access"` // AccessAll or AccessGranted
	Grants  []string `json:"grants,omitempty"`
	Devices []string `json:"devices,omitempty"`
}

// Person access modes.
const (
	AccessAll     = "all"     // writes everywhere (the default before grants)
	AccessGranted = "granted" // writes only inside Grants
)

// Person is the immutable, validated person of a principals table.
type Person struct {
	ID     string
	Name   string
	Scoped bool     // access "granted"
	Grants []string // clean absolute paths, sorted
}

var personIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

const (
	maxPeople       = 4096
	maxGrants       = 1024
	maxGrantDepth   = 16
	maxGrantNameLen = 255
)

// CleanGrantPath validates a grant path and returns its canonical form:
// absolute, components separated by single slashes, no trailing slash, no
// "." or "..", no control characters.
func CleanGrantPath(p string) (string, error) {
	if !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("grant %q: not an absolute path", p)
	}
	t := strings.TrimSuffix(p, "/")
	if t == "" {
		return "", fmt.Errorf("grant %q: the volume root cannot be granted", p)
	}
	parts := strings.Split(t[1:], "/")
	if len(parts) > maxGrantDepth {
		return "", fmt.Errorf("grant %q: deeper than %d folders", p, maxGrantDepth)
	}
	for _, c := range parts {
		switch {
		case c == "" || c == "." || c == "..":
			return "", fmt.Errorf("grant %q: empty, \".\" or \"..\" component", p)
		case len(c) > maxGrantNameLen:
			return "", fmt.Errorf("grant %q: a component is longer than %d bytes", p, maxGrantNameLen)
		}
		for i := 0; i < len(c); i++ {
			if c[i] < 0x20 || c[i] == 0x7f {
				return "", fmt.Errorf("grant %q: control character", p)
			}
		}
	}
	return t, nil
}

// parsePeople validates the people section against the principal entries.
func (ps *Principals) parsePeople(people []PersonEntry, entries []PrincipalEntry) error {
	if len(people) > maxPeople {
		return fmt.Errorf("principals: more than %d people", maxPeople)
	}
	kinds := map[string]Kind{}
	for _, e := range entries {
		kinds[e.Name] = e.Kind
	}
	ids := map[string]bool{}
	for i, pe := range people {
		if !personIDRe.MatchString(pe.ID) {
			return fmt.Errorf("people[%d]: bad id %q", i, pe.ID)
		}
		if ids[pe.ID] {
			return fmt.Errorf("people[%d]: duplicate id %q", i, pe.ID)
		}
		ids[pe.ID] = true
		if len(pe.Name) > 256 || strings.ContainsAny(pe.Name, "\r\n\x00") {
			return fmt.Errorf("people[%d] %s: bad name", i, pe.ID)
		}
		p := &Person{ID: pe.ID, Name: pe.Name}
		switch pe.Access {
		case AccessAll:
		case AccessGranted:
			p.Scoped = true
		default:
			return fmt.Errorf("people[%d] %s: access must be %q or %q", i, pe.ID, AccessAll, AccessGranted)
		}
		if len(pe.Grants) > maxGrants {
			return fmt.Errorf("people[%d] %s: more than %d grants", i, pe.ID, maxGrants)
		}
		seen := map[string]bool{}
		for _, g := range pe.Grants {
			c, err := CleanGrantPath(g)
			if err != nil {
				return fmt.Errorf("people[%d] %s: %w", i, pe.ID, err)
			}
			if !seen[c] {
				seen[c] = true
				p.Grants = append(p.Grants, c)
			}
		}
		sort.Strings(p.Grants)
		for _, d := range pe.Devices {
			k, known := kinds[d]
			switch {
			case known && k != KindMac:
				return fmt.Errorf("people[%d] %s: device %q is a %s principal; only Macs belong to people", i, pe.ID, d, k)
			case ps.people[d] != nil:
				return fmt.Errorf("people[%d] %s: device %q already belongs to %s", i, pe.ID, d, ps.people[d].ID)
			}
			if ps.people == nil {
				ps.people = map[string]*Person{}
			}
			ps.people[d] = p // a device not in the table is kept: harmless, and a later table may add it
		}
		ps.scoped = ps.scoped || p.Scoped
		ps.nPeople++
	}
	return nil
}

// PersonOf returns the person a principal belongs to (nil: none).
func (ps *Principals) PersonOf(name string) *Person {
	if ps == nil {
		return nil
	}
	return ps.people[name]
}

// GrantsInForce reports whether grants are in force: a person's writes
// are limited to grants, or the file says so (restrict_unassigned, which
// the Manager keeps on once grants were used, so deleting the last scoped
// person does not lift the limit on the Macs left without a person). While
// grants are in force a Mac with no person may read but not write.
func (ps *Principals) GrantsInForce() bool { return ps != nil && (ps.scoped || ps.restrict) }

// HasPeople reports whether the table names any person.
func (ps *Principals) HasPeople() bool { return ps != nil && ps.nPeople > 0 }

// grantsApply: the unit of principal p is checked against p's grants (a
// scoped person), or refused outright (a Mac with no person while grants
// are in force).
func grantsApply(p Principal, u *Unit) bool {
	return p.Kind == KindMac && ((u.Person != nil && u.Person.Scoped) || u.unassigned)
}

// grantCache maps grant paths to directory inodes. It is valid for one
// ancestry generation and one root-namespace epoch: a committed (or
// possibly committed) unit that changes an entry above the project depth
// bumps the epoch, so a renamed, removed or newly created folder is looked
// up again before the next decision that needs it.
type grantCache struct {
	epoch atomic.Uint64

	mu   sync.Mutex
	key  [2]uint64
	dirs map[string]uint64 // path -> directory inode; 0 = no such directory
}

// grantedDirs resolves grants (paths) to directory inodes.
func (e *LeaseEngine) grantedDirs(grants []string) (map[uint64]bool, error) {
	gc := &e.grants
	key := [2]uint64{e.anc.memo.generation(), gc.epoch.Load()}
	out := make(map[uint64]bool, len(grants))
	gc.mu.Lock()
	if gc.key != key || gc.dirs == nil {
		gc.key, gc.dirs = key, map[string]uint64{}
	}
	var missing []string
	for _, g := range grants {
		if ino, ok := gc.dirs[g]; ok {
			if ino != 0 {
				out[ino] = true
			}
			continue
		}
		missing = append(missing, g)
	}
	gc.mu.Unlock()
	for _, g := range missing {
		ino, err := e.lookupPath(g)
		if err != nil {
			return nil, err
		}
		gc.mu.Lock()
		if gc.key == key {
			gc.dirs[g] = ino
		}
		gc.mu.Unlock()
		if ino != 0 {
			out[ino] = true
		}
	}
	return out, nil
}

// lookupPath walks path from the root with side reads. 0: no such
// directory (a missing component or a non-directory).
func (e *LeaseEngine) lookupPath(path string) (uint64, error) {
	dir := uint64(rootIno)
	for _, c := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		ino, typ, ok, err := e.anc.entryAt(dir, c)
		if err != nil {
			return 0, err
		}
		if !ok || typ != typeDir {
			return 0, nil
		}
		dir = ino
	}
	return dir, nil
}

// covered: directory d, or a directory above it, is granted.
func (e *LeaseEngine) covered(d uint64, granted map[uint64]bool) (bool, error) {
	for i := 0; i < 4096; i++ {
		if d == rootIno {
			return false, nil
		}
		if granted[d] {
			return true, nil
		}
		p, err := e.anc.parentOfDir(d)
		if err != nil {
			return false, err
		}
		d = p
	}
	return false, errUnresolved
}

// grantCheck refuses a unit of a scoped person, or of a Mac with no person
// while grants are in force, that touches anything the grants do not cover
// (see the file comment). It is called with the topology lock held, before
// the lease rules, in every mode.
func (e *LeaseEngine) grantCheck(p Principal, u *Unit, f *unitFacts) *Denial {
	person := u.Person
	if person == nil {
		return &Denial{Reason: ReasonNotGranted,
			Detail: "Mac " + p.Name + " belongs to no person; while grants are in force such a Mac may read but not write (assign it to a person in the Manager)"}
	}
	deny := func(proj uint64, detail string) *Denial {
		return &Denial{Reason: ReasonNotGranted, Proj: proj,
			Detail: "person " + person.ID + " (" + p.Name + "): " + detail}
	}
	if len(f.unresolvable) > 0 {
		return deny(0, "write the proxy cannot place: "+f.unresolvable[0])
	}
	granted, err := e.grantedDirs(person.Grants)
	if err != nil {
		return deny(0, "grant lookup failed: "+err.Error())
	}
	// dirCovered checks a directory against the grants.
	dirCovered := func(d uint64, what string) *Denial {
		ok, err := e.covered(d, granted)
		switch {
		case err != nil:
			return deny(d, "cannot resolve directory "+inoStr(d)+": "+err.Error())
		case !ok:
			if lbl := e.anc.label(d); lbl != "" {
				what += " (" + lbl + ")"
			}
			return deny(d, what+" is not granted")
		}
		return nil
	}
	// scope checks one place: a project root, or a directory above the
	// project depth whose entries or attributes the unit writes.
	scope := func(loc location) *Denial {
		switch loc.kind {
		case nsDerivatives:
			return nil
		case nsTrash:
			// Inside a trashed directory: no Mac writes there (the
			// lease engine refuses it as BG_ONLY in enforce).
			return deny(0, "write inside a trashed directory")
		case nsProject:
			return dirCovered(loc.proj, "project "+inoStr(loc.proj))
		case nsRoot:
			return dirCovered(loc.dir, "folder "+inoStr(loc.dir)+" above the project depth")
		}
		return deny(0, "write to an unresolved place")
	}
	// Quota definitions (dirQuota rows) need a grant on their directory,
	// also above the project depth (the usage counters there do not).
	for _, dir := range f.quota {
		loc, err := e.anc.resolveDir(dir)
		if err != nil {
			return deny(0, "quota row of an unresolvable directory "+inoStr(dir))
		}
		if loc.kind == nsRoot {
			if d := dirCovered(dir, "the quota of folder "+inoStr(dir)); d != nil {
				return d
			}
			continue
		}
		if d := scope(loc); d != nil {
			return d
		}
	}
	if f.dirStatOnly() {
		for _, dir := range f.dirStat {
			loc, err := e.anc.resolveDir(dir)
			if err != nil {
				return deny(0, "dir-stat row of an unresolvable directory "+inoStr(dir))
			}
			if loc.kind == nsRoot {
				// Usage counters of a folder above the project depth
				// (the lease engine also leaves them open); quota rows
				// were checked above.
				continue
			}
			if d := scope(loc); d != nil {
				return d
			}
		}
		return nil
	}
	locs, pd := e.placeUnit(u, f, placeGrant)
	if pd != nil {
		return deny(pd.Proj, "write the proxy cannot place: "+pd.Detail)
	}
	for _, l := range locs {
		if d := scope(l); d != nil {
			if u.wantAudit {
				u.audit = e.describe(f, locs) // the refusal's record names every project
			}
			return d
		}
	}
	// A restore out of the trash: placeUnit admitted it (the entry goes
	// back outside the trash, which scope covered), and the folder the
	// entry was deleted from must be granted too, or a person could take
	// another project's deleted files into their own.
	for _, op := range f.dentries {
		if !op.trash || !op.del {
			continue
		}
		parent, ok := trashEntryParent(op.name)
		if !ok {
			return deny(0, "restore of trash entry "+strconv.Quote(op.name)+": its original folder is not known")
		}
		if d := dirCovered(parent, "folder "+inoStr(parent)+" the restored entry was deleted from"); d != nil {
			return d
		}
	}
	return nil
}

// trashEntryParent decodes the folder a trash entry was deleted from:
// juicefs names it "<parent>-<inode>-<name>" (pkg/meta/base.go trashEntry).
func trashEntryParent(name string) (uint64, bool) {
	i := strings.IndexByte(name, '-')
	if i <= 0 {
		return 0, false
	}
	j := strings.IndexByte(name[i+1:], '-')
	if j <= 0 {
		return 0, false
	}
	if _, ok := digits(name[i+1 : i+1+j]); !ok {
		return 0, false
	}
	p, ok := digits(name[:i])
	return p, ok && p != 0
}

// touchesRoot: the unit changes an entry in a directory above the project
// depth, so a folder a grant names may be created, renamed or removed by
// it. Its Done bumps the grant cache epoch if it committed or may have.
func (e *LeaseEngine) touchesRoot(f *unitFacts) bool {
	for _, op := range f.dentries {
		if op.trash {
			continue
		}
		loc, err := e.anc.resolveDir(op.dir)
		if err != nil || loc.kind == nsRoot {
			return true
		}
	}
	return false
}
