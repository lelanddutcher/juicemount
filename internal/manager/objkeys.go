package manager

// objkeys.go: per-Mac MinIO object keys (H-8 per-device design §4-§7).
//
// With JM_OBJ_PER_MAC=1 every approved Mac gets its own MinIO service
// account, mac-<12 hex>-<gen>, under the parent user jm-macs (canned policy
// jm-client: Get/Put/List, no Delete). Revoking the Mac removes it, so a
// revoked Mac's stored key stops working at MinIO, not only at Redis.
//
// The Manager holds no MinIO admin credential. It talks to the minio-keyd
// sidecar (server/minio/keyd.sh) through files in JM_OBJKEYS_DIR:
//
//	desired.json        Manager -> keyd, no secrets. Written atomically
//	                    (temporary file + rename in the same directory),
//	                    compact json.Marshal output, "keys" never null.
//	                    seq rises on every write, so a needs_secret request
//	                    always carries a seq higher than keyd's last create.
//	results/<name>.json keyd -> Manager, one secret per created key. The
//	                    Manager persists the secret in its store (0600),
//	                    rewrites desired.json without needs_secret, and only
//	                    then deletes the file.
//	applied.json        keyd -> Manager, what MinIO reports. A key is ready
//	                    only when MinIO lists it, on, under jm-macs, with the
//	                    parent's policy (implied_policy), and the store holds
//	                    its secret.
//
// Invariant (per-Mac mode on): a Mac the Manager approves has an active key.
// Approve waits for the key and refuses (409) when keyd does not deliver.
// Auto-approval cannot wait (Enroll holds the store lock), so the record
// stays pending with AutoApprovePending until the reconciler sees the key.
// Macs approved before the switch keep the shared jm-client key they
// already hold (their poll answers carry no key) until their own key is
// active (rollout step M2 backfill).
//
// Names are never reused: the highest reserved generation per principal is
// kept in the store (ObjKeyGens), survives Forget, and rises past any
// generation keyd reports. It is written ahead to manager-gens.json in the
// keyd directory, so a store restored from an older backup still mints past
// every reserved generation. An unused skipped generation is safe. A key the store holds that
// keyd no longer has (removed in a timeline the restored store never saw,
// or by hand) is retired and replaced by a new generation, never re-created
// under its old name.
//
// Orphan keys: keys keyd reports under jm-macs that the store does not
// know (a lost or restored store, or a key made by hand). The Manager keeps
// them in desired.json, so keyd neither removes them nor refuses the file
// for a mass delete, and lists them on the Principals page. Only the
// admin's "Remove orphan keys" action (RemoveOrphanObjKeys) drops them; it
// and the Manager's own revokes and retirements are the only removals that
// may carry allow_mass_delete.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	objKeysParent     = "jm-macs"
	objKeyDesiredName = "desired.json"
	objKeyAppliedName = "applied.json"
	objKeyResultsDir  = "results"
	// objKeyGensName: the Manager's own record of the highest generation it
	// ever asked keyd for, per principal. keyd ignores it.
	objKeyGensName = "manager-gens.json"
	objKeyMaxGen   = 999

	// ObjKeyMinting: the Manager asked keyd for the key and does not yet
	// hold a verified key with its secret.
	ObjKeyMinting = "minting"
	// ObjKeyActive: MinIO reports the key (on, parent jm-macs, parent's
	// policy) and the store holds its secret. Only an active key is
	// delivered to a Mac.
	ObjKeyActive = "active"
)

var (
	// keyd's NAME_RE: gen 1-999, no leading zero.
	objKeyNameRe = regexp.MustCompile(`^mac-[0-9a-f]{12}-[1-9][0-9]{0,2}$`)
	// keyd accepts MinIO-generated secrets of this form (they may contain
	// '+' and '/').
	objKeySecretRe = regexp.MustCompile(`^[A-Za-z0-9+/]{32,40}$`)
)

// Timing (variables for tests).
var (
	// objKeyWait bounds how long Approve waits for keyd (design §5: 10 s,
	// five keyd cycles).
	objKeyWait = 10 * time.Second
	// objKeyPollEvery: how often Approve re-reads keyd's files while waiting.
	objKeyPollEvery = 100 * time.Millisecond
	// objKeyReconcileEvery: the background reconciler's period (keyd's own).
	objKeyReconcileEvery = 2 * time.Second
	// objKeydStaleAfter: applied.json older than this means keyd stopped
	// (it rewrites it every cycle).
	objKeydStaleAfter = 30 * time.Second
	// onObjKeyReconcilerStart, when set, receives the stop function of
	// every reconciler registerH8B starts (tests stop them).
	onObjKeyReconcilerStart func(stop func())
)

var (
	// ErrObjectKeyNotReady: Approve could not get the Mac's storage key from
	// minio-keyd in time. The Mac stays pending.
	ErrObjectKeyNotReady = errors.New("the storage key could not be created")
	// ErrObjectKeyRemovalPending: Forget of a Mac whose storage key keyd has
	// not yet confirmed removed.
	ErrObjectKeyRemovalPending = fmt.Errorf("%w: storage key removal not confirmed", ErrPrincipalState)
	// ErrObjKeysOff: an orphan-key action while JM_OBJ_PER_MAC is not 1.
	ErrObjKeysOff = errors.New("per-Mac storage keys are off (JM_OBJ_PER_MAC is not 1)")
	// ErrNotOrphanKey: RemoveOrphanObjKeys named a key that is not a
	// current orphan.
	ErrNotOrphanKey = errors.New("not an orphan storage key")
)

// objKeysDesiredWriteError: the store was saved (the intent is durable) but
// manager-gens.json or desired.json could not be written. The reconciler
// writes desired.json again (desiredSeqOnDisk is behind the store's seq).
type objKeysDesiredWriteError struct{ err error }

func (e *objKeysDesiredWriteError) Error() string {
	return "desired.json not written (the store holds the change; retried): " + e.err.Error()
}
func (e *objKeysDesiredWriteError) Unwrap() error { return e.err }

// ObjKeyRemoval is a key the Manager stopped asking for. It stays on the
// record until keyd reports it gone (applied.json at Seq or later with no
// existing key of that name).
type ObjKeyRemoval struct {
	Name string `json:"name"`
	// Seq: the desired.json seq that first omitted the key (0 = not yet
	// written).
	Seq int64 `json:"seq,omitempty"`
}

// parsePerMacFlag reads JM_OBJ_PER_MAC: exactly "1" is on; "" and "0" are
// off; anything else is reported and treated as off.
func parsePerMacFlag(v string) bool {
	switch v {
	case "1":
		return true
	case "", "0":
		return false
	}
	log.Printf("manager: ERROR: JM_OBJ_PER_MAC=%q is not 1 or 0; per-Mac object keys stay off (approved Macs get the shared jm-client key)", sanitizeFlag(v))
	return false
}

func sanitizeFlag(v string) string {
	if len(v) > 16 {
		v = v[:16]
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return '?'
		}
		return r
	}, v)
}

// objKeysDesired is desired.json. Field order and names are keyd's.
type objKeysDesired struct {
	Seq             int64             `json:"seq"`
	StoreCount      int               `json:"store_count"`
	AllowMassDelete int64             `json:"allow_mass_delete,omitempty"`
	Keys            []objKeysDesiredK `json:"keys"`
	Remove          []string          `json:"remove,omitempty"`
}

type objKeysDesiredK struct {
	Name        string `json:"name"`
	State       string `json:"state"`
	NeedsSecret bool   `json:"needs_secret,omitempty"`
}

// objKeysApplied is applied.json.
type objKeysApplied struct {
	Seq        int64             `json:"seq"`
	VerifiedMS int64             `json:"verified_ms"`
	Error      string            `json:"error"`
	Keys       []objKeysAppliedK `json:"keys"`
}

type objKeysAppliedK struct {
	Name          string `json:"name"`
	Exists        bool   `json:"exists"`
	Status        string `json:"status"`
	Parent        string `json:"parent"`
	ImpliedPolicy bool   `json:"implied_policy"`
}

func (a *objKeysApplied) key(name string) (objKeysAppliedK, bool) {
	if a == nil {
		return objKeysAppliedK{}, false
	}
	for _, k := range a.Keys {
		if k.Name == name {
			return k, true
		}
	}
	return objKeysAppliedK{}, false
}

// exists: MinIO lists the key.
func (a *objKeysApplied) exists(name string) bool {
	k, ok := a.key(name)
	return ok && k.Exists
}

// ready: MinIO lists the key, enabled, under jm-macs, with the parent's
// policy. Built by keyd from svcacct info, not from its own intent.
func (a *objKeysApplied) ready(name string) bool {
	k, ok := a.key(name)
	return ok && k.Exists && k.Status == "on" && k.Parent == objKeysParent && k.ImpliedPolicy
}

type objKeysResult struct {
	Name      string `json:"name"`
	SecretKey string `json:"secret_key"`
	CreatedMS int64  `json:"created_ms"`
}

// objKeys is the per-Mac key configuration of a PrincipalStore (nil = off).
type objKeys struct {
	dir string
	// seq: the last desired.json seq this store wrote (persisted).
	seq int64
	// gens: the highest generation reserved per principal (persisted;
	// survives Forget, so a name is never reused).
	gens map[string]int
	// gensFileBody: the last manager-gens.json body whose directory sync
	// succeeded. A visible rename after sync failure is retried.
	gensFileBody string
	// orphans: keys keyd last reported under jm-macs that the store does not
	// know (sorted; persisted, so a moment without applied.json never drops
	// them from desired.json).
	orphans []string
	// orphanRemoving: orphan keys the admin confirmed for removal, until
	// keyd reports them gone (persisted).
	orphanRemoving []ObjKeyRemoval
	// keyRefs: a stored current or retiring key is issuance evidence even
	// when a damaged store has no usable generation counter.
	keyRefs bool
}

// loadGensFile raises gens to manager-gens.json's values. A damaged file
// cannot safely be replaced from a store backup with a lower floor.
func (c *objKeys) loadGensFile() error {
	if c.dir == "" {
		return nil
	}
	for k, v := range c.gens {
		if !macNameRe.MatchString(k) || v < 1 || v > objKeyMaxGen {
			return fmt.Errorf("principal store has invalid storage-key generation for %q", k)
		}
	}
	b, err := os.ReadFile(c.path(objKeyGensName))
	if errors.Is(err, os.ErrNotExist) {
		if err := c.requireFreshGenerationHistory(); err != nil {
			return fmt.Errorf("%s missing: %w", objKeyGensName, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read %s: %w", objKeyGensName, err)
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	if t, e := d.Token(); e != nil || t != json.Delim('{') {
		return fmt.Errorf("%s: expected object", objKeyGensName)
	}
	if !d.More() {
		return fmt.Errorf("%s: missing gens", objKeyGensName)
	}
	if t, e := d.Token(); e != nil || t != "gens" {
		return fmt.Errorf("%s: expected gens", objKeyGensName)
	}
	if t, e := d.Token(); e != nil || t != json.Delim('{') {
		return fmt.Errorf("%s: gens must be object", objKeyGensName)
	}
	fileGens := map[string]int{}
	for d.More() {
		t, e := d.Token()
		k, ok := t.(string)
		if e != nil || !ok || !macNameRe.MatchString(k) || fileGens[k] != 0 {
			return fmt.Errorf("%s: invalid or duplicate name %q", objKeyGensName, k)
		}
		var v int
		if e := d.Decode(&v); e != nil || v < 1 || v > objKeyMaxGen {
			return fmt.Errorf("%s: invalid generation for %s", objKeyGensName, k)
		}
		fileGens[k] = v
	}
	if t, e := d.Token(); e != nil || t != json.Delim('}') {
		return fmt.Errorf("%s: incomplete gens", objKeyGensName)
	}
	if d.More() {
		return fmt.Errorf("%s: extra member", objKeyGensName)
	}
	if t, e := d.Token(); e != nil || t != json.Delim('}') {
		return fmt.Errorf("%s: incomplete file", objKeyGensName)
	}
	var extra any
	if e := d.Decode(&extra); !errors.Is(e, io.EOF) {
		return fmt.Errorf("%s: trailing or invalid data", objKeyGensName)
	}
	for k, v := range c.gens {
		if fileGens[k] < v {
			return fmt.Errorf("%s: missing or lower generation for %s (store has %d); refusing key minting", objKeyGensName, k, v)
		}
	}
	for k, v := range fileGens {
		if v > c.gens[k] {
			c.gens[k] = v
		}
	}
	return nil
}

// A missing floor is safe only before any key could have been issued. keyd
// may have started first and written an empty applied file/results directory.
func (c *objKeys) requireFreshGenerationHistory() error {
	if c.seq != 0 || len(c.gens) != 0 || c.keyRefs || len(c.orphans) != 0 || len(c.orphanRemoving) != 0 {
		return errors.New("the store records storage-key history; refusing key minting")
	}
	if _, err := os.Stat(c.path(objKeyDesiredName)); err == nil {
		return errors.New("desired.json exists; refusing key minting")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot inspect desired.json: %w", err)
	}
	if b, err := os.ReadFile(c.path(objKeyAppliedName)); err == nil {
		var a struct {
			Seq        int64              `json:"seq"`
			VerifiedMS int64              `json:"verified_ms"`
			Error      string             `json:"error"`
			Keys       *[]objKeysAppliedK `json:"keys"`
		}
		d := json.NewDecoder(strings.NewReader(string(b)))
		d.DisallowUnknownFields()
		var extra any
		if d.Decode(&a) != nil || d.Decode(&extra) != io.EOF || a.Seq != 0 || a.VerifiedMS <= 0 || a.Error != "" || a.Keys == nil || len(*a.Keys) != 0 {
			return errors.New("applied.json is not a valid empty first-run report; refusing key minting")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot inspect applied.json: %w", err)
	}
	if b, err := os.ReadFile(c.path("keyd-created-seq")); err == nil {
		if strings.TrimSpace(string(b)) != "" {
			return errors.New("keyd-created-seq records issuance; refusing key minting")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot inspect keyd-created-seq: %w", err)
	}
	if entries, err := os.ReadDir(c.path(objKeyResultsDir)); err == nil {
		if len(entries) != 0 {
			return errors.New("results directory contains key history; refusing key minting")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot inspect results directory: %w", err)
	}
	return nil
}

// writeGensFile confirms gens in manager-gens.json before a store save.
func (c *objKeys) writeGensFile() error {
	body, err := json.Marshal(struct {
		Gens map[string]int `json:"gens"`
	}{c.gens})
	if err != nil {
		return err
	}
	if string(body) == c.gensFileBody {
		return nil
	}
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return err
	}
	if err := writeFileAtomic0600Durable(c.path(objKeyGensName), append(body, '\n')); err != nil {
		return err
	}
	c.gensFileBody = string(body)
	return nil
}

func objKeyName(principal string, gen int) string { return principal + "-" + strconv.Itoa(gen) }

// splitObjKeyName returns the principal and generation of a key name.
func splitObjKeyName(name string) (string, int, bool) {
	if !objKeyNameRe.MatchString(name) {
		return "", 0, false
	}
	i := strings.LastIndexByte(name, '-')
	g, err := strconv.Atoi(name[i+1:])
	if err != nil {
		return "", 0, false
	}
	return name[:i], g, true
}

func (c *objKeys) path(name string) string { return filepath.Join(c.dir, name) }

// readApplied returns applied.json and its modification time; nil when
// absent or unreadable (keyd never wrote one, or it is mid-replace: keyd
// writes it atomically, so this is only "absent").
func (c *objKeys) readApplied() (*objKeysApplied, time.Time) {
	if c.dir == "" {
		return nil, time.Time{}
	}
	p := c.path(objKeyAppliedName)
	fi, err := os.Stat(p)
	if err != nil {
		return nil, time.Time{}
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, fi.ModTime()
	}
	var a objKeysApplied
	if err := json.Unmarshal(b, &a); err != nil {
		log.Printf("manager: objkeys: applied.json is not valid JSON: %v", err)
		return nil, fi.ModTime()
	}
	return &a, fi.ModTime()
}

// desiredSeqOnDisk is the seq of the desired.json on disk (0 if none).
func (c *objKeys) desiredSeqOnDisk() int64 {
	b, err := os.ReadFile(c.path(objKeyDesiredName))
	if err != nil {
		return 0
	}
	var d struct {
		Seq int64 `json:"seq"`
	}
	if json.Unmarshal(b, &d) != nil {
		return 0
	}
	return d.Seq
}

// objKeyResultFile is one results/<name>.json.
type objKeyResultFile struct {
	path   string
	name   string // from the file name
	secret string // "" when the file is unusable
}

// readResults lists results/. Every usable or corrupt file found is
// returned (a corrupt one with secret ""), so the caller deletes it after
// its decision is persisted. ok is false when the directory could not be
// listed; seen then holds nothing, and the caller must not conclude that a
// result is missing. seen also names files that exist but could not be
// read this time.
func (c *objKeys) readResults() (files []objKeyResultFile, seen map[string]bool, ok bool) {
	seen = map[string]bool{}
	if c.dir == "" {
		return nil, seen, false
	}
	dir := c.path(objKeyResultsDir)
	ents, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, seen, true // keyd has created no key yet
		}
		log.Printf("manager: objkeys: cannot list %s: %v", dir, err)
		return nil, seen, false
	}
	for _, e := range ents {
		fn := e.Name()
		if e.IsDir() || !strings.HasSuffix(fn, ".json") || strings.HasPrefix(fn, ".") {
			continue // keyd's temporary files are <name>.json.tmp.<pid>
		}
		name := strings.TrimSuffix(fn, ".json")
		if !objKeyNameRe.MatchString(name) {
			continue // not keyd's; leave it alone
		}
		seen[name] = true
		rf := objKeyResultFile{path: filepath.Join(dir, fn), name: name}
		b, err := os.ReadFile(rf.path)
		if err != nil {
			log.Printf("manager: objkeys: cannot read results/%s: %v", fn, err)
			continue // retried next cycle; not deleted
		}
		var r objKeysResult
		if json.Unmarshal(b, &r) == nil && r.Name == name && objKeySecretRe.MatchString(r.SecretKey) {
			rf.secret = r.SecretKey
		} else {
			log.Printf("manager: objkeys: results/%s is not a usable result; it is discarded and the key re-created", fn)
		}
		files = append(files, rf)
	}
	return files, seen, true
}

// ---- store operations (all with s.mu held) ---------------------------------

// startObjKeyMintLocked gives p (a copy that the caller commits) the next
// key generation. The name is new: higher than any generation the store
// or keyd has seen for this principal.
func (s *PrincipalStore) startObjKeyMintLocked(p *MacPrincipal) error {
	c := s.objk
	gen := c.gens[p.Name]
	if p.ObjKeyGen > gen {
		gen = p.ObjKeyGen
	}
	gen++
	if gen > objKeyMaxGen {
		return fmt.Errorf("%w: %s has used all %d storage key generations", ErrObjectKeyNotReady, p.Name, objKeyMaxGen)
	}
	// Raised now, even if the caller's commit fails: a skipped generation
	// is harmless, a reused one is not. saveStoreLocked writes this floor
	// ahead of any store record that can name the new generation.
	c.gens[p.Name] = gen
	p.ObjKeyGen = gen
	p.ObjKeyName = objKeyName(p.Name, gen)
	p.ObjKeyState = ObjKeyMinting
	p.ObjKeySecret = ""
	p.ObjKeyNeedsSecret = false
	p.ObjKeyMintSeq = 0
	return nil
}

// retireObjKey moves p's current key (if any) to the removal list and
// clears the secret. It works with per-Mac mode off too: a key that exists
// must be confirmed gone before Forget.
func retireObjKey(p *MacPrincipal) {
	if p.ObjKeyName != "" {
		p.ObjKeyRemoving = append(append([]ObjKeyRemoval(nil), p.ObjKeyRemoving...), ObjKeyRemoval{Name: p.ObjKeyName})
	}
	p.ObjKeyName, p.ObjKeySecret, p.ObjKeyState = "", "", ""
	p.ObjKeyNeedsSecret, p.ObjKeyMintSeq = false, 0
}

// objKeyDeliverable: the record holds a verified key and its secret.
func objKeyDeliverable(p *MacPrincipal) bool {
	return p != nil && p.ObjKeyState == ObjKeyActive && p.ObjKeyName != "" && p.ObjKeySecret != ""
}

// knownObjKeysLocked: every key name this store accounts for (current keys,
// keys being removed, confirmed orphan removals).
func (s *PrincipalStore) knownObjKeysLocked() map[string]bool {
	known := map[string]bool{}
	for _, r := range s.recs {
		if r.ObjKeyName != "" {
			known[r.ObjKeyName] = true
		}
		for _, rm := range r.ObjKeyRemoving {
			known[rm.Name] = true
		}
	}
	for _, rm := range s.objk.orphanRemoving {
		known[rm.Name] = true
	}
	return known
}

// refreshObjKeyOrphansLocked recomputes the orphan set from applied.json
// and reports whether it changed. Without applied.json the last set stays.
func (s *PrincipalStore) refreshObjKeyOrphansLocked(app *objKeysApplied) bool {
	c := s.objk
	if app == nil {
		return false
	}
	known := s.knownObjKeysLocked()
	seen := map[string]bool{}
	var out []string
	for _, k := range app.Keys {
		if k.Exists && objKeyNameRe.MatchString(k.Name) && !known[k.Name] && !seen[k.Name] {
			seen[k.Name] = true
			out = append(out, k.Name)
		}
	}
	sort.Strings(out)
	if strings.Join(out, " ") == strings.Join(c.orphans, " ") {
		return false
	}
	was := map[string]bool{}
	for _, n := range c.orphans {
		was[n] = true
	}
	for _, n := range out {
		if !was[n] {
			log.Printf("manager: objkeys: WARNING: minio-keyd reports %s, a storage key this Manager's store does not know (store lost or restored, or made by hand). It is kept, and listed on the Principals page until an admin removes it", n)
		}
	}
	c.orphans = out
	return true
}

// objKeyIntentUnwrittenLocked: some change is recorded in the store but no
// desired.json has carried it yet (a key to mint, a removal or an orphan
// removal without a seq).
func (s *PrincipalStore) objKeyIntentUnwrittenLocked() bool {
	for _, r := range s.recs {
		if r.ObjKeyName != "" && r.ObjKeyState == ObjKeyMinting && r.ObjKeyMintSeq == 0 {
			return true
		}
		for _, rm := range r.ObjKeyRemoving {
			if rm.Seq == 0 {
				return true
			}
		}
	}
	for _, rm := range s.objk.orphanRemoving {
		if rm.Seq == 0 {
			return true
		}
	}
	return false
}

// writeObjKeysDesiredLocked raises seq, stamps the records waiting for it,
// persists the store, then writes manager-gens.json and desired.json. The
// store is persisted first: a crash in between leaves a store whose
// generations and seq are already ahead, and the next reconcile rewrites
// desired.json from it. If the store cannot be saved, nothing changes (the
// stamps are undone, so the reconciler still sees the work as unwritten).
// If the store was saved but a file in the keyd directory was not, the
// error is an *objKeysDesiredWriteError and the reconciler retries.
func (s *PrincipalStore) writeObjKeysDesiredLocked(app *objKeysApplied) error {
	c := s.objk
	if c.dir == "" {
		return errors.New("JM_OBJKEYS_DIR is not set")
	}
	seq := c.seq
	if app != nil && app.Seq > seq {
		seq = app.Seq
	}
	if d := c.desiredSeqOnDisk(); d > seq {
		seq = d
	}
	seq++
	s.refreshObjKeyOrphansLocked(app)
	doc := objKeysDesired{Seq: seq, Keys: []objKeysDesiredK{}}
	removing := map[string]bool{}
	stamped := map[string]*MacPrincipal{} // originals of the records stamped below
	for _, name := range s.sortedNamesLocked() {
		r := s.recs[name]
		changed := false
		next := *r
		if next.ObjKeyName != "" && (next.ObjKeyState == ObjKeyMinting || next.ObjKeyState == ObjKeyActive) {
			doc.Keys = append(doc.Keys, objKeysDesiredK{Name: next.ObjKeyName, State: "active", NeedsSecret: next.ObjKeyNeedsSecret})
			if next.ObjKeyState == ObjKeyMinting && next.ObjKeyMintSeq == 0 {
				next.ObjKeyMintSeq = seq
				changed = true
			}
		}
		if len(next.ObjKeyRemoving) > 0 {
			rm := append([]ObjKeyRemoval(nil), next.ObjKeyRemoving...)
			for i := range rm {
				removing[rm[i].Name] = true
				if rm[i].Seq == 0 {
					rm[i].Seq = seq
					changed = true
				}
			}
			next.ObjKeyRemoving = rm
		}
		if changed {
			stamped[name] = r
			s.recs[name] = &next
		}
	}
	doc.StoreCount = len(doc.Keys)
	// Orphan keys stay wanted until the admin removes them: keyd must not
	// remove them, and must not refuse the whole file for a mass delete.
	for _, n := range c.orphans {
		doc.Keys = append(doc.Keys, objKeysDesiredK{Name: n, State: "active"})
	}
	prevOrphanRemoving := c.orphanRemoving
	if len(c.orphanRemoving) > 0 {
		rm := append([]ObjKeyRemoval(nil), c.orphanRemoving...)
		for i := range rm {
			removing[rm[i].Name] = true
			if rm[i].Seq == 0 {
				rm[i].Seq = seq
			}
		}
		c.orphanRemoving = rm
	}
	for name := range removing {
		doc.Remove = append(doc.Remove, name)
	}
	sort.Strings(doc.Remove)
	// keyd refuses to remove more than a few keys at once unless told to.
	// Confirm only named removals the store revoked or retired, or the
	// admin confirmed. keyd cannot delete an unlisted name with this flag.
	if app != nil {
		n := 0
		for _, k := range app.Keys {
			if k.Exists && removing[k.Name] {
				n++
			}
		}
		if n > 0 {
			doc.AllowMassDelete = seq
		}
	}
	prevSeq := c.seq
	c.seq = seq
	if err := s.saveStoreLocked(); err != nil {
		c.seq = prevSeq
		c.orphanRemoving = prevOrphanRemoving
		for name, r := range stamped {
			s.recs[name] = r
		}
		return err
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return &objKeysDesiredWriteError{err}
	}
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return &objKeysDesiredWriteError{err}
	}
	// manager-gens.json before desired.json: no name reaches keyd before the
	// record that it was used survives a store restore.
	if err := c.writeGensFile(); err != nil {
		return &objKeysDesiredWriteError{err}
	}
	if err := writeFileAtomic0600(c.path(objKeyDesiredName), append(body, '\n')); err != nil {
		return &objKeysDesiredWriteError{err}
	}
	return nil
}

func (s *PrincipalStore) sortedNamesLocked() []string {
	names := make([]string, 0, len(s.recs))
	for n := range s.recs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// reconcileObjKeysLocked reads keyd's files and advances every record:
// stores delivered secrets, asks keyd to re-create a key whose secret never
// arrived, marks verified keys active, approves auto-approved Macs whose key
// is active, mints for approved Macs that have no key (backfill), and drops
// removals keyd confirmed. forceDesired rewrites desired.json even when
// nothing changed (start-up: the store is the source of truth).
//
// Order of writes: proxy files (an auto-approval), store, desired.json, and
// only then the deletion of consumed results/ files. A failure before the
// deletion leaves the result to be read again.
func (s *PrincipalStore) reconcileObjKeysLocked(forceDesired bool) error {
	c := s.objk
	if c == nil {
		return nil
	}
	// applied.json BEFORE results/: keyd writes a key's result before the
	// applied.json that lists it, so a key listed here whose result is not
	// in results/ below really has no result (needs_secret is safe).
	if c.dir == "" {
		return errors.New("JM_OBJKEYS_DIR is not set")
	}
	app, _ := c.readApplied()
	results, resultFor, resultsOK := c.readResults()
	diskSeq := c.desiredSeqOnDisk()
	// behind: keyd applied, or desired.json holds, a seq this store never
	// wrote. The store was lost or restored from an older copy, so a key it
	// holds may have been delivered and removed in a timeline it never saw.
	behind := diskSeq > c.seq || (app != nil && app.Seq > c.seq)

	before := make(map[string]*MacPrincipal, len(s.recs))
	for k, v := range s.recs {
		before[k] = v
	}
	prevOrphans, prevOrphanRemoving := c.orphans, c.orphanRemoving
	storeDirty, desiredDirty, proxyChanged := false, forceDesired, false
	// A desired.json the store says it wrote is not on disk (the write
	// failed after the store was saved): write it again.
	if diskSeq < c.seq {
		desiredDirty = true
	}

	// 1. Never reuse a name keyd reports.
	if app != nil {
		for _, k := range app.Keys {
			if p, g, ok := splitObjKeyName(k.Name); ok && k.Exists && g > c.gens[p] {
				c.gens[p] = g
				storeDirty = true
			}
		}
	}

	// 2. Delivered secrets.
	for _, rf := range results {
		p, _, _ := splitObjKeyName(rf.name)
		r := s.recs[p]
		if r == nil || r.ObjKeyName != rf.name {
			continue // no longer wanted: keyd removes the key; the file is dropped
		}
		next := *r
		if rf.secret == "" {
			// Unusable result. If it answered a needs_secret request, clear
			// the flag (and the mint seq) in desired.json BEFORE the file
			// goes: keyd ignores a needs_secret still set when the result is
			// taken until it reads a desired.json without it
			// (KEYD_PROTOCOL.md). The request is made again, at a new seq,
			// once keyd has applied the cleared file.
			if next.ObjKeyNeedsSecret {
				next.ObjKeyNeedsSecret = false
				next.ObjKeyMintSeq = 0
				s.recs[p] = &next
				storeDirty, desiredDirty = true, true
			}
			continue
		}
		if next.ObjKeyState == ObjKeyActive && next.ObjKeySecret != "" && next.ObjKeySecret != rf.secret {
			// keyd created a key this Manager already delivered a second
			// time (it vanished from MinIO while desired.json named it). Its
			// name has now been used twice: replace it with a new generation.
			log.Printf("manager: objkeys: %s was created again by minio-keyd (it vanished from the object store); retiring it, a new generation replaces it", rf.name)
			retireObjKey(&next)
			s.recs[p] = &next
			storeDirty, desiredDirty = true, true
			continue
		}
		next.ObjKeySecret = rf.secret
		if next.ObjKeyNeedsSecret {
			next.ObjKeyNeedsSecret = false
			desiredDirty = true // desired.json loses needs_secret before the file goes
		}
		s.recs[p] = &next
		storeDirty = true
	}

	// 3. Per record.
	now := s.opts.Now()
	windowOpen := now.Before(s.autoUntil)
	for _, name := range s.sortedNamesLocked() {
		r := s.recs[name]
		next := *r
		changed := false
		if next.ObjKeyName != "" {
			// A generation the store has passed: keyd reports a newer one, or
			// manager-gens.json records one (the store was restored from an
			// older copy). Adopt it as the floor and mint past it; the newer
			// key the store holds no secret for stays an orphan until the
			// admin removes it.
			passed := c.gens[name] > next.ObjKeyGen
			// A key the store holds that keyd does not have: an active key
			// keyd once reported and no longer does (removed by hand, or
			// MinIO lost it), or any key of a store that is behind keyd. Its
			// name may already have been used, so it is never asked for again.
			gone := app != nil && !app.exists(next.ObjKeyName) &&
				(behind || (next.ObjKeyState == ObjKeyActive && next.ObjKeyMintSeq > 0 && app.Seq >= next.ObjKeyMintSeq))
			if passed || gone {
				log.Printf("manager: objkeys: %s: retiring %s (passed=%v gone=%v); a new generation replaces it", name, next.ObjKeyName, passed, gone)
				retireObjKey(&next)
				changed, desiredDirty = true, true
			}
		}
		// The auto-approve window closed (or expired) before this Mac's key
		// was ready: it now waits for an admin like any pending request.
		if next.Status == PrincipalPending && next.AutoApprovePending && !windowOpen {
			next.AutoApprovePending = false
			changed = true
			log.Printf("manager: principal %s: the auto-approve window closed before its storage key was ready; it now waits for an admin", name)
		}
		// Backfill: approved before per-Mac mode (or by a store that lost the
		// key), or a retired key above: mint. Until it is active the Mac
		// keeps the key it has.
		if next.ObjKeyName == "" && (next.Status == PrincipalApproved || (next.Status == PrincipalPending && next.AutoApprovePending)) {
			if err := s.startObjKeyMintLocked(&next); err != nil {
				log.Printf("manager: objkeys: %s: %v", name, err)
			} else {
				changed, desiredDirty = true, true
			}
		}
		if next.ObjKeyState == ObjKeyMinting {
			switch {
			case next.ObjKeySecret != "" && app.ready(next.ObjKeyName):
				next.ObjKeyState = ObjKeyActive
				changed = true
			case next.ObjKeySecret == "" && !next.ObjKeyNeedsSecret && next.ObjKeyMintSeq > 0 &&
				app != nil && app.Seq >= next.ObjKeyMintSeq && app.exists(next.ObjKeyName) && resultsOK && !resultFor[next.ObjKeyName]:
				// keyd created the key but its secret never arrived (keyd
				// stopped between create and result). It was never delivered:
				// ask keyd to re-create it, at a new seq.
				log.Printf("manager: objkeys: %s exists but no secret arrived; asking minio-keyd to re-create it", next.ObjKeyName)
				next.ObjKeyNeedsSecret = true
				next.ObjKeyMintSeq = 0
				changed, desiredDirty = true, true
			}
		}
		// (A closed or expired window cleared AutoApprovePending above.)
		if next.Status == PrincipalPending && next.AutoApprovePending && objKeyDeliverable(&next) {
			next.Status = PrincipalApproved
			next.AutoApproved = true
			next.AutoApprovePending = false
			next.DecidedAt = now.UnixMilli()
			changed, proxyChanged = true, true
			log.Printf("manager: principal %s auto-approved (storage key %s active)", name, next.ObjKeyName)
		}
		if len(next.ObjKeyRemoving) > 0 && app != nil {
			var keep []ObjKeyRemoval
			for _, rm := range next.ObjKeyRemoving {
				if rm.Seq > 0 && app.Seq >= rm.Seq && !app.exists(rm.Name) {
					log.Printf("manager: objkeys: %s removal confirmed by minio-keyd", rm.Name)
					changed = true
					continue
				}
				keep = append(keep, rm)
			}
			next.ObjKeyRemoving = keep
		}
		if changed {
			s.recs[name] = &next
			storeDirty = true
		}
	}

	// Orphan removals the admin confirmed: done when keyd reports them gone
	// at the seq that first omitted them.
	if len(c.orphanRemoving) > 0 && app != nil {
		var keep []ObjKeyRemoval
		for _, rm := range c.orphanRemoving {
			if rm.Seq > 0 && app.Seq >= rm.Seq && !app.exists(rm.Name) {
				log.Printf("manager: objkeys: orphan key %s removal confirmed by minio-keyd", rm.Name)
				storeDirty = true
				continue
			}
			keep = append(keep, rm)
		}
		c.orphanRemoving = keep
	}
	if s.refreshObjKeyOrphansLocked(app) {
		storeDirty, desiredDirty = true, true
	}
	// Recorded work no desired.json has carried yet (a Revoke whose
	// desired.json write failed, for example).
	if s.objKeyIntentUnwrittenLocked() {
		desiredDirty = true
	}

	undo := func() {
		s.recs = before
		// Retain raised floors even when the store decision rolls back.
		// writeGensFile may already have durably recorded them.
		c.orphans, c.orphanRemoving = prevOrphans, prevOrphanRemoving
	}
	if proxyChanged {
		if err := s.commitLocked(true); err != nil {
			undo()
			return err
		}
	} else if storeDirty && !desiredDirty {
		if err := s.saveStoreLocked(); err != nil {
			undo()
			return err
		}
	}
	if desiredDirty {
		if err := s.writeObjKeysDesiredLocked(app); err != nil {
			var dw *objKeysDesiredWriteError
			if proxyChanged || errors.As(err, &dw) {
				// The decisions are in the store (and the proxy); only
				// desired.json is behind, and the next cycle rewrites it.
				// The results are kept until then.
				return err
			}
			undo()
			return err
		}
	}
	// Every decision is persisted: drop the consumed results.
	for _, rf := range results {
		if err := os.Remove(rf.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("manager: objkeys: cannot delete %s: %v", rf.path, err)
		}
	}
	return nil
}

// syncObjKeys rewrites desired.json after a transition changed the key set.
func (s *PrincipalStore) syncObjKeys() error {
	if s.objk == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	app, _ := s.objk.readApplied()
	return s.writeObjKeysDesiredLocked(app)
}

// RemoveOrphanObjKeys is the admin's "Remove orphan keys" confirmation:
// each name must be an orphan keyd reports now (the list the admin saw).
// desired.json then omits them, with allow_mass_delete when keyd needs it.
// A Mac still using one of them loses object access until it has its own
// key. Returns nil once the decision is in the store (desired.json is
// retried by the reconciler if only its write failed).
func (s *PrincipalStore) RemoveOrphanObjKeys(names []string) error {
	if s.objk == nil {
		return ErrObjKeysOff
	}
	if len(names) == 0 {
		return fmt.Errorf("%w: no key named", ErrNotOrphanKey)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.objk
	app, _ := c.readApplied()
	s.refreshObjKeyOrphansLocked(app)
	cur := map[string]bool{}
	for _, n := range c.orphans {
		cur[n] = true
	}
	pick := map[string]bool{}
	for _, n := range names {
		if !cur[n] {
			return fmt.Errorf("%w: %s", ErrNotOrphanKey, n)
		}
		pick[n] = true
	}
	prevOrphans, prevRemoving := c.orphans, c.orphanRemoving
	var keep []string
	for _, n := range c.orphans {
		if !pick[n] {
			keep = append(keep, n)
		}
	}
	rm := append([]ObjKeyRemoval(nil), c.orphanRemoving...)
	for _, n := range c.orphans {
		if pick[n] {
			rm = append(rm, ObjKeyRemoval{Name: n})
		}
	}
	c.orphans, c.orphanRemoving = keep, rm
	if err := s.writeObjKeysDesiredLocked(app); err != nil {
		var dw *objKeysDesiredWriteError
		if errors.As(err, &dw) {
			log.Printf("manager: objkeys: orphan key removal recorded; %v", err)
			return nil
		}
		c.orphans, c.orphanRemoving = prevOrphans, prevRemoving
		return err
	}
	log.Printf("manager: objkeys: admin confirmed removal of orphan keys %s", strings.Join(names, ", "))
	return nil
}

// OrphanObjKey is one orphan key on the Principals page.
type OrphanObjKey struct {
	Name string `json:"name"`
	// Principal: the Mac the name belongs to; Known: the store has that
	// Mac's record (its row shows a red badge too).
	Principal string `json:"principal"`
	Known     bool   `json:"known"`
	// InUse: that Mac reports it is using this key (object_key_in_use).
	InUse bool `json:"in_use,omitempty"`
	// Removing: the admin confirmed the removal; keyd has not yet reported
	// the key gone.
	Removing bool `json:"removing,omitempty"`
}

// orphanObjKeysLocked lists the orphan keys and orphan removals.
func (s *PrincipalStore) orphanObjKeysLocked(orphans []string, removing []ObjKeyRemoval) []OrphanObjKey {
	var out []OrphanObjKey
	add := func(n string, rm bool) {
		p, _, _ := splitObjKeyName(n)
		r := s.recs[p]
		out = append(out, OrphanObjKey{Name: n, Principal: p, Known: r != nil, InUse: r != nil && r.ObjKeyInUse == n, Removing: rm})
	}
	for _, n := range orphans {
		add(n, false)
	}
	for _, rm := range removing {
		add(rm.Name, true)
	}
	return out
}

// ReconcileObjKeys runs one reconcile cycle (the background loop's step).
func (s *PrincipalStore) ReconcileObjKeys() error {
	if s.objk == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reconcileObjKeysLocked(false)
}

// StartObjKeyReconciler runs ReconcileObjKeys every objKeyReconcileEvery
// until stop is called. It is what auto-approves, backfills and confirms
// removals when no admin action is waiting.
func (s *PrincipalStore) StartObjKeyReconciler() (stop func()) {
	if s.objk == nil {
		return func() {}
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		t := time.NewTicker(objKeyReconcileEvery)
		defer t.Stop()
		lastErr := ""
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			if err := s.ReconcileObjKeys(); err != nil {
				if msg := err.Error(); msg != lastErr {
					log.Printf("manager: objkeys: reconcile: %v", err)
					lastErr = msg
				}
			} else {
				lastErr = ""
			}
		}
	}()
	var once bool
	return func() {
		if !once {
			once = true
			close(done)
			<-exited
		}
	}
}

// ensureObjKey makes sure the record has a key and waits for keyd to make
// it active. It returns ErrObjectKeyNotReady (with the reason) on timeout.
func (s *PrincipalStore) ensureObjKey(name string) error {
	s.mu.Lock()
	cur, ok := s.recs[name]
	if !ok {
		s.mu.Unlock()
		return ErrPrincipalNotFound
	}
	if cur.ObjKeyName == "" {
		next := *cur
		if err := s.startObjKeyMintLocked(&next); err != nil {
			s.mu.Unlock()
			return err
		}
		s.recs[name] = &next
		app, _ := s.objk.readApplied()
		if err := s.writeObjKeysDesiredLocked(app); err != nil {
			// keyd never saw the name (desired.json was not replaced), so
			// handing the generation back reuses nothing.
			s.recs[name] = cur
			// Keep the skipped generation: the write-ahead floor may
			// already be durable even if desired.json was not replaced.
			s.mu.Unlock()
			return fmt.Errorf("%w: desired.json could not be written: %v", ErrObjectKeyNotReady, err)
		}
		log.Printf("manager: objkeys: asked minio-keyd for %s (principal %s)", next.ObjKeyName, name)
	}
	keyName := s.recs[name].ObjKeyName
	s.mu.Unlock()

	deadline := time.Now().Add(objKeyWait)
	for {
		s.mu.Lock()
		err := s.reconcileObjKeysLocked(false)
		r := s.recs[name]
		ready := r != nil && r.ObjKeyName == keyName && objKeyDeliverable(r)
		gone := r == nil || r.ObjKeyName != keyName
		problem := s.objKeydProblemLocked()
		s.mu.Unlock()
		if err != nil {
			log.Printf("manager: objkeys: reconcile while approving %s: %v", name, err)
		}
		if ready {
			return nil
		}
		if gone {
			return fmt.Errorf("%w: the request changed while waiting (revoked or forgotten)", ErrObjectKeyNotReady)
		}
		if !time.Now().Before(deadline) {
			if problem == "" {
				problem = "minio-keyd did not create " + keyName + " in time"
			}
			return fmt.Errorf("%w: %s", ErrObjectKeyNotReady, problem)
		}
		time.Sleep(objKeyPollEvery)
	}
}

// objKeydProblemLocked describes why keyd is not delivering ("" when it
// looks healthy).
func (s *PrincipalStore) objKeydProblemLocked() string {
	c := s.objk
	if c == nil {
		return ""
	}
	if c.dir == "" {
		return "JM_OBJKEYS_DIR is not set, so the Manager cannot ask minio-keyd for keys"
	}
	app, at := c.readApplied()
	if at.IsZero() {
		return "minio-keyd not answering: it has written no applied.json in " + c.dir
	}
	if age := time.Since(at); age > objKeydStaleAfter {
		return fmt.Sprintf("minio-keyd not answering: applied.json was last written %s ago", age.Round(time.Second))
	}
	if app == nil {
		return "minio-keyd's applied.json could not be read"
	}
	if app.Error != "" {
		return "minio-keyd reports: " + app.Error
	}
	if app.Seq < c.seq {
		return fmt.Sprintf("minio-keyd has not yet applied request %d (it reports %d)", c.seq, app.Seq)
	}
	return ""
}

// objKeyStatusLocked is the Principals page's key column for p.
func (s *PrincipalStore) objKeyStatusLocked(p *MacPrincipal) string {
	switch {
	case len(p.ObjKeyRemoving) > 0 && p.ObjKeyName == "":
		return "removing"
	case p.ObjKeyState == ObjKeyActive:
		return "active"
	case p.ObjKeyNeedsSecret:
		return "recreating"
	case p.ObjKeyState == ObjKeyMinting:
		return "minting"
	case s.objk != nil && p.Status == PrincipalApproved:
		return "shared"
	}
	return ""
}
