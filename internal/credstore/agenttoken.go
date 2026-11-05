package credstore

// Agent tokens (continuity slice 6b). juicemount-mcp, the MCP server agents
// run on a Mac, keeps one agent token: a JSON record holding the token and
// the Manager it was minted on. Unlike the app's slots it is written here,
// by juicemount-mcp itself, in an item of its own (service
// AgentTokenService), so it never touches the items the app owns.
//
//   - darwin with cgo: one generic-password item in the login Keychain,
//     stamped with this Mac's device digest like the app's items, its label
//     naming the token's ID (AgentTokenLabel) so StoredAgentTokenID tells it
//     from the item's attributes, never its data. It is
//     created with the default access list, which trusts the creating
//     program (juicemount-mcp), so `serve` reads it with user interaction
//     disabled.
//   - darwin without cgo: no storage (refused); a token is never written to a
//     plain file on a Mac.
//   - elsewhere: a private 0600 file, credential-agent-token.json, in
//     os.UserConfigDir()/juicemount (a 0700 directory), in the isolated
//     entry format.
//   - an isolated acceptance root (internal/appisolation), any platform: the
//     same file inside the root.
//
// The app's Manager enrollment record (slot Enrollment) is what mints a
// token. The app mints from Settings (bridge agenttokens.go) and stores the
// token with AppAgentToken: the same item, but created with an access list
// that trusts the app and the bundled juicemount-mcp, so `serve` reads it
// without a prompt and the enrollment item is never opened to another
// program. `juicemount-mcp token create`, for a Mac without the app's
// Settings at hand, reads the enrollment with LoadEnrollment instead: the
// item's access list names the app and its service, not juicemount-mcp, so
// the read may show the macOS Keychain prompt (it is run by a person in a
// terminal).

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

const (
	// AgentTokenService is the Keychain service of juicemount-mcp's item.
	AgentTokenService = "com.juicemount.agent-token"
	// AgentToken is its slot (the Keychain account attribute, the file stem).
	AgentToken Slot = "agent-token"
	// agentTokenAccount is the item's fixed endpoint (generic attribute):
	// which Manager the token belongs to is in the record.
	agentTokenAccount = "juicemount-mcp"

	// Enrollment is the app's Manager enrollment record (EnrollmentRecord in
	// RedisCredentialStore.swift: enrollclient.State JSON), stored under the
	// fixed account EnrollmentAccount.
	Enrollment        Slot = "enrollment"
	EnrollmentAccount      = "manager-enrollment"
)

// ErrNoKeychain means this build cannot store a secret on this platform
// (a darwin build without cgo).
var ErrNoKeychain = errors.New("this build of juicemount-mcp has no Keychain support (built without cgo); use a release build")

// SaveAgentToken stores record (the token record JSON) for token id,
// replacing any.
func SaveAgentToken(record, id string) error {
	return saveAgentToken(record, id, func(device string) error {
		return keychainSave(AgentTokenService, string(AgentToken), agentTokenAccount, AgentTokenLabel(id), record, device, agentTokenUI)
	})
}

// saveAgentToken stores record in the agent token file when there is one,
// else with keychain, given this Mac's device digest.
func saveAgentToken(record, id string, keychain func(device string) error) error {
	if record == "" {
		return errors.New("empty agent token record")
	}
	device, _ := deviceid.Fingerprint()
	if path, ok, err := agentTokenFile(); err != nil {
		return err
	} else if ok {
		if !agentTokenIDRe.MatchString(id) {
			id = ""
		}
		return saveFile(path, isolatedEntry{Account: agentTokenAccount, Secret: record, Device: device, ID: id})
	}
	return keychain(device)
}

// agentTokenIDRe is an agent token's ID; agentTokenLabelRe the label that
// names one.
var (
	agentTokenIDRe    = regexp.MustCompile(`^[0-9a-f]{16}$`)
	agentTokenLabelRe = regexp.MustCompile(`^JuiceMount agent token ([0-9a-f]{16}) \(juicemount-mcp\)$`)
)

// AgentTokenLabel is the Keychain label of the item holding token id. It is
// not secret: it lets the ID be told (StoredAgentTokenID) without reading the
// token, which for an item another program made shows the Keychain's prompt.
func AgentTokenLabel(id string) string {
	if !agentTokenIDRe.MatchString(id) {
		return "JuiceMount agent token (juicemount-mcp)"
	}
	return "JuiceMount agent token " + id + " (juicemount-mcp)"
}

// agentTokenIDFromLabel is the ID a label names ("" for an item labelled
// before IDs were kept there).
func agentTokenIDFromLabel(label string) string {
	if m := agentTokenLabelRe.FindStringSubmatch(label); m != nil {
		return m[1]
	}
	return ""
}

// StoredAgentTokenID is the ID of the stored agent token, told from what is
// kept beside it (the Keychain item's label, the file's id), never from the
// token: on a Mac it reads the item's attributes only, which shows no
// prompt. "" with a nil error: none stored, or one stored before IDs were
// kept beside it (unknown). A token stamped for another Mac is
// ErrOtherDevice.
func StoredAgentTokenID() (string, error) {
	if path, ok, err := agentTokenFile(); err != nil {
		return "", err
	} else if ok {
		e, err := loadIsolatedEntry(path, agentTokenAccount)
		if errors.Is(err, ErrNotFound) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		return e.ID, nil
	}
	if !keychainAvailable {
		return "", ErrNoKeychain
	}
	label, stamp, err := keychainReadAttributes(AgentTokenService, string(AgentToken), agentTokenAccount)
	if errors.Is(err, ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if err := checkDevice(deviceFromComment(stamp)); err != nil {
		return "", err
	}
	return agentTokenIDFromLabel(label), nil
}

// LoadAgentToken returns the stored record (ErrNotFound when none; on a Mac
// build without cgo, ErrNoKeychain). It shows no UI unless
// AllowAgentTokenUI was called. A record stamped for another Mac is
// ErrOtherDevice.
func LoadAgentToken() (string, error) {
	if path, ok, err := agentTokenFile(); err != nil {
		return "", err
	} else if ok {
		return loadIsolated(path, agentTokenAccount)
	}
	read := keychainRead
	if agentTokenUI {
		read = keychainReadInteractive
	}
	return loadAgentTokenKeychain(read)
}

// loadAgentTokenKeychain reads the agent token item with read and checks
// its device stamp.
func loadAgentTokenKeychain(read func(service, slot, account string) (string, string, error)) (string, error) {
	if !keychainAvailable {
		return "", ErrNoKeychain
	}
	secret, stamp, err := read(AgentTokenService, string(AgentToken), agentTokenAccount)
	if err != nil {
		return "", err
	}
	if err := checkDevice(deviceFromComment(stamp)); err != nil {
		return "", err
	}
	return secret, nil
}

// DeleteAgentToken removes the stored record; none stored is not an error.
func DeleteAgentToken() error {
	return deleteAgentToken(func() error { return keychainDelete(AgentTokenService, string(AgentToken), agentTokenUI) })
}

// deleteAgentToken removes the agent token file when there is one, else
// with keychain.
func deleteAgentToken(keychain func() error) error {
	if path, ok, err := agentTokenFile(); err != nil {
		return err
	} else if ok {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	return keychain()
}

// AppAgentToken is the agent token as the JuiceMount app keeps it (bridge
// agenttokens.go): the item SaveAgentToken writes, with the same record and
// device stamp, in the same file off a Mac or in an isolated root. On a Mac,
// Save always creates the item afresh with an access list that trusts the
// app (the calling process) and Trusted, the bundled juicemount-mcp: that is
// the legacy keychain's access list, as the app's RedisCredentialStore makes
// for the service, because the data-protection keychain needs a
// keychain-access-groups entitlement, which needs a provisioning profile. A
// Developer ID juicemount-mcp in the app's team then reads it with user
// interaction off (`serve`); an ad-hoc build is refused (its partition is
// its own cdhash), as the service is for the app's items.
//
// None of its calls changes the process's Keychain user-interaction setting:
// it runs in the app, which may show the Keychain's prompt (for an item
// another program made) to the person using it.
type AppAgentToken struct {
	// Trusted are the executables besides the calling process that may read
	// the item without asking: the bundled juicemount-mcp.
	Trusted []string
}

// Save stores record for token id, replacing any.
func (a AppAgentToken) Save(record, id string) error {
	return saveAgentToken(record, id, func(device string) error {
		return keychainReplaceTrusting(AgentTokenService, string(AgentToken), agentTokenAccount, AgentTokenLabel(id), record, device, a.Trusted)
	})
}

// StoredID is StoredAgentTokenID: it never reads the token.
func (AppAgentToken) StoredID() (string, error) { return StoredAgentTokenID() }

// Load returns the stored record, as LoadAgentToken.
func (AppAgentToken) Load() (string, error) {
	if path, ok, err := agentTokenFile(); err != nil {
		return "", err
	} else if ok {
		return loadIsolated(path, agentTokenAccount)
	}
	return loadAgentTokenKeychain(keychainLoadAsIs)
}

// Delete removes the stored record; none stored is not an error.
func (AppAgentToken) Delete() error {
	return deleteAgentToken(func() error { return keychainDeleteAsIs(AgentTokenService, string(AgentToken)) })
}

// agentTokenUI: the agent token's Keychain calls may ask the person.
var agentTokenUI bool

// AllowAgentTokenUI lets the agent token's Keychain calls show the
// Keychain's prompts for the rest of the process: for juicemount-mcp's token
// commands, which a person runs in a terminal (after a rebuild or update,
// the Keychain asks before another build of the program may use the item).
// juicemount-mcp serve never calls it: it runs under an agent, with no one
// to answer.
func AllowAgentTokenUI() { agentTokenUI = true }

// LoadEnrollment returns the app's enrollment record (enrollclient.State
// JSON). On a Mac it may show the Keychain prompt: the item trusts the app
// and its service, and this is a person's command.
func LoadEnrollment() (string, error) {
	scope, isolated, err := appisolation.Current()
	if err != nil {
		return "", err
	}
	if isolated {
		return loadIsolated(filepath.Join(scope.Root, isolatedLeaf(Enrollment)), EnrollmentAccount)
	}
	secret, stamp, err := keychainReadInteractive(keychainService, string(Enrollment), EnrollmentAccount)
	if err != nil {
		return "", err
	}
	if err := checkDevice(deviceFromComment(stamp)); err != nil {
		return "", err
	}
	return secret, nil
}

// agentTokenFile is the file that holds the record when it is kept in a
// file (ok): inside an isolated root, or off darwin.
func agentTokenFile() (string, bool, error) {
	scope, isolated, err := appisolation.Current()
	if err != nil {
		return "", false, err
	}
	if isolated {
		return filepath.Join(scope.Root, isolatedLeaf(AgentToken)), true, nil
	}
	if !agentTokenInFile {
		return "", false, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", false, err
	}
	return filepath.Join(dir, "juicemount", isolatedLeaf(AgentToken)), true, nil
}

// keychainReadInteractive is the Keychain reader LoadEnrollment uses
// (tests substitute it).
var keychainReadInteractive = keychainLoadInteractive
