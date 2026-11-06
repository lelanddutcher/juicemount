// Package manager — Permissions tab (Part 2/3).
//
// Three endpoints, matched to existing precedents:
//
//	GET     /api/permissions/inspect?path=    read-only owner/mode + a "writable
//	                                           by the mount client" verdict
//	POST    /api/permissions/fix              typed-confirm (X-Confirm-Fix)
//	                                           recursive chown+chmod remedy
//	GET/PUT /api/permissions/default-owner     the default migration owner (rule #0)
//
// Everything is confined to the JuiceFS volume via confineVolumePath, which
// cleans the path once, rewrites it, and refuses one that resolves outside
// the FUSE mount. Every endpoint requires embedded mode (a.fuseMount != "")
// because chown/chmod need a real filesystem to touch — standalone returns 501.
// Today's single default owner is rule #0 of a future per-path ACL list.
package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"
)

// ── DTOs ───────────────────────────────────────────────────────────────────

type permInspectResponse struct {
	Path             string `json:"path"` // user-facing /jfs/...
	Exists           bool   `json:"exists"`
	IsDir            bool   `json:"is_dir"`
	UID              int    `json:"uid"`
	GID              int    `json:"gid"`
	OwnerName        string `json:"owner_name"` // resolved from uid; "" if unknown
	Mode             string `json:"mode"`       // symbolic, e.g. "drwxr-xr-x"
	ModeOctal        string `json:"mode_octal"` // e.g. "0644"
	WritableByClient bool   `json:"writable_by_client"`
	ClientUID        int    `json:"client_uid"` // the default owner the verdict is against
	ClientGID        int    `json:"client_gid"`
	Reason           string `json:"reason,omitempty"`
}

type permFixRequest struct {
	Path      string `json:"path"`          // user-facing /jfs/...
	UID       *int   `json:"uid,omitempty"` // nil → the default owner
	GID       *int   `json:"gid,omitempty"`
	Recursive bool   `json:"recursive"`
}
type permFixResponse struct {
	Path string `json:"path"`
	UID  int    `json:"uid"`
	GID  int    `json:"gid"`
	Note string `json:"note,omitempty"`
}

type permOwnerResponse struct {
	UID       int `json:"uid"`
	GID       int `json:"gid"`
	ClientUID int `json:"client_uid"` // == uid; lets the UI drop its 501 placeholder
	ClientGID int `json:"client_gid"`
}
type permOwnerUpdateRequest struct {
	UID int `json:"uid"`
	GID int `json:"gid"`
}

// ── helpers (pure; unit-tested like chownArgs) ──────────────────────────────

// writableByClient decides whether the process the client mounts as
// (ownerUID/ownerGID) can WRITE the entry, returning the verdict + a short
// reason. gid < 0 (owner "leave group unchanged") disables the group arm;
// world-writable always wins.
func writableByClient(fi os.FileInfo, uid, gid, ownerUID, ownerGID int) (bool, string) {
	m := fi.Mode().Perm()
	if ownerUID > 0 && uid == ownerUID && m&0200 != 0 {
		return true, "client is the owner and owner has write"
	}
	if ownerGID >= 0 && gid == ownerGID && m&0020 != 0 {
		return true, "client is in the owning group and group has write"
	}
	if m&0002 != 0 {
		return true, "world-writable"
	}
	return false, fmt.Sprintf("owner %d:%d, mode %04o — not writable by client %d:%d",
		uid, gid, m, ownerUID, ownerGID)
}

// firstLine caps child output to its first (TrimSpace'd) line so an on-disk
// absolute path in a chown/chmod error can't leak in the HTTP body.
func firstLine(b []byte) string {
	s := strings.TrimSpace(string(b))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}

// ── GET /api/permissions/inspect?path= ──────────────────────────────────────

func (a *API) handlePermissionsInspect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if a.fuseMount == "" {
		http.Error(w, "permissions require embedded mode (FUSE mount)", http.StatusNotImplemented)
		return
	}
	userPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if userPath == "" {
		userPath = a.destMount
	}
	// GATE: clean once, rewrite, and confine where the kernel lands. A
	// symlink that leads out of the volume is refused. Only the cleaned
	// values are used below.
	vp, err := a.confineVolume(userPath, true)
	if err != nil {
		http.Error(w, "path outside the JuiceFS volume", http.StatusForbidden)
		return
	}
	userPath, real := vp.User, vp.Fuse
	ownerUID, ownerGID := a.jobs.MountOwner()

	fi, err := os.Lstat(real)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			writeJSON(w, http.StatusOK, permInspectResponse{
				Path: userPath, Exists: false, ClientUID: ownerUID, ClientGID: ownerGID,
			})
			return
		}
		log.Printf("manager: permissions inspect Lstat(%q): %v", real, err) // don't echo real path
		http.Error(w, "cannot stat path", http.StatusBadRequest)
		return
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		http.Error(w, "cannot read ownership", http.StatusInternalServerError)
		return
	}
	uid, gid := int(st.Uid), int(st.Gid)
	writable, reason := writableByClient(fi, uid, gid, ownerUID, ownerGID)
	ownerName := ""
	if u, e := user.LookupId(strconv.Itoa(uid)); e == nil {
		ownerName = u.Username
	}
	writeJSON(w, http.StatusOK, permInspectResponse{
		Path: userPath, Exists: true, IsDir: fi.IsDir(),
		UID: uid, GID: gid, OwnerName: ownerName,
		Mode: fi.Mode().String(), ModeOctal: fmt.Sprintf("%04o", fi.Mode().Perm()),
		WritableByClient: writable, ClientUID: ownerUID, ClientGID: ownerGID, Reason: reason,
	})
}

// ── POST /api/permissions/fix ───────────────────────────────────────────────
// chmod u+rwX,g+rwX,o+rX + chown <owner>, both confined to fuseMount. Mirrors
// applyPostSyncPermissions so the tab's one-shot remedy and the migration's
// post-sync fixup produce identical ownership.
func (a *API) handlePermissionsFix(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if a.fuseMount == "" {
		http.Error(w, "permissions require embedded mode (FUSE mount)", http.StatusNotImplemented)
		return
	}
	// Distinct header so an Empty-Trash confirmation can't be replayed here.
	if strings.ToLower(r.Header.Get("X-Confirm-Fix")) != "yes" {
		http.Error(w, "missing X-Confirm-Fix: yes header (typed confirmation required)", http.StatusPreconditionRequired)
		return
	}
	var req permFixRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Path) == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	// GATE: clean once, rewrite, and confine where the kernel lands. A
	// symlink that leads out of the volume is refused.
	vp, err := a.confineVolume(req.Path, true)
	if err != nil {
		http.Error(w, "path outside the JuiceFS volume", http.StatusForbidden)
		return
	}
	// The walk starts at the RESOLVED path, so the named entry is never
	// a symlink, and it follows no symlink found while recursing.
	real := vp.Real
	if vp.Fuse == strings.TrimSuffix(a.fuseMount, "/") || real == vp.MountReal {
		http.Error(w, "refusing to fix the entire volume root — name a subpath", http.StatusBadRequest)
		return
	}
	if isInsideTrash(vp.Fuse, a.fuseMount) || isInsideTrash(real, vp.MountReal) {
		http.Error(w, "refusing to fix inside .trash", http.StatusBadRequest)
		return
	}
	if _, err := os.Lstat(real); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			http.Error(w, "path not found", http.StatusNotFound)
			return
		}
		log.Printf("manager: permissions fix Lstat(%q): %v", real, err) // don't echo real path
		http.Error(w, "cannot stat path", http.StatusBadRequest)
		return
	}

	uid, gid := a.jobs.MountOwner()
	if req.UID != nil {
		uid = *req.UID
	}
	if req.GID != nil {
		gid = *req.GID
	}
	// Refuse uid <= 0: never chown to root.
	if chownArgs(uid, gid, real) == nil {
		http.Error(w, "refusing to chown: owner uid must be a non-root client uid (>0). Set the default owner first.", http.StatusBadRequest)
		return
	}
	// applyPermsBeneath opens the resolved path component by component
	// from the mount with O_NOFOLLOW and changes entries through their
	// fds, never following a symlink (see permwalk_linux.go).
	rel := strings.TrimPrefix(real, vp.MountReal)
	if err := applyPermsBeneath(r.Context(), vp.MountReal, rel, uid, gid, req.Recursive); err != nil {
		log.Printf("manager: permissions fix %q: %v", vp.User, err)
		http.Error(w, "permissions fix incomplete; see the Manager log", http.StatusInternalServerError)
		return
	}
	scope := "recursively"
	if !req.Recursive {
		scope = "on the named path only"
	}
	writeJSON(w, http.StatusOK, permFixResponse{
		Path: vp.User, UID: uid, GID: gid,
		Note: fmt.Sprintf("chmod u+rwX,g+rwX,o+rX + chown %s applied %s", chownSpec(uid, gid), scope),
	})
}

// ── GET/PUT /api/permissions/default-owner ──────────────────────────────────

func (a *API) handlePermissionsDefaultOwner(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		uid, gid := a.jobs.MountOwner()
		writeJSON(w, http.StatusOK, permOwnerResponse{UID: uid, GID: gid, ClientUID: uid, ClientGID: gid})
	case http.MethodPut, http.MethodPost:
		var req permOwnerUpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.UID <= 0 {
			http.Error(w, "uid must be a non-root client uid (>0)", http.StatusBadRequest)
			return
		}
		if req.GID < -1 {
			http.Error(w, "gid must be >= -1 (-1 = leave group unchanged)", http.StatusBadRequest)
			return
		}
		a.jobs.SetMountOwner(req.UID, req.GID) // mutates m.spec + persists
		writeJSON(w, http.StatusOK, permOwnerResponse{UID: req.UID, GID: req.GID, ClientUID: req.UID, ClientGID: req.GID})
	default:
		http.Error(w, "GET or PUT", http.StatusMethodNotAllowed)
	}
}
