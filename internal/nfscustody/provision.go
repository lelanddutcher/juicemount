package nfscustody

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"golang.org/x/sys/unix"
)

const maxProvisionFrame = 4096

type provisionRequest struct {
	Version           int    `json:"version"`
	Mode              string `json:"mode"`
	Nonce             string `json:"nonce"`
	Boot              string `json:"boot"`
	Operation         string `json:"operation"`
	Path              string `json:"path"`
	UID               uint32 `json:"uid"`
	GID               uint32 `json:"gid"`
	ExpectedParentDev uint64 `json:"expected_parent_dev"`
	ExpectedParentIno uint64 `json:"expected_parent_ino"`
	ExpectedLeafDev   uint64 `json:"expected_leaf_dev"`
	ExpectedLeafIno   uint64 `json:"expected_leaf_ino"`
}
type provisionReceipt struct {
	Request    provisionRequest `json:"request"`
	State      string           `json:"state"`
	NoEffects  bool             `json:"no_effects"`
	ParentDev  uint64           `json:"parent_dev"`
	ParentIno  uint64           `json:"parent_ino"`
	LeafDev    uint64           `json:"leaf_dev"`
	LeafIno    uint64           `json:"leaf_ino"`
	BeforeUID  uint32           `json:"before_uid"`
	BeforeGID  uint32           `json:"before_gid"`
	AfterUID   uint32           `json:"after_uid"`
	AfterGID   uint32           `json:"after_gid"`
	BeforeMode uint32           `json:"before_mode"`
	AfterMode  uint32           `json:"after_mode"`
	Reason     string           `json:"reason,omitempty"`
}

func directVolumesLeaf(path string) bool {
	if filepath.Dir(path) != "/Volumes" || filepath.Clean(path) != path {
		return false
	}
	name := filepath.Base(path)
	if name == "" || name == "." || name == ".." || name == "Volumes" || strings.ContainsAny(name, "/\\\x00\n\r\t") || strings.IndexFunc(name, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return false
	}
	return true
}
func classifyProvision(path string) (mode string, parentDev, parentIno, leafDev, leafIno uint64, err error) {
	if !directVolumesLeaf(path) {
		return "", 0, 0, 0, 0, errors.New("administrator provisioning is limited to a direct /Volumes child")
	}
	parent, err := unix.Open("/Volumes", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", 0, 0, 0, 0, err
	}
	defer unix.Close(parent)
	var ps, named unix.Stat_t
	if err := unix.Fstat(parent, &ps); err != nil {
		return "", 0, 0, 0, 0, err
	}
	if err := unix.Lstat("/Volumes", &named); err != nil {
		return "", 0, 0, 0, 0, err
	}
	if ps.Dev != named.Dev || ps.Ino != named.Ino || ps.Uid != 0 || ps.Mode&0022 != 0 {
		return "", 0, 0, 0, 0, errors.New("/Volumes parent is not trusted")
	}
	parentDev, parentIno = uint64(ps.Dev), ps.Ino
	var st unix.Stat_t
	err = unix.Fstatat(parent, filepath.Base(path), &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return "create", parentDev, parentIno, 0, 0, nil
	}
	if err != nil {
		return "", 0, 0, 0, 0, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != 0 {
		return "", 0, 0, 0, 0, errors.New("migration requires an existing root-owned directory")
	}
	return "migrate", parentDev, parentIno, uint64(st.Dev), st.Ino, nil
}
func (r provisionRequest) valid() bool {
	return r.Version == 1 && (r.Mode == "create" || r.Mode == "migrate") && validNonce(r.Nonce) && validNonce(r.Operation) && r.Boot != "" && directVolumesLeaf(r.Path) && r.UID > 0 && r.UID != ^uint32(0) && r.GID != ^uint32(0) && r.ExpectedParentDev != 0 && r.ExpectedParentIno != 0 && (r.Mode == "create" && r.ExpectedLeafDev == 0 && r.ExpectedLeafIno == 0 || r.Mode == "migrate" && r.ExpectedLeafDev != 0 && r.ExpectedLeafIno != 0)
}
func sameRequest(a, b provisionRequest) bool { return a == b }

// ProvisionRun is an explicit one-shot mode in the bundled helper. A known
// refusal emits a terminal correlated frame and exits zero so osascript keeps
// the evidence; a partial effect emits uncertain and never permits mount.
func ProvisionRun(args []string, stdout, stderr io.Writer) int {
	return provisionRunWith(args, stdout, stderr, os.Geteuid(), nativeProvision)
}
func provisionRunWith(args []string, stdout, stderr io.Writer, euid int, provision func(provisionRequest) (provisionReceipt, bool, error)) int {
	if len(args) != 11 {
		fmt.Fprintln(stderr, "provision arguments invalid")
		return 64
	}
	uid, ue := strconv.ParseUint(args[5], 10, 32)
	gid, ge := strconv.ParseUint(args[6], 10, 32)
	pd, pde := strconv.ParseUint(args[7], 10, 64)
	pi, pie := strconv.ParseUint(args[8], 10, 64)
	ld, lde := strconv.ParseUint(args[9], 10, 64)
	li, lie := strconv.ParseUint(args[10], 10, 64)
	r := provisionRequest{Version: 1, Mode: args[0], Nonce: args[1], Boot: args[2], Operation: args[3], Path: args[4], UID: uint32(uid), GID: uint32(gid), ExpectedParentDev: pd, ExpectedParentIno: pi, ExpectedLeafDev: ld, ExpectedLeafIno: li}
	if ue != nil || ge != nil || pde != nil || pie != nil || lde != nil || lie != nil || !r.valid() || euid != 0 {
		fmt.Fprintln(stderr, "provision principal or request invalid")
		return 64
	}
	receipt, effect, err := provision(r)
	if err != nil {
		receipt.Request = r
		receipt.State = "refused"
		receipt.NoEffects = !effect
		if effect {
			receipt.State = "uncertain"
		}
		receipt.Reason = "provision preconditions or postconditions not satisfied"
	}
	data, marshalErr := json.Marshal(receipt)
	if marshalErr != nil || len(data)+1 > maxProvisionFrame {
		return 74
	}
	data = append(data, '\n')
	n, writeErr := stdout.Write(data)
	if writeErr != nil || n != len(data) {
		return 74
	}
	return 0
}

const provisionAppleScript = `on run argv
 if (count of argv) is not 12 then error "invalid provision request"
 set helperPath to item 1 of argv
 set targetPath to item 6 of argv
 set targetUID to item 7 of argv
 set actionName to item 2 of argv
 set commandText to quoted form of helperPath & " --nfs-provision-leaf"
 repeat with i from 2 to 12
   set commandText to commandText & " " & quoted form of (item i of argv)
 end repeat
 if actionName is "migrate" then
   set promptText to "JuiceMount requests permission to change ownership of the empty folder " & targetPath & " to local user ID " & targetUID
 else
   set promptText to "JuiceMount requests permission to create the folder " & targetPath & " for local user ID " & targetUID
 end if
 return do shell script commandText with administrator privileges with prompt promptText
end run`

func (c *Controller) provision(rec mountbroker.Record) error {
	mode, pd, pi, ld, li, err := classifyProvision(c.spec.MountPath)
	if err != nil {
		return err
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	req := provisionRequest{Version: 1, Mode: mode, Nonce: hex.EncodeToString(nonce), Boot: rec.BootID, Operation: rec.OperationID, Path: c.spec.MountPath, UID: c.targetUID, GID: c.targetGID, ExpectedParentDev: pd, ExpectedParentIno: pi, ExpectedLeafDev: ld, ExpectedLeafIno: li}
	if !req.valid() {
		return errors.New("invalid NFS provision request")
	}
	args := []string{"-e", provisionAppleScript, c.helper, req.Mode, req.Nonce, req.Boot, req.Operation, req.Path, strconv.FormatUint(uint64(req.UID), 10), strconv.FormatUint(uint64(req.GID), 10), strconv.FormatUint(req.ExpectedParentDev, 10), strconv.FormatUint(req.ExpectedParentIno, 10), strconv.FormatUint(req.ExpectedLeafDev, 10), strconv.FormatUint(req.ExpectedLeafIno, 10)}
	cmd := exec.Command("/usr/bin/osascript", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = os.Stderr
	c.mu.Lock()
	c.provisionAttempted = true
	c.provisionCmd = cmd
	c.provisionDone = make(chan struct{})
	done := c.provisionDone
	c.mu.Unlock()
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		c.mu.Lock()
		c.provisionStartErr = err
		c.provisionSettled = true
		c.mu.Unlock()
		close(done)
		return err
	}
	reader := bufio.NewReaderSize(stdout, maxProvisionFrame+1)
	raw, readErr := io.ReadAll(io.LimitReader(reader, maxProvisionFrame+1))
	_, drainErr := io.Copy(io.Discard, reader)
	waitErr := cmd.Wait()
	close(done)
	return c.finishProvision(req, raw, readErr, drainErr, waitErr)
}

// finishProvision retains bounded transport evidence even when it cannot be
// accepted. Only a validated successful receipt may authorize settlement.
func (c *Controller) finishProvision(req provisionRequest, raw []byte, readErr, drainErr, waitErr error) error {
	c.mu.Lock()
	c.provisionWaitErr = waitErr
	c.provisionOutput = append([]byte(nil), raw...)
	c.mu.Unlock()
	if readErr != nil || drainErr != nil || waitErr != nil || len(raw) == 0 || len(raw) > maxProvisionFrame {
		return errors.New("NFS administrator helper outcome uncertain; original operation retained")
	}
	reply, refused, err := parseProvisionReply(raw, req)
	if err != nil {
		return err
	}
	if refused {
		c.mu.Lock()
		c.provisionSettled = true
		c.mu.Unlock()
		return errors.New("NFS administrator helper refused exact directory change")
	}
	c.mu.Lock()
	c.provisionReceipt = &reply
	c.mu.Unlock()
	return c.settleProvision()
}
func parseProvisionReply(raw []byte, req provisionRequest) (provisionReceipt, bool, error) {
	var reply provisionReceipt
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reply); err != nil {
		return reply, false, errors.New("NFS administrator helper receipt malformed")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF || !sameRequest(reply.Request, req) {
		return reply, false, errors.New("NFS administrator helper receipt correlation or trailing output invalid")
	}
	if reply.State == "refused" {
		if !reply.NoEffects {
			return reply, false, errors.New("NFS administrator helper refusal has unproven effects")
		}
		return reply, true, nil
	}
	if reply.State != "created" && reply.State != "migrated" {
		return reply, false, errors.New("NFS administrator helper outcome uncertain")
	}
	if reply.State != map[string]string{"create": "created", "migrate": "migrated"}[req.Mode] || reply.NoEffects || reply.AfterUID != req.UID || reply.AfterGID != req.GID || reply.AfterMode&07777 != 0700 || reply.LeafDev == 0 || reply.LeafIno == 0 || reply.ParentDev != req.ExpectedParentDev || reply.ParentIno != req.ExpectedParentIno || req.Mode == "migrate" && (reply.LeafDev != req.ExpectedLeafDev || reply.LeafIno != req.ExpectedLeafIno) {
		return reply, false, errors.New("NFS administrator helper receipt invalid")
	}
	return reply, false, nil
}
func (c *Controller) settleProvision() error {
	c.mu.Lock()
	receipt := c.provisionReceipt
	c.mu.Unlock()
	if receipt == nil || receipt.State != "created" && receipt.State != "migrated" {
		return errors.New("NFS provision has no terminal successful original receipt")
	}
	if err := persistProvisionReceipt(c.creator.Directory, *receipt); err != nil {
		return err
	}
	snapshot, err := c.observe(context.Background())
	if err != nil {
		return err
	}
	if snapshot.Version != 1 || snapshot.BootID != receipt.Request.Boot || snapshot.Entries == nil {
		return errors.New("NFS provision snapshot unavailable or boot changed")
	}
	for _, entry := range snapshot.Entries {
		if entry.Path == receipt.Request.Path || strings.HasPrefix(entry.Path, receipt.Request.Path+"/") {
			return errors.New("NFS provision target occupied before traversal")
		}
		if (entry.Path == "/" || entry.Path == "/Volumes" || strings.HasPrefix(receipt.Request.Path, entry.Path+"/")) && entry.Type != "apfs" && entry.Type != "hfs" {
			return errors.New("NFS provision ancestry is not local")
		}
	}
	g, err := openOwnedTarget(receipt.Request.Path)
	if err != nil {
		return err
	}
	defer g.Close()
	if uint64(g.stat.Dev) != receipt.LeafDev || g.stat.Ino != receipt.LeafIno || uint64(g.parentStat.Dev) != receipt.ParentDev || g.parentStat.Ino != receipt.ParentIno {
		return errors.New("NFS provisioned directory identity changed before mount")
	}
	c.mu.Lock()
	c.provisionSettled = true
	c.mu.Unlock()
	return nil
}
func persistProvisionReceipt(directory string, receipt provisionReceipt) error {
	if directory == "" || !receipt.Request.valid() {
		return errors.New("invalid private provision receipt")
	}
	name := receipt.Request.Operation + ".provision.json"
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if len(data) > maxProvisionFrame {
		return errors.New("provision receipt oversized")
	}
	tmp := filepath.Join(directory, "."+name+"."+receipt.Request.Nonce)
	fd, err := unix.Open(tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), "private NFS provision receipt")
	defer func() { _ = os.Remove(tmp) }()
	if _, err = file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	final := filepath.Join(directory, name)
	if err = os.Link(tmp, final); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		fd, openErr := unix.Open(final, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr != nil {
			return openErr
		}
		original := os.NewFile(uintptr(fd), "existing provision receipt")
		prior, readErr := io.ReadAll(io.LimitReader(original, maxProvisionFrame+2))
		if readErr != nil {
			_ = original.Close()
			return readErr
		}
		if string(prior) != string(append(data, '\n')) {
			_ = original.Close()
			return errors.New("different provision receipt already exists")
		}
		if err = original.Sync(); err != nil {
			_ = original.Close()
			return err
		}
		if err = original.Close(); err != nil {
			return err
		}
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// The operation job waits for the original privilege wrapper and retains its
// Wait result. This accessor is intentionally observation-only.
func (c *Controller) ProvisionPending() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.provisionAttempted || c.provisionSettled {
		return false
	}
	select {
	case <-c.provisionDone:
		return false
	default:
		return true
	}
}
