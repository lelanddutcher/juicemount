package nfscustody

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
	"golang.org/x/sys/unix"
)

// Controller belongs to exactly one journal operation and one original mount
// child. Publish it before Start. Failure never authorizes discarding it.
type Controller struct {
	creator            mountbroker.Creator
	spec               mountbroker.Spec
	options            string
	helper             string
	gate               chan struct{}
	mu                 sync.Mutex
	started            bool
	jobDone            chan struct{}
	jobErr             error
	record             *mountbroker.Record
	finalized          *mountbroker.Record
	command            *exec.Cmd
	commandDone        chan struct{}
	commandErr         error
	commandStarted     bool
	commandForeground  bool
	mountExecutable    string    // fixed production path; test may substitute a harmless owned child
	mountInput         io.Reader // test-only held stdin; nil for production mount_nfs
	session            *Session
	capability         *CapabilityProbe
	retired            bool
	stopObservedJob    func() // package test witness; never set by production
	interactive        bool
	targetUID          uint32
	targetGID          uint32
	provisionAttempted bool
	provisionSettled   bool
	provisionCmd       *exec.Cmd
	provisionDone      chan struct{}
	provisionStartErr  error
	provisionWaitErr   error
	provisionOutput    []byte // bounded diagnostic evidence only; never mount or retirement authority
	provisionReceipt   *provisionReceipt
	// H-3 L4: the privileged unmount helper. All fields are guarded by mu
	// and are never held across a helper call.
	unmountd        UnmountHelper
	unmountdLease   UnmountLease
	unmountdOp      string
	unmountdState   string
	unmountdErr     string
	unmountdProof   *mountbroker.HelperRetirement
	abandonedKeeper bool
	// retireGate orders helper retire requests against withdrawals and owns
	// the eject-now busy window (/eject-now forces only after the founder's
	// 30 s floor measured from it). It has its own locks; never held with mu.
	retireGate HelperRetireGate
}

func New(creator mountbroker.Creator, spec mountbroker.Spec, options string) (*Controller, error) {
	return NewWithProvisioning(creator, spec, options, false)
}
func NewWithProvisioning(creator mountbroker.Creator, spec mountbroker.Spec, options string, interactive bool) (*Controller, error) {
	if creator.Observe == nil || spec.MountPath == "" || spec.Source == "" || options == "" {
		return nil, errors.New("incomplete NFS custody creation")
	}
	if interactive && (os.Getuid() <= 0 || uint64(os.Getuid()) >= uint64(^uint32(0)) || os.Getgid() < 0 || uint64(os.Getgid()) >= uint64(^uint32(0))) {
		return nil, errors.New("NFS provisioning requires an original nonroot user")
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	helper := filepath.Join(filepath.Dir(exe), "juicemount-mount-probe")
	c := &Controller{creator: creator, spec: spec, options: options, helper: helper, gate: make(chan struct{}, 1), mountExecutable: "/sbin/mount_nfs", interactive: interactive, targetUID: uint32(os.Getuid()), targetGID: uint32(os.Getgid())}
	c.gate <- struct{}{}
	return c, nil
}
func (c *Controller) lock(timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("NFS controller busy")
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-c.gate:
		return nil
	case <-t.C:
		return errors.New("NFS controller pending")
	}
}
func (c *Controller) unlock() { c.gate <- struct{}{} }
func (c *Controller) Active() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	ready := !c.retired && c.started && c.record != nil && c.record.Mount != nil
	s := c.session
	c.mu.Unlock()
	return ready && s.LiveAdmitted() && s.Armed()
}
func (c *Controller) Path() string {
	if c == nil {
		return ""
	}
	return c.spec.MountPath
}
func (c *Controller) Retired() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.retired
}
func (c *Controller) Record() *mountbroker.Record {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.record == nil {
		return nil
	}
	r := *c.record
	if r.Mount != nil {
		m := *r.Mount
		r.Mount = &m
	}
	return &r
}

// A current-user-owned, empty, canonical local directory is already a valid
// configured target. Missing or root-owned /Volumes children require the
// separately reviewed one-shot privileged provisioning/migration route.
type targetGuard struct {
	parent, leaf int
	name         string
	stat         unix.Stat_t
	parentPath   string
	parentStat   unix.Stat_t
}

func (g *targetGuard) Close() {
	if g == nil {
		return
	}
	_ = unix.Close(g.leaf)
	_ = unix.Close(g.parent)
}
func (g *targetGuard) Revalidate() error {
	var leaf, parent unix.Stat_t
	var namedParent unix.Stat_t
	if err := unix.Lstat(g.parentPath, &namedParent); err != nil {
		return err
	}
	if namedParent.Dev != g.parentStat.Dev || namedParent.Ino != g.parentStat.Ino {
		return errors.New("NFS target parent replaced before mount birth")
	}
	if err := unix.Fstatat(g.parent, g.name, &leaf, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if err := unix.Fstat(g.leaf, &parent); err != nil {
		return err
	}
	if leaf.Dev != g.stat.Dev || leaf.Ino != g.stat.Ino || parent.Dev != g.stat.Dev || parent.Ino != g.stat.Ino {
		return errors.New("NFS target directory replaced before mount birth")
	}
	if parent.Uid != uint32(os.Getuid()) || parent.Mode&unix.S_IFMT != unix.S_IFDIR || parent.Mode&0022 != 0 || parent.Mode&0700 != 0700 {
		return errors.New("NFS target ownership or permissions changed before mount birth")
	}
	var currentParent unix.Stat_t
	if err := unix.Fstat(g.parent, &currentParent); err != nil {
		return err
	}
	if currentParent.Dev != g.parentStat.Dev || currentParent.Ino != g.parentStat.Ino || (currentParent.Uid != 0 && currentParent.Uid != uint32(os.Getuid())) || currentParent.Mode&0022 != 0 {
		return errors.New("NFS target parent permissions changed before mount birth")
	}
	if err := provisionACL(g.parent, false); err != nil {
		return err
	}
	if err := provisionACL(g.leaf, true); err != nil {
		return err
	}
	dup, err := unix.Openat(g.leaf, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(dup), "NFS target revalidation")
	names, readErr := f.Readdirnames(1)
	closeErr := f.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(names) != 0 {
		return errors.New("NFS target became nonempty before mount birth")
	}
	return nil
}
func openTrustedParent(path string) (int, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	check := func(fd int) error {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			return err
		}
		if st.Uid != 0 && st.Uid != uint32(os.Getuid()) || st.Mode&0022 != 0 {
			return errors.New("NFS target ancestry is writable by another user")
		}
		if err := provisionACL(fd, false); err != nil {
			return err
		}
		return nil
	}
	if err := check(fd); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			_ = unix.Close(fd)
			return -1, err
		}
		_ = unix.Close(fd)
		fd = next
		if err := check(fd); err != nil {
			_ = unix.Close(fd)
			return -1, err
		}
	}
	return fd, nil
}
func openOwnedTarget(path string) (*targetGuard, error) {
	if path == "" || path == "/" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("NFS target path is not canonical")
	}
	parentPath, name := filepath.Dir(path), filepath.Base(path)
	resolved, err := filepath.EvalSymlinks(parentPath)
	if err != nil {
		return nil, err
	}
	if resolved != parentPath {
		return nil, errors.New("NFS target parent contains a symlink")
	}
	parent, err := openTrustedParent(parentPath)
	if err != nil {
		return nil, err
	}
	leaf, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(parent)
		return nil, fmt.Errorf("NFS target requires explicit provisioning: %w", err)
	}
	g := &targetGuard{parent: parent, leaf: leaf, name: name, parentPath: parentPath}
	valid := false
	defer func() {
		if !valid {
			g.Close()
		}
	}()
	var parentFS, leafFS unix.Statfs_t
	if err := unix.Fstat(parent, &g.parentStat); err != nil {
		return nil, err
	}
	if err := unix.Fstat(leaf, &g.stat); err != nil {
		return nil, err
	}
	if err := unix.Fstatfs(parent, &parentFS); err != nil {
		return nil, err
	}
	if err := unix.Fstatfs(leaf, &leafFS); err != nil {
		return nil, err
	}
	if !localTargetFilesystem(parentFS, leafFS) {
		return nil, errors.New("NFS target must be an empty directory on the local parent filesystem")
	}
	if g.stat.Uid != uint32(os.Getuid()) {
		return nil, errors.New("NFS target requires explicit ownership provisioning or migration")
	}
	if err := provisionACL(leaf, true); err != nil {
		return nil, err
	}
	if g.stat.Mode&0022 != 0 {
		return nil, errors.New("NFS target is writable by another user")
	}
	if g.stat.Mode&0700 != 0700 || unix.Access(path, unix.W_OK|unix.X_OK) != nil {
		return nil, errors.New("NFS target lacks current-user directory access")
	}
	dup, err := unix.Openat(leaf, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(dup), "NFS target listing")
	names, readErr := f.Readdirnames(1)
	closeErr := f.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(names) != 0 {
		return nil, errors.New("NFS target is not empty")
	}
	if err := g.Revalidate(); err != nil {
		return nil, err
	}
	valid = true
	return g, nil
}
func validateOwnedTarget(path string) error {
	g, err := openOwnedTarget(path)
	if err != nil {
		return err
	}
	g.Close()
	return nil
}

func (c *Controller) observe(ctx context.Context) (mounttable.Snapshot, error) {
	if ctx == nil {
		return mounttable.Snapshot{}, errors.New("missing NFS observation context")
	}
	return c.creator.Observe(ctx)
}
func matchingRow(s mounttable.Snapshot, spec mountbroker.Spec, fsid [2]int32) (*mounttable.MountEntry, error) {
	if s.Version != 1 || s.BootID == "" || s.Entries == nil {
		return nil, errors.New("incomplete NFS mount snapshot")
	}
	var row *mounttable.MountEntry
	for _, e := range s.Entries {
		if strings.HasPrefix(e.Path, spec.MountPath+"/") {
			return nil, errors.New("NFS target has mounted descendant")
		}
		if e.Path == spec.MountPath || e.Source == spec.Source || fsid != [2]int32{} && e.FSID == fsid {
			if row != nil || e.Path != spec.MountPath || e.Source != spec.Source || e.Type != "nfs" || e.FSID == [2]int32{} || fsid != [2]int32{} && e.FSID != fsid {
				return nil, errors.New("NFS mount identity replaced or duplicated")
			}
			copy := e
			row = &copy
		}
	}
	return row, nil
}
func (c *Controller) startCommand(rec mountbroker.Record) error {
	c.mu.Lock()
	c.record = &rec
	c.mu.Unlock()
	if err := validateEventCapacity(rec.BootID, rec.OperationID, c.spec.MountPath, c.spec.Source); err != nil {
		return err
	}
	fresh, err := c.observe(context.Background())
	if err != nil {
		return err
	}
	if fresh.BootID != rec.BootID {
		return errors.New("NFS boot changed before target validation")
	}
	row, err := matchingRow(fresh, c.spec, [2]int32{})
	if err != nil {
		return err
	}
	if row != nil {
		return errors.New("NFS target became mounted before command")
	}
	for _, e := range fresh.Entries {
		if (e.Path == "/" || strings.HasPrefix(c.spec.MountPath, e.Path+"/")) && e.Type != "apfs" && e.Type != "hfs" {
			return errors.New("NFS target has a nonlocal containing mount")
		}
	}
	g, err := openOwnedTarget(c.spec.MountPath)
	if err != nil {
		if !c.interactive {
			return err
		}
		if err := c.provision(rec); err != nil {
			return err
		}
		g, err = openOwnedTarget(c.spec.MountPath)
		if err != nil {
			return err
		}
	}
	defer g.Close()
	if err := g.Revalidate(); err != nil {
		return err
	}
	args := c.mountArgs()
	cmd := exec.Command(c.mountExecutable, args...)
	cmd.Stdin = c.mountInput
	c.mu.Lock()
	c.command = cmd
	c.commandForeground = foregroundMountArgs(args)
	c.commandDone = make(chan struct{})
	done := c.commandDone
	c.mu.Unlock()
	if err := cmd.Start(); err != nil {
		c.mu.Lock()
		c.commandErr = err
		c.mu.Unlock()
		close(done)
		return err
	}
	c.mu.Lock()
	c.commandStarted = true
	c.mu.Unlock()
	go func() { err := cmd.Wait(); c.mu.Lock(); c.commandErr = err; c.mu.Unlock(); close(done) }()
	<-done
	c.mu.Lock()
	commandErr := c.commandErr
	c.mu.Unlock()
	if commandErr != nil {
		return fmt.Errorf("original mount_nfs child: %w", commandErr)
	}
	return c.admit(8 * time.Second)
}
func (c *Controller) mountArgs() []string {
	// Apple mount_nfs reads nfs.conf before argv. A separate final -o makes
	// nobg and the normal foreground two-attempt count win over that file and
	// over caller-provided bg/retrycnt options. No later option may re-enable bg.
	return []string{"-o", c.options, "-o", "nobg,retrycnt=1", c.spec.Source, c.spec.MountPath}
}
func foregroundMountArgs(args []string) bool {
	return len(args) == 6 && args[0] == "-o" && args[2] == "-o" && args[3] == "nobg,retrycnt=1" && args[4] != "" && args[5] != ""
}
func validateEventCapacity(boot, operation, path, source string) error {
	return writeFrame(io.Discard, event{Version: protocolVersion, Session: strings.Repeat("0", 32), ID: ^uint64(0), State: "admission_refused", Boot: boot, Operation: operation, Path: path, Source: source, FSID: [2]int32{-2147483648, -2147483648}})
}
func (c *Controller) admit(timeout time.Duration) error {
	c.mu.Lock()
	r, s := c.record, c.session
	c.mu.Unlock()
	if r == nil {
		return errors.New("NFS journal operation missing")
	}
	if s == nil {
		fresh, err := c.observe(context.Background())
		if err != nil {
			return err
		}
		if fresh.BootID != r.BootID {
			return errors.New("NFS boot identity changed")
		}
		row, err := matchingRow(fresh, c.spec, [2]int32{})
		if err != nil {
			return err
		}
		if row == nil {
			return errors.New("NFS mount not observed for custodian admission")
		}
		s, err = startSession(c.helper, r.BootID, r.OperationID, c.spec.MountPath, c.spec.Source)
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.session = s
		c.mu.Unlock()
	}
	if err := s.WaitReady(timeout); err != nil {
		return err
	}
	fsid, ready := s.Identity()
	if !ready {
		return errors.New("NFS custodian has not admitted root")
	}
	fresh, err := c.observe(context.Background())
	if err != nil {
		return err
	}
	if fresh.BootID != r.BootID {
		return errors.New("NFS boot identity changed")
	}
	row, err := matchingRow(fresh, c.spec, fsid)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.New("NFS custodian row absent")
	}
	c.mu.Lock()
	r.Mount = row
	c.mu.Unlock()
	return nil
}

func (c *Controller) Start(ctx context.Context) error {
	if c == nil {
		return errors.New("missing NFS controller")
	}
	if ctx == nil {
		return errors.New("missing NFS start context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.lock(5 * time.Second); err != nil {
		return err
	}
	defer c.unlock()
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("NFS controller already started")
	}
	c.started = true
	c.mu.Unlock()
	probe, err := StartCapability(c.helper)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.capability = probe
	c.mu.Unlock()
	if err := probe.Wait(5 * time.Second); err != nil {
		return err
	}
	done := c.createOperation(ctx, c.startCommand)
	select {
	case <-done:
		c.mu.Lock()
		err := c.jobErr
		finalized, commandDone, commandErr, s := c.finalized, c.commandDone, c.commandErr, c.session
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if finalized == nil || finalized.State != mountbroker.Observed ||
			finalized.Mount == nil || commandDone == nil || s == nil {
			return errors.New("NFS creation completed without original arm authority")
		}
		select {
		case <-commandDone:
		default:
			return errors.New("original mount command not joined before arm")
		}
		if commandErr != nil {
			return errors.New("original mount command failed before arm")
		}
		if err := s.Arm(*finalized, 5*time.Second); err != nil {
			return fmt.Errorf("original NFS custodian arm unsettled: %w", err)
		}
		c.registerWithHelper(*finalized)
		return nil
	case <-time.After(180 * time.Second):
		return errors.New("NFS creation job pending; custody retained")
	}
}
func (c *Controller) createOperation(ctx context.Context, mount func(mountbroker.Record) error) <-chan struct{} {
	c.mu.Lock()
	c.jobDone = make(chan struct{})
	done := c.jobDone
	c.mu.Unlock()
	go func() {
		result := c.creator.CreateBound(context.WithoutCancel(ctx), c.spec, mount)
		c.mu.Lock()
		c.jobErr = result.Failure()
		if result.State == mountbroker.Observed && result.Err == nil && result.Record != nil {
			copy := *result.Record
			if copy.Mount != nil {
				mount := *copy.Mount
				copy.Mount = &mount
			}
			c.finalized = &copy
		}
		c.mu.Unlock()
		close(done)
	}()
	return done
}

// Stop never infers authority from a path or journal row. Every possible
// mount birth remains attached to this controller until descriptor release,
// the original child wait, and durable retirement have all completed.
func (c *Controller) Stop(ctx context.Context, timeout time.Duration) error {
	if c == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	if err := c.lock(timeout); err != nil {
		return err
	}
	defer c.unlock()
	c.mu.Lock()
	if c.retired {
		c.mu.Unlock()
		return nil
	}
	job := c.jobDone
	c.mu.Unlock()
	if c.stopObservedJob != nil {
		c.stopObservedJob()
	}
	if job != nil {
		select {
		case <-job:
		case <-time.After(time.Until(deadline)):
			return errors.New("NFS creation job still active")
		}
	}
	c.mu.Lock()
	r, done, s, cmd, probe := c.record, c.commandDone, c.session, c.command, c.capability
	finalized := c.finalized
	c.mu.Unlock()
	if s != nil && s.ArmPending() {
		if finalized == nil {
			return errors.New("pending NFS arm lost finalized operation")
		}
		if err := s.Arm(*finalized, time.Until(deadline)); err != nil {
			return fmt.Errorf("pending original NFS arm not reconciled: %w", err)
		}
	}
	if r == nil {
		if cmd != nil {
			return errors.New("NFS command exists without journal operation")
		}
		if probe != nil && !probe.Settled() {
			return errors.New("NFS capability child still live")
		}
		c.mu.Lock()
		c.retired = true
		c.mu.Unlock()
		return nil
	}
	if done == nil {
		c.mu.Lock()
		attempted, settled := c.provisionAttempted, c.provisionSettled
		c.mu.Unlock()
		if attempted && !settled {
			if err := c.settleProvision(); err != nil {
				return fmt.Errorf("NFS directory provision outcome remains uncertain: %w", err)
			}
		}
		fresh, err := c.observe(ctx)
		if err != nil {
			return err
		}
		if fresh.BootID != r.BootID {
			return errors.New("NFS boot changed before no-birth retirement")
		}
		row, err := matchingRow(fresh, c.spec, [2]int32{})
		if err != nil {
			return err
		}
		if row != nil {
			return errors.New("NFS mount exists without original command")
		}
		if err := c.creator.Retire(ctx, *r, func() error { return nil }); err != nil {
			return err
		}
		c.mu.Lock()
		c.retired = true
		c.mu.Unlock()
		return nil
	}
	select {
	case <-done:
	case <-time.After(time.Until(deadline)):
		return errors.New("original mount_nfs child still live")
	}
	fresh, err := c.observe(ctx)
	if err != nil {
		return err
	}
	if fresh.BootID != r.BootID {
		return errors.New("NFS boot changed before retirement")
	}
	row, err := matchingRow(fresh, c.spec, [2]int32{})
	if err != nil {
		return err
	}
	proof := c.helperProof()
	if row != nil && proof != nil {
		// The helper retired our fsid; whatever is at the path now is not
		// ours to detach.
		return errors.New("NFS path occupied after helper retirement")
	}
	if row != nil {
		detachErr := func() error {
			_, ready := s.Identity()
			if s == nil || !ready {
				if err := c.admit(time.Until(deadline)); err != nil {
					return err
				}
				c.mu.Lock()
				s = c.session
				c.mu.Unlock()
			}
			fsid, ready := s.Identity()
			if !ready || row.FSID != fsid {
				return errors.New("NFS retained descriptor differs from mounted row")
			}
			return s.Detach(time.Until(deadline))
		}()
		if detachErr != nil {
			// Descriptor tier unavailable (busy, lost or never admitted):
			// the helper retires the registered fsid instead. The engine is
			// this live process, so the liveness is Alive: the helper only
			// ever issues ordinary (MNT_NOBLOCK) unmounts for it and never
			// forces. If this Stop gives up, the ordinary job is cancelled
			// so a busy volume is not ejected later behind the user's back.
			wait := time.Until(deadline) - time.Second
			if wait > time.Duration(client.MaxRetireWait)*time.Millisecond {
				wait = time.Duration(client.MaxRetireWait) * time.Millisecond
			}
			if wait <= 0 {
				return detachErr
			}
			hctx, cancel := context.WithTimeout(ctx, wait+time.Second)
			_, herr := c.RetireWithHelper(hctx, client.RetireRequest{Liveness: client.LivenessAlive}, wait)
			cancel()
			if herr != nil {
				c.CancelHelperRetire()
				return fmt.Errorf("%w; unmount helper: %v", detachErr, herr)
			}
			proof = c.helperProof()
			c.mu.Lock()
			s = c.session
			c.mu.Unlock()
		}
	} else if s != nil && !s.Detached() && proof == nil {
		if err := s.ReconcileDetach(time.Until(deadline)); err != nil {
			return fmt.Errorf("NFS mount absent but custodian detach unconfirmed: %w", err)
		}
	}
	if row == nil && s == nil {
		c.mu.Lock()
		commandErr := c.commandErr
		commandStarted := c.commandStarted
		foreground := c.commandForeground
		c.mu.Unlock()
		if commandStarted {
			var exited *exec.ExitError
			if !foreground || cmd == nil || cmd.ProcessState == nil || !cmd.ProcessState.Exited() || cmd.ProcessState.ExitCode() <= 0 || !errors.As(commandErr, &exited) {
				return errors.New("started NFS command lacks normal failed foreground exit proof")
			}
		} else if commandErr == nil {
			return errors.New("NFS command has no proven failed start")
		}
	}
	if s != nil {
		if !s.Detached() && proof == nil {
			return errors.New("NFS custodian not detached")
		}
		absent, err := c.observe(ctx)
		if err != nil {
			return err
		}
		if absent.BootID != r.BootID {
			return errors.New("NFS boot changed after descriptor detach")
		}
		still, err := matchingRow(absent, c.spec, [2]int32{})
		if err != nil {
			return err
		}
		if still != nil {
			return errors.New("NFS mount still present after descriptor detach")
		}
		if proof != nil && !s.Detached() {
			if err := c.abandonKeeper(s, time.Until(deadline)); err != nil {
				return err
			}
		} else if err := s.Release(time.Until(deadline)); err != nil {
			return err
		}
	}
	// The live callback stays separate from the journal record; an observed
	// row by itself never grants a path-based detach or retirement.
	settle := func() error {
		select {
		case <-done:
		default:
			return errors.New("original mount child not joined")
		}
		if s != nil && !s.Settled() && !(proof != nil && keeperJoined(s)) {
			return errors.New("NFS descriptor helper not joined")
		}
		if row == nil && s == nil {
			c.mu.Lock()
			started, foreground, commandErr := c.commandStarted, c.commandForeground, c.commandErr
			c.mu.Unlock()
			if started {
				var exited *exec.ExitError
				if !foreground || cmd == nil || cmd.ProcessState == nil || !cmd.ProcessState.Exited() || cmd.ProcessState.ExitCode() <= 0 || !errors.As(commandErr, &exited) {
					return errors.New("original foreground mount failure proof lost")
				}
			}
			independent, err := c.observe(ctx)
			if err != nil || independent.Version != 1 || independent.BootID != r.BootID || independent.Entries == nil {
				return errors.New("independent NFS absence snapshot unavailable")
			}
			again, err := matchingRow(independent, c.spec, [2]int32{})
			if err != nil || again != nil {
				return errors.New("NFS mount appeared before retirement")
			}
		}
		return nil
	}
	if proof != nil {
		err = c.creator.RetireWithHelperProof(ctx, *r, *proof, settle)
	} else {
		err = c.creator.Retire(ctx, *r, settle)
	}
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.retired = true
	c.mu.Unlock()
	c.finishHelperReceipt(2 * time.Second)
	return nil
}

// UnmountLease is a held helper receipt binding. It is an alias of an
// unnamed interface so one adapter satisfies the identical seams in this
// package and in health.
type UnmountLease = interface {
	OpID() string
	Lost() <-chan struct{}
	Err() error
	Close()
}

// UnmountHelper is the service-side seam to the privileged unmount helper
// (juicemount-unmountd, H-3). Production wraps *client.Client; tests use a
// fake. The helper targets a mount by fsid plus the receipt registered here,
// never by path.
type UnmountHelper interface {
	Register(ctx context.Context, rr client.RegisterRequest) (UnmountLease, error)
	Retire(ctx context.Context, opID string, rr client.RetireRequest, wait time.Duration) (client.Outcome, error)
	Cancel(ctx context.Context, opID string) (client.Outcome, error)
}

// Helper receipt states reported by HelperStatus.
const (
	HelperStateNone           = "none"            // no mount registered yet
	HelperStateUnavailable    = "unavailable"     // no helper client in this process
	HelperStateRegistered     = "registered"      // receipt stored, lease held
	HelperStateRegisterFailed = "register_failed" // helper refused or unreachable; today's behaviour
	HelperStateLost           = "lease_lost"      // lease ended while the mount was still ours
	HelperStateRetiring       = "retiring"        // a helper retirement is running
	HelperStateRetired        = "retired"         // helper proved the fsid gone
)

// helperRegisterTimeout bounds Register after a mount birth. Start never
// waits longer than this for an optional helper.
var helperRegisterTimeout = 5 * time.Second

// HelperStatus is the helper view of this controller for /health and logs.
type HelperStatus struct {
	OpID  string `json:"op_id,omitempty"`
	State string `json:"state"`
	Error string `json:"error,omitempty"`
}

// SetUnmountHelper wires the helper before Start. A nil helper keeps the
// descriptor-only behaviour.
func (c *Controller) SetUnmountHelper(h UnmountHelper) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unmountd = h
}

// HelperStatus reports the receipt state. A lease that ended while the mount
// is still ours is reported as lost, with the reason.
func (c *Controller) HelperStatus() HelperStatus {
	if c == nil {
		return HelperStatus{State: HelperStateNone}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := HelperStatus{OpID: c.unmountdOp, State: c.unmountdState, Error: c.unmountdErr}
	if st.State == "" {
		st.State = HelperStateNone
	}
	if st.State == HelperStateRegistered && c.unmountdLease != nil {
		select {
		case <-c.unmountdLease.Lost():
			st.State = HelperStateLost
			if err := c.unmountdLease.Err(); err != nil {
				st.Error = err.Error()
			}
		default:
		}
	}
	return st
}

// HelperOpID is the helper op this controller registered ("" before).
func (c *Controller) HelperOpID() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unmountdOp
}

// registerWithHelper records a receipt for the exact Observed mount and
// holds the lease. A helper failure never fails the mount: the GUI gates
// Start on helper approval separately, and the descriptor tier still works.
func (c *Controller) registerWithHelper(rec mountbroker.Record) {
	c.mu.Lock()
	h := c.unmountd
	c.mu.Unlock()
	if h == nil {
		c.setHelperState("", HelperStateUnavailable, "")
		return
	}
	op := mountbroker.HelperOpID(rec)
	if op == "" || rec.Mount == nil {
		c.setHelperState("", HelperStateRegisterFailed, "observed record lacks a mount identity")
		return
	}
	req := client.RegisterRequest{OpID: op, RegisterArgs: client.RegisterArgs{
		Type:        "nfs",
		FSID:        rec.Mount.FSID,
		Token:       strings.TrimPrefix(rec.Spec.ExportPath, "/jm-"),
		MntOnName:   rec.Mount.Path,
		MntFromName: rec.Mount.Source,
	}}
	ctx, cancel := context.WithTimeout(context.Background(), helperRegisterTimeout)
	lease, err := h.Register(ctx, req)
	cancel()
	if err == nil && lease == nil {
		err = errors.New("helper returned no lease")
	}
	if err != nil {
		log.Printf("[nfs] unmount helper register %s failed; descriptor custody only: %v", op, err)
		c.setHelperState(op, HelperStateRegisterFailed, err.Error())
		return
	}
	c.mu.Lock()
	c.unmountdLease = lease
	c.unmountdOp = op
	c.unmountdState = HelperStateRegistered
	c.unmountdErr = ""
	c.mu.Unlock()
	log.Printf("[nfs] unmount helper receipt %s registered (fsid %v)", op, rec.Mount.FSID)
}

func (c *Controller) setHelperState(op, state, detail string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if op != "" {
		c.unmountdOp = op
	}
	c.unmountdState = state
	c.unmountdErr = detail
}

// RetireWithHelper asks the helper to retire this controller's receipt and
// waits up to wait. It takes no path. Only a RETIRED outcome for this op is
// kept as proof; Stop then completes durable retirement without the
// descriptor. It never takes the controller gate, so an eject-now can run
// while a Stop is blocked on a busy descriptor detach. The request counts as
// arriving now (see RetireWithHelperAt).
func (c *Controller) RetireWithHelper(ctx context.Context, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	if c == nil {
		return client.Outcome{}, errors.New("missing NFS controller")
	}
	return c.RetireWithHelperAt(ctx, c.retireGate.Generation(), rr, wait)
}

// HelperWithdrawGeneration is read when an eject-now request ARRIVES and
// passed to RetireWithHelperAt: a withdrawal after that point wins.
func (c *Controller) HelperWithdrawGeneration() uint64 {
	if c == nil {
		return 0
	}
	return c.retireGate.Generation()
}

// RetireWithHelperAt is RetireWithHelper for a request that arrived at
// withdraw generation gen (HelperRetireGate). A withdrawal since then makes
// it return ErrHelperRetireWithdrawn: a force is then never sent, and an
// ordinary job it started is withdrawn again without re-arming the busy
// window.
func (c *Controller) RetireWithHelperAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	return c.retireWithHelperAt(ctx, gen, rr, wait, false)
}

// StartRetireWithHelperAt is RetireWithHelperAt for the busy-Quit panel's
// START request (HelperRetireGate.RetireStart): an ordinary retire that
// never forces and restarts the eject-now busy window at its arrival. A
// force-wanting rr is refused with ErrHelperRetireStartForce.
func (c *Controller) StartRetireWithHelperAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration) (client.Outcome, error) {
	return c.retireWithHelperAt(ctx, gen, rr, wait, true)
}

func (c *Controller) retireWithHelperAt(ctx context.Context, gen uint64, rr client.RetireRequest, wait time.Duration, start bool) (client.Outcome, error) {
	if c == nil {
		return client.Outcome{}, errors.New("missing NFS controller")
	}
	c.mu.Lock()
	h, op, proof := c.unmountd, c.unmountdOp, c.unmountdProof
	registered := c.unmountdLease != nil
	c.mu.Unlock()
	if proof != nil {
		return client.Outcome{OpID: proof.OpID, State: proof.State}, nil
	}
	if h == nil {
		return client.Outcome{}, errors.New("unmount helper unavailable")
	}
	if op == "" || !registered {
		return client.Outcome{}, errors.New("NFS mount has no helper receipt")
	}
	gateRetire := c.retireGate.Retire
	if start {
		gateRetire = c.retireGate.RetireStart
	}
	out, err := gateRetire(ctx, gen, rr,
		func(ctx context.Context) (client.Outcome, error) {
			c.setHelperState(op, HelperStateRetiring, "")
			return h.Retire(ctx, op, rr, wait)
		},
		func(ctx context.Context) (client.Outcome, error) { return h.Cancel(ctx, op) })
	if err != nil {
		c.setHelperState(op, HelperStateRegistered, err.Error())
		return out, err
	}
	if out.OpID != op || out.State != mountbroker.HelperRetiredState {
		c.setHelperState(op, HelperStateRegistered, "helper final state "+out.State)
		return out, fmt.Errorf("unmount helper ended %s in state %s", op, out.State)
	}
	c.recordHelperProof(op, out)
	return out, nil
}

func (c *Controller) recordHelperProof(op string, out client.Outcome) {
	c.mu.Lock()
	c.unmountdProof = &mountbroker.HelperRetirement{OpID: op, State: out.State}
	c.unmountdState = HelperStateRetired
	c.unmountdErr = ""
	c.mu.Unlock()
	log.Printf("[nfs] unmount helper retired %s (forced=%v)", op, out.Forced)
}

func (c *Controller) helperProof() *mountbroker.HelperRetirement {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unmountdProof == nil {
		return nil
	}
	p := *c.unmountdProof
	return &p
}

// finishHelperReceipt runs after a descriptor detach proved the mount gone:
// the helper sees the fsid absent and retires the receipt without a syscall.
// Failure is harmless (the helper's orphan sweep retires an absent fsid), so
// it is only logged. The lease is released afterwards either way.
func (c *Controller) finishHelperReceipt(wait time.Duration) {
	c.mu.Lock()
	h, op, lease, proof := c.unmountd, c.unmountdOp, c.unmountdLease, c.unmountdProof
	c.unmountdLease = nil
	c.mu.Unlock()
	if lease == nil {
		return
	}
	if proof == nil && h != nil && op != "" {
		ctx, cancel := context.WithTimeout(context.Background(), wait+time.Second)
		out, err := h.Retire(ctx, op, client.RetireRequest{Liveness: client.LivenessAlive}, wait)
		cancel()
		if err != nil || out.State != mountbroker.HelperRetiredState {
			log.Printf("[nfs] unmount helper receipt %s left for its orphan sweep: state=%s err=%v", op, out.State, err)
		} else {
			c.setHelperState(op, HelperStateRetired, "")
		}
	}
	lease.Close()
}

// abandonKeeper ends the custodian after the helper retired its mount. Its
// descriptor now refers to a detached mount, so it can never report a
// descriptor detach and would hold forever. It is our unreaped child (its
// exec handle is retained), so SIGKILL reaches exactly that process; it is
// the mount-probe custodian, never JuiceFS.
func (c *Controller) abandonKeeper(s *Session, timeout time.Duration) error {
	if s == nil {
		return nil
	}
	select {
	case <-s.waitDone:
		return nil
	default:
	}
	if s.cmd == nil || s.cmd.Process == nil {
		return errors.New("NFS custodian exec handle missing")
	}
	if err := s.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	c.mu.Lock()
	c.abandonedKeeper = true
	c.mu.Unlock()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-s.waitDone:
		return nil
	case <-t.C:
		return errors.New("NFS custodian not joined after helper retirement")
	}
}

func keeperJoined(s *Session) bool {
	if s == nil {
		return true
	}
	select {
	case <-s.waitDone:
		return true
	default:
		return false
	}
}

// helperNow is the clock for the eject-now floor (test seam).
var helperNow = time.Now

// OrdinaryBusyMaxGap is the longest gap between two busy observations that
// still counts as one continuous busy window.
var OrdinaryBusyMaxGap = 90 * time.Second

// OrdinaryBusy reports when an ordinary helper retire of this op first and
// most recently came back still busy (zero: never, or since cancelled).
func (c *Controller) OrdinaryBusy() (since, last time.Time) {
	if c == nil {
		return time.Time{}, time.Time{}
	}
	return c.retireGate.Busy()
}

// CancelHelperRetire withdraws this op's ordinary helper retirement after a
// Stop (or the stale retirer) gave up, so an orphaned EBUSY-backoff job
// cannot eject the volume later. Best effort, bounded; a refusal (a force was
// requested or issued) is only logged. It is JuiceMount's own withdrawal
// (HelperRetireGate.WithdrawInternal): it never refuses a user's eject-now
// request in flight, and never makes one claim Quit was cancelled.
func (c *Controller) CancelHelperRetire() {
	out, err := c.withdrawHelperRetire(context.Background(), true)
	if err != nil {
		log.Printf("[nfs] unmount helper cancel %s refused or failed (state %s): %v", c.HelperStatus().OpID, out.State, err)
	}
}

// WithdrawHelperRetire cancels this op's ordinary helper retirement and
// clears the eject-now busy window. Every retire request that arrived before
// it is invalidated (HelperRetireGate): one still in flight forces nothing
// and withdraws any job it started. It is refused with
// client.ErrNotCancellable while a force-wanting request of this controller
// runs, and by the helper on its force path, which is never cancelled. The
// helper accepts it otherwise (also with an ordinary call in the kernel:
// nothing further is issued). With no receipt, or once the helper has
// already retired the mount, there is nothing to withdraw: it returns the
// zero Outcome (or the RETIRED proof) and no error. A helper job that had
// already ended RETIRED is recorded as the proof and returned.
//
// It is the USER's withdrawal (DELETE /eject-now). JuiceMount's own
// withdrawals go through CancelHelperRetire.
func (c *Controller) WithdrawHelperRetire(ctx context.Context) (client.Outcome, error) {
	return c.withdrawHelperRetire(ctx, false)
}

func (c *Controller) withdrawHelperRetire(ctx context.Context, internal bool) (client.Outcome, error) {
	if c == nil {
		return client.Outcome{}, nil
	}
	c.mu.Lock()
	h, op, proof := c.unmountd, c.unmountdOp, c.unmountdProof
	c.mu.Unlock()
	if proof != nil {
		return client.Outcome{OpID: proof.OpID, State: proof.State}, nil
	}
	withdraw := c.retireGate.Withdraw
	if internal {
		withdraw = c.retireGate.WithdrawInternal
	}
	if h == nil || op == "" {
		_, _ = withdraw(ctx, func(context.Context) (client.Outcome, error) { return client.Outcome{}, nil })
		return client.Outcome{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, helperWithdrawTimeout)
	defer cancel()
	out, err := withdraw(ctx, func(ctx context.Context) (client.Outcome, error) { return h.Cancel(ctx, op) })
	if errors.Is(err, client.ErrNotCancellable) && out.OpID == "" {
		out.OpID = op
	}
	if err != nil {
		return out, err
	}
	if out.OpID == op && out.State == mountbroker.HelperRetiredState {
		c.recordHelperProof(op, out)
		return out, nil
	}
	c.setHelperState(op, HelperStateRegistered, "")
	return out, nil
}
