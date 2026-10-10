package nfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func heldNoLeaseFixture(t *testing.T, spool *SpoolStore) (*SpoolEntry, *metadata.SpoolRow) {
	t.Helper()
	e := writeSpoolEntry(t, spool, "Film/ProjA/card/A001.mov", []byte("retained local save"))
	if ok, err := spool.Meta().MarkDraining(e.ID()); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := spool.Meta().HoldForProject(e.ID(), "Film/ProjA", "prior lease client"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	return e, row
}

func TestDrainNoLeaseClaimDoesNotBypassInstalledClient(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	e, row := heldNoLeaseFixture(t, spool)
	f := newFakeLeaseServer(t)
	c := newTestLeaseClient(t, f, spool, nil)
	d.SetProjectLeases(c)
	defer d.inFlight.Wait()
	if !d.dispatchRow(row) {
		t.Fatal("dispatch stopped unexpectedly")
	}
	d.inFlight.Wait()
	after, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if after.DrainState != metadata.DrainReady {
		t.Fatalf("client-present claim changed state: %s", after.DrainState)
	}
	if wait, err := spool.Meta().WaitProject(e.ID()); err != nil || wait != "Film/ProjA" {
		t.Fatalf("client-present claim changed wait: %q %v", wait, err)
	}
}

// A client installed after the no-client claim but before worker admission
// must be consulted by the worker, and a foreign hold must prevent the write.
func TestDrainNoLeaseClaimRechecksClientAfterLaneWait(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{Workers: 1, SmallWorkers: -1})
	e, row := heldNoLeaseFixture(t, spool)
	d.sem <- struct{}{} // retain the only media slot until the client changes
	occupied := true
	dispatched := make(chan bool, 1)
	go func() { dispatched <- d.dispatchRow(row) }()
	dispatchJoined := false
	// Even a failed assertion must release the original barrier and collect the
	// original dispatch and worker before newTestDrainer closes its database.
	defer func() {
		if occupied {
			<-d.sem
			occupied = false
		}
		if !dispatchJoined {
			<-dispatched
		}
		d.inFlight.Wait()
	}()
	deadline := time.After(2 * time.Second)
	for {
		claimed, err := spool.Meta().Get(e.ID())
		if err != nil {
			t.Fatal(err)
		}
		wait, err := spool.Meta().WaitProject(e.ID())
		if err != nil {
			t.Fatal(err)
		}
		if claimed.DrainState == metadata.DrainDraining && wait == "" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("no-client claim did not reach lane wait: state=%s wait=%q", claimed.DrainState, wait)
		case <-time.After(time.Millisecond):
		}
	}
	f := newFakeLeaseServer(t)
	f.setHolder("Film/ProjA", "editor@Other-Mac")
	c := newTestLeaseClient(t, f, spool, nil)
	d.SetProjectLeases(c)
	<-d.sem
	occupied = false
	select {
	case ok := <-dispatched:
		dispatchJoined = true
		if !ok {
			t.Fatal("dispatch stopped unexpectedly")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("original dispatch did not join")
	}
	joined := make(chan struct{})
	go func() { d.inFlight.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("original worker did not join")
	}
	after, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if after.DrainState != metadata.DrainReady || after.DrainAttempts != 0 {
		t.Fatalf("foreign client did not rehold: state=%s attempts=%d", after.DrainState, after.DrainAttempts)
	}
	if wait, err := spool.Meta().WaitProject(e.ID()); err != nil || wait != "Film/ProjA" {
		t.Fatalf("foreign client wait: %q %v", wait, err)
	}
	if _, err := os.Lstat(filepath.Join(d.fuseRoot, row.NFSPath)); !os.IsNotExist(err) {
		t.Fatalf("backend changed before lease gate: %v", err)
	}
	if _, err := os.Stat(e.SpoolFilePath()); err != nil {
		t.Fatalf("retained spool source lost: %v", err)
	}
}
