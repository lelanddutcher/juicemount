package main

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

type fakeCLICustody struct {
	starts, stops int
	stopErr       error
}

func (f *fakeCLICustody) Start(context.Context) error {
	f.starts++
	return errors.New("original mount result uncertain")
}
func (f *fakeCLICustody) Stop(context.Context, time.Duration) error { f.stops++; return f.stopErr }

func TestCLIMountCreationCarriesUncertainCustody(t *testing.T) {
	savedCreator, savedFactory, savedCustody, savedStopping := newCLINFSMountCreator, newCLINFSCustodyController, cliNFSCustody, cliNFSStopping
	defer func() {
		newCLINFSMountCreator, newCLINFSCustodyController, cliNFSCustody, cliNFSStopping = savedCreator, savedFactory, savedCustody, savedStopping
	}()
	cliNFSCustody = nil
	cliNFSStopping = false
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	created := 0
	newCLINFSMountCreator = func() (mountbroker.Creator, error) {
		return mountbroker.Creator{Directory: directory, Observe: func(context.Context) (mounttable.Snapshot, error) {
			return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{}}, nil
		}}, nil
	}
	const export = "/jm-0123456789abcdef0123456789abcdef"
	fake := &fakeCLICustody{}
	newCLINFSCustodyController = func(_ mountbroker.Creator, spec mountbroker.Spec, opts string, interactive bool) (cliNFSController, error) {
		created++
		if !interactive {
			t.Fatal("explicit CLI mount lost setup authorization")
		}
		if spec.MountPath != "/Volumes/fixture" || spec.Source != "127.0.0.1:"+export || spec.ExportPath != export || spec.MetadataPath != "/private/fixture/meta.db" || opts == "" {
			t.Fatal("wrong controller binding")
		}
		return fake, nil
	}
	err := mountNFS("127.0.0.1:12049", export, "/Volumes/fixture", "/private/fixture/meta.db")
	if created != 1 || fake.starts != 1 || err == nil || cliNFSCustody != fake {
		t.Fatalf("created=%d starts=%d err=%v owner=%v", created, fake.starts, err, cliNFSCustody)
	}
	if err := mountNFS("127.0.0.1:12049", export, "/Volumes/fixture", "/private/fixture/meta.db"); err == nil || created != 1 || fake.starts != 1 {
		t.Fatal("uncertain creation replayed")
	}
	fake.stopErr = errors.New("helper response pending")
	if unmountNFS("/Volumes/fixture") || cliNFSCustody != fake {
		t.Fatal("uncertain retirement released custody")
	}
	fake.stopErr = nil
	if !unmountNFS("/Volumes/fixture") || cliNFSCustody != nil {
		t.Fatal("settled retirement did not clear custody")
	}
}

func TestCLIShutdownWaitsForObservedAbsence(t *testing.T) {
	signals := make(chan os.Signal, 1)
	entered, done := make(chan struct{}, 1), make(chan struct{})
	var absent atomic.Bool
	go func() {
		defer close(done)
		waitForNFSMountAbsent("/Volumes/fixture", signals, time.Hour, func(string) bool {
			select {
			case entered <- struct{}{}:
			default:
			}
			return absent.Load()
		})
	}()
	defer func() {
		absent.Store(true)
		select {
		case signals <- syscall.SIGTERM:
		default:
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("shutdown waiter leaked")
		}
	}()
	<-entered
	select {
	case <-done:
		t.Fatal("shutdown completed while mount observation was incomplete")
	default:
	}
	absent.Store(true)
	signals <- syscall.SIGTERM
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after observed absence")
	}
}

func TestCLIRecoveryCannotAuthorizeDirectoryMigration(t *testing.T) {
	oldCreator, oldFactory := newCLINFSMountCreator, newCLINFSCustodyController
	cliNFSMu.Lock()
	oldController, oldStopping := cliNFSCustody, cliNFSStopping
	cliNFSCustody = nil
	cliNFSStopping = false
	cliNFSMu.Unlock()
	defer func() {
		newCLINFSMountCreator, newCLINFSCustodyController = oldCreator, oldFactory
		cliNFSMu.Lock()
		cliNFSCustody, cliNFSStopping = oldController, oldStopping
		cliNFSMu.Unlock()
	}()
	newCLINFSMountCreator = func() (mountbroker.Creator, error) { return mountbroker.Creator{}, nil }
	called := false
	newCLINFSCustodyController = func(_ mountbroker.Creator, _ mountbroker.Spec, _ string, interactive bool) (cliNFSController, error) {
		called = true
		if interactive {
			t.Fatal("background recovery may request directory migration")
		}
		return &fakeCLICustody{}, nil
	}
	_ = mountNFSNonInteractive("127.0.0.1:12049", "/jm-0123456789abcdef0123456789abcdef", "/Volumes/fixture", "/fixture/meta.db")
	if !called {
		t.Fatal("recovery did not reach controller")
	}
}
