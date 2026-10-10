//go:build serviceclient

package main

// Desktop archive: the app process runs no juicefs, so the object-key status
// comes from the serving process through the object-key status call,
// "enroll-verify" {enrollVerifyOpObjectKeyStatus, "", ""}.

import (
	"context"
	"net"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
)

func TestDesktopObjectKeyStatusComesFromServingProcess(t *testing.T) {
	var got []servicecontrol.Request
	installControlFake(t, func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
		got = append(got, req)
		return servicecontrol.Response{Version: 1, Text: `{"live":false,"route":"status","object_key":{"object_key_in_use":"mac-4f1c2a9e7b3d-1","refusals":2,"refused_code":"AccessDenied","refused_at_ms":5}}`}, nil
	})
	st, known := currentObjectKeyStatus()
	if !known || st.InUse != "mac-4f1c2a9e7b3d-1" || st.Refusals != 2 || st.RefusedCode != "AccessDenied" {
		t.Fatalf("status = %+v known=%v", st, known)
	}
	if len(got) != 1 || got[0].Method != "enroll-verify" || len(got[0].Strings) != 3 ||
		got[0].Strings[0] != enrollVerifyOpObjectKeyStatus || got[0].Strings[1] != "" || got[0].Strings[2] != "" {
		t.Fatalf("forwarded request = %+v", got)
	}

	// A serving process that does not know the status call answers it as a
	// refused login check, with no status: unknown, not "nothing in use".
	installControlFake(t, func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
		return servicecontrol.Response{Version: 1, Text: `{"live":false,"reason":"x","route":"refused"}`}, nil
	})
	if _, known := currentObjectKeyStatus(); known {
		t.Fatal("an answer without a status was taken as known")
	}
	// No serving process.
	installControlFake(t, func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
		return servicecontrol.Response{}, &net.OpError{Op: "dial", Net: "unix", Err: syscall.ECONNREFUSED}
	})
	if _, known := currentObjectKeyStatus(); known {
		t.Fatal("no serving process, but the status is known")
	}
}
