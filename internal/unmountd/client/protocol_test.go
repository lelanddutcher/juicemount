package client

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

var testToken = strings.Repeat("0123456789abcdef", 2)

func validRegister() Request {
	return Request{Op: OpRegister, OpID: "nfs-1", Register: &RegisterArgs{
		Type: "nfs", FSID: [2]int32{0x1a000031, 0x31}, Token: testToken,
		MntOnName: "/Volumes/zpool", MntFromName: "127.0.0.1:/jm-" + testToken}}
}

func TestRequestRoundTrip(t *testing.T) {
	for _, r := range []Request{
		{Op: OpPing},
		validRegister(),
		{Op: OpReclaim, OpID: "nfs-1"},
		{Op: OpStatus, OpID: "nfs-1"},
		{Op: OpRetire, OpID: "nfs-1", Retire: &RetireArgs{Liveness: LivenessDead, EjectNow: true}, WaitMillis: MaxRetireWait},
	} {
		b, err := EncodeRequest(r)
		if err != nil {
			t.Fatalf("%s: encode: %v", r.Op, err)
		}
		got, err := DecodeRequest(ProtocolVersion, b)
		if err != nil {
			t.Fatalf("%s: decode: %v", r.Op, err)
		}
		again, _ := EncodeRequest(got)
		if !bytes.Equal(b, again) {
			t.Fatalf("%s: round trip changed the body:\n%s\n%s", r.Op, b, again)
		}
	}
}

func TestDecodeRequestRejectsBadFraming(t *testing.T) {
	good, err := EncodeRequest(validRegister())
	if err != nil {
		t.Fatal(err)
	}
	huge := append([]byte(`{"op":"ping","x":"`), bytes.Repeat([]byte("a"), MaxBody)...)
	huge = append(huge, '"', '}')
	cases := map[string]struct {
		v    int64
		body []byte
	}{
		"wrong version":       {2, good},
		"zero version":        {0, good},
		"empty body":          {ProtocolVersion, nil},
		"oversized body":      {ProtocolVersion, huge},
		"not json":            {ProtocolVersion, []byte("op=ping")},
		"trailing object":     {ProtocolVersion, append(append([]byte{}, good...), []byte(`{"op":"ping"}`)...)},
		"unknown field":       {ProtocolVersion, []byte(`{"op":"ping","uid":0}`)},
		"payload uid ignored": {ProtocolVersion, []byte(`{"op":"status","op_id":"a","caller":{"uid":0}}`)},
		"array body":          {ProtocolVersion, []byte(`[{"op":"ping"}]`)},
	}
	for name, c := range cases {
		if _, err := DecodeRequest(c.v, c.body); !errors.Is(err, ErrFraming) {
			t.Errorf("%s: err = %v, want ErrFraming", name, err)
		}
	}
}

func TestRequestValidate(t *testing.T) {
	reg := validRegister()
	badType := validRegister()
	badType.Register.Type = "smbfs"
	zeroFSID := validRegister()
	zeroFSID.Register.FSID = [2]int32{}
	shortTok := validRegister()
	shortTok.Register.Token = "abc"
	longName := validRegister()
	longName.Register.MntOnName = "/" + strings.Repeat("a", 1024)
	cases := map[string]Request{
		"unknown op":            {Op: "unmount_path", OpID: "a"},
		"no op":                 {OpID: "a"},
		"ping with op id":       {Op: OpPing, OpID: "a"},
		"status no op id":       {Op: OpStatus},
		"status bad op id":      {Op: OpStatus, OpID: "../../etc/passwd"},
		"status upper op id":    {Op: OpStatus, OpID: "ABC"},
		"status with args":      {Op: OpStatus, OpID: "a", Retire: &RetireArgs{Liveness: LivenessDead}},
		"register no args":      {Op: OpRegister, OpID: "a"},
		"register extra args":   {Op: OpRegister, OpID: reg.OpID, Register: reg.Register, Retire: &RetireArgs{Liveness: LivenessAlive}},
		"register bad type":     badType,
		"register zero fsid":    zeroFSID,
		"register short token":  shortTok,
		"register long name":    longName,
		"retire no args":        {Op: OpRetire, OpID: "a"},
		"retire bad liveness":   {Op: OpRetire, OpID: "a", Retire: &RetireArgs{Liveness: "probably-dead"}},
		"retire empty liveness": {Op: OpRetire, OpID: "a", Retire: &RetireArgs{}},
		"retire wait too long":  {Op: OpRetire, OpID: "a", Retire: &RetireArgs{Liveness: LivenessAlive}, WaitMillis: MaxRetireWait + 1},
		"retire negative wait":  {Op: OpRetire, OpID: "a", Retire: &RetireArgs{Liveness: LivenessAlive}, WaitMillis: -1},
		"reclaim with wait":     {Op: OpReclaim, OpID: "a", WaitMillis: 5},
	}
	for name, r := range cases {
		if err := r.Validate(); !errors.Is(err, ErrFraming) {
			t.Errorf("%s: Validate = %v, want ErrFraming", name, err)
		}
		if _, err := EncodeRequest(r); err == nil {
			t.Errorf("%s: EncodeRequest accepted an invalid request", name)
		}
	}
}

func TestResponseCodec(t *testing.T) {
	r := Response{Code: CodePending, Outcome: &Outcome{OpID: "a", State: "WAIT_NESTED",
		Descendants: []Entry{{FSID: [2]int32{1, 2}, Owner: 0, Type: "apfs", MntOnName: "/Volumes/zpool/x"}}}}
	b, err := EncodeResponse(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeResponse(ProtocolVersion, b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Code != CodePending || got.Outcome.State != "WAIT_NESTED" || len(got.Outcome.Descendants) != 1 {
		t.Fatalf("decoded %+v", got)
	}
	if _, err := EncodeResponse(Response{}); !errors.Is(err, ErrFraming) {
		t.Fatalf("response without code encoded: %v", err)
	}
	for name, body := range map[string][]byte{
		"no code":       []byte(`{"message":"x"}`),
		"unknown field": []byte(`{"code":"ok","extra":1}`),
		"trailing":      []byte(`{"code":"ok"} {"code":"ok"}`),
	} {
		if _, err := DecodeResponse(ProtocolVersion, body); !errors.Is(err, ErrFraming) {
			t.Errorf("%s: err = %v, want ErrFraming", name, err)
		}
	}
	if _, err := DecodeResponse(ProtocolVersion+1, b); !errors.Is(err, ErrFraming) {
		t.Errorf("wrong version accepted: %v", err)
	}
}

func TestOutcomeTerminal(t *testing.T) {
	for _, s := range []string{"RETIRED", "REFUSED_IDENTITY", "VOID", "FAILED", "ANOMALY"} {
		if !(Outcome{State: s}).Terminal() {
			t.Errorf("%s not terminal", s)
		}
	}
	for _, s := range []string{"REGISTERED", "VERIFY", "ORDINARY", "BACKOFF", "IN_KERNEL", "WAIT_NESTED", "FORCE", ""} {
		if (Outcome{State: s}).Terminal() {
			t.Errorf("%s terminal", s)
		}
	}
}

func TestMaxBodyIsEnforced(t *testing.T) {
	// Pinned literally: the limit bounds what a same-uid peer can make the
	// root daemon buffer per message, so raising it is a reviewed change.
	const limit = 64 << 10
	base, err := EncodeRequest(Request{Op: OpPing})
	if err != nil {
		t.Fatal(err)
	}
	pad := func(n int) []byte { // a valid body of exactly n bytes (JSON allows trailing space)
		return append(append([]byte{}, base...), bytes.Repeat([]byte(" "), n-len(base))...)
	}
	if _, err := DecodeRequest(ProtocolVersion, pad(limit)); err != nil {
		t.Fatalf("a %d-byte body was refused: %v", limit, err)
	}
	if _, err := DecodeRequest(ProtocolVersion, pad(limit+1)); !errors.Is(err, ErrFraming) || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("a %d-byte body: err = %v, want the size limit", limit+1, err)
	}
	if _, err := DecodeResponse(ProtocolVersion, append([]byte(`{"code":"ok"}`), bytes.Repeat([]byte(" "), limit)...)); !errors.Is(err, ErrFraming) {
		t.Fatalf("oversized response accepted: %v", err)
	}
	if _, err := EncodeResponse(Response{Code: CodeInternal, Message: strings.Repeat("x", limit)}); !errors.Is(err, ErrFraming) {
		t.Fatalf("oversized response encoded: %v", err)
	}
}
