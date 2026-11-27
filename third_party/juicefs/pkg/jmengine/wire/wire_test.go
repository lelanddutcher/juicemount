package wire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestRoundTrips(t *testing.T) {
	var b bytes.Buffer
	if err := WriteRequest(&b, Request{Op: OpGetAttr, Class: ClassMeta, Flags: 2, DeadlineMs: 900, Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if b.Len() != RequestHeaderLen+1 {
		t.Fatalf("request length %d", b.Len())
	}
	r, err := ReadRequest(&b)
	if err != nil || r.Op != OpGetAttr || r.Class != ClassMeta || r.Flags != 2 || r.DeadlineMs != 900 || string(r.Body) != "x" {
		t.Fatalf("request %+v %v", r, err)
	}
	if err := WriteReply(&b, Reply{Status: 2, Flags: 1, Body: []byte("yz")}); err != nil {
		t.Fatal(err)
	}
	p, err := ReadReply(&b)
	if err != nil || p.Status != 2 || p.Flags != 1 || string(p.Body) != "yz" {
		t.Fatalf("reply %+v %v", p, err)
	}
	if err := WriteStartMessage(&b, StartMessage{Token: "ab", MetaPassword: "p"}); err != nil {
		t.Fatal(err)
	}
	m, err := ReadStartMessage(&b)
	if err != nil || m.Token != "ab" || m.MetaPassword != "p" {
		t.Fatalf("start %+v %v", m, err)
	}
}

func TestBounds(t *testing.T) {
	if err := WriteRequest(&bytes.Buffer{}, Request{Body: make([]byte, MaxRequestBody+1)}); err != ErrTooLarge {
		t.Fatalf("large request written: %v", err)
	}
	h := make([]byte, RequestHeaderLen)
	binary.BigEndian.PutUint32(h[8:], MaxRequestBody+1)
	if _, err := ReadRequest(bytes.NewReader(h)); err != ErrTooLarge {
		t.Fatalf("large request read: %v", err)
	}
	h = make([]byte, ReplyHeaderLen)
	binary.BigEndian.PutUint32(h[8:], MaxReplyBody+1)
	if _, err := ReadReply(bytes.NewReader(h)); err != ErrTooLarge {
		t.Fatalf("large reply read: %v", err)
	}
	if _, err := ReadStartMessage(bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff})); err != ErrTooLarge {
		t.Fatalf("large start message read: %v", err)
	}
}
