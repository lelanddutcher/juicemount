package servicecontrol

import (
	"context"
	"testing"
	"time"
)

func TestServerCloseJoinsOriginalHandlerAndReply(t *testing.T) {
	path := testPath(t)
	s, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	served := make(chan error, 1)
	go func() {
		served <- s.Serve(func(Request) Response {
			close(entered)
			<-release
			return Response{Text: "original"}
		})
	}()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = s.Close()
		select {
		case <-served:
		case <-time.After(time.Second):
			t.Error("Serve not joined")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.WaitClosed(ctx); err != nil {
			t.Errorf("original handlers not joined: %v", err)
		}
		// Retain the actual completion channel even when a negative control
		// deliberately disables WaitClosed's public wait.
		select {
		case <-s.drained:
		case <-ctx.Done():
			t.Error("original transport cleanup not physically joined")
		}
	}()
	client := make(chan error, 1)
	clientJoined := false
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := Call(ctx, path, Request{Version: ProtocolVersion, Build: "fixture", Method: "original"})
		if err == nil && resp.Text != "original" {
			err = ErrClosed
		}
		client <- err
	}()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		if !clientJoined {
			select {
			case <-client:
			case <-time.After(2 * time.Second):
				t.Error("original client not joined")
			}
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler not entered")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := s.WaitClosed(ctx); err == nil {
		t.Fatal("Close claimed physically held handler joined")
	}
	cancel()
	close(release)
	select {
	case err := <-client:
		clientJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reply not joined")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
}
