package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
)

var errHTTPGracefulShutdownUnavailable = errors.New("HTTP graceful shutdown was not completed")

// ServeErrors reports unexpected termination of either successfully bound
// listener. The two-slot channel cannot block either original Serve worker.
// It remains open: absence of a failure must not wake callers as a nil error.
func (s *Server) ServeErrors() <-chan error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.serveErrors == nil {
		s.serveErrors = make(chan error, 2)
	}
	return s.serveErrors
}

// startServe runs with lifecycleMu held, before Start returns.
func (s *Server) startServe(listener net.Listener) {
	done := make(chan struct{})
	s.serveDone = append(s.serveDone, done)
	server, failures := s.httpSrv, s.serveErrors
	go func() {
		defer close(done)
		err := server.Serve(listener)
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		s.lifecycleMu.Lock()
		stopping := s.stopping
		s.lifecycleMu.Unlock()
		if !stopping {
			failures <- err
		}
	}()
}

// StopAndWait closes admission and joins the original Serve workers. Closing
// HTTP connections does not prove all legacy application handlers have drained;
// each application service must own its own request/resource drain.
func (s *Server) StopAndWait(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.stopping = true
		server, primary, secondary := s.httpSrv, s.listener, s.listener6
		s.lifecycleMu.Unlock()
		if server != nil {
			s.stopErr = server.Close()
		}
		// Also close listeners not yet adopted by their Serve goroutine.
		for _, listener := range []net.Listener{primary, secondary} {
			if listener != nil {
				if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					s.stopErr = errors.Join(s.stopErr, err)
				}
			}
		}
	})
	s.lifecycleMu.Lock()
	done := append([]<-chan struct{}(nil), s.serveDone...)
	s.lifecycleMu.Unlock()
	for _, worker := range done {
		select {
		case <-worker:
		case <-ctx.Done():
			return errors.Join(s.stopErr, ctx.Err())
		}
	}
	return s.stopErr
}

// ShutdownAndWait proves a successful graceful HTTP drain and joins every
// original Serve worker. A timed-out Shutdown reports failure even after the
// forced Close fallback; Serve return alone cannot certify active handlers.
func (s *Server) ShutdownAndWait(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.stopping = true
		server, primary, secondary := s.httpSrv, s.listener, s.listener6
		s.lifecycleMu.Unlock()
		if server != nil {
			s.stopErr = server.Shutdown(ctx)
			if s.stopErr == nil {
				s.lifecycleMu.Lock()
				s.graceful = true
				s.lifecycleMu.Unlock()
			}
			if s.stopErr != nil {
				s.stopErr = errors.Join(s.stopErr, server.Close())
			}
		} else {
			s.lifecycleMu.Lock()
			s.graceful = true // Nothing was ever served.
			s.lifecycleMu.Unlock()
		}
		for _, listener := range []net.Listener{primary, secondary} {
			if listener != nil {
				if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					s.stopErr = errors.Join(s.stopErr, err)
				}
			}
		}
	})
	s.lifecycleMu.Lock()
	done := append([]<-chan struct{}(nil), s.serveDone...)
	graceful := s.graceful
	s.lifecycleMu.Unlock()
	for _, worker := range done {
		select {
		case <-worker:
		case <-ctx.Done():
			return errors.Join(s.stopErr, ctx.Err())
		}
	}
	if !graceful {
		return errors.Join(s.stopErr, errHTTPGracefulShutdownUnavailable)
	}
	return s.stopErr
}

func (s *Server) stopAndWaitUnbounded() { _ = s.StopAndWait(context.Background()) }
