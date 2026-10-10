package nfs

import (
	"sync"
	"testing"
)

// SetLogger is called for each top-level JuiceMount server while keep-awake and
// request goroutines may still be alive. Exercise the exact swap/read boundary
// under -race so the exported process-wide logger can never regress to a raw
// interface assignment.
func TestSetLoggerConcurrentWithLogging(t *testing.T) {
	original := globalLog.current()
	t.Cleanup(func() { SetLogger(original) })

	const iterations = 1_000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			logger := &DefaultLogger{}
			logger.SetLevel(ErrorLevel)
			SetLogger(logger)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = Log.GetLevel()
			Log.Debug("concurrent logger read")
		}
	}()
	wg.Wait()
}
