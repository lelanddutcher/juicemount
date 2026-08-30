package main

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestIsFarmStoragePressure(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("commit proxy: %w", syscall.ENOSPC), true},
		{fmt.Errorf("ffmpeg: Error writing trailer: Input/output error"), true},
		{fmt.Errorf("MISCONF Errors writing to the AOF file: No space left on device"), true},
		{errors.New("hardware decoder does not support prores"), false},
		{errors.New("ffmpeg exited with code 1"), false},
		{nil, false},
	}
	for _, tt := range tests {
		if got := isFarmStoragePressure(tt.err); got != tt.want {
			t.Errorf("isFarmStoragePressure(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestCheckWorkerStorageHeadroomUsesConfiguredReserve(t *testing.T) {
	headroom, err := checkWorkerStorageHeadroom(t.TempDir(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if headroom.Total == 0 || headroom.Required != 1 || headroom.Available == 0 {
		t.Fatalf("headroom = %+v", headroom)
	}
}

func TestCheckWorkerStorageHeadroomUsesPhysicalCapacityOverride(t *testing.T) {
	const capacity = uint64(100 << 40)
	headroom, err := checkWorkerStorageHeadroom(t.TempDir(), 0, capacity)
	if err != nil {
		t.Fatal(err)
	}
	if headroom.Total != capacity || headroom.Required != capacity/100 {
		t.Fatalf("headroom = %+v, want total=%d required=%d", headroom, capacity, capacity/100)
	}
}
