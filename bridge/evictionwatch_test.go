package main

import (
	"testing"
	"time"
)

// TestEvictionRepairDecision pins the trigger contract: a large usage drop on
// a fast link, in-capacity, past cooldown → repair; every guard suppresses.
func TestEvictionRepairDecision(t *testing.T) {
	g := int64(1) << 30
	old := 100 * g
	cases := []struct {
		name       string
		prev, cur  int64
		fast, over bool
		since      time.Duration
		want       bool
	}{
		{"big drop fires", old, old - 20*g, true, false, time.Hour, true},
		{"exact threshold fires", old, old - 10*g, true, false, time.Hour, true}, // 10% of 100G
		{"small drop ignored", old, old - 1*g, true, false, time.Hour, false},
		{"growth ignored", old, old + 5*g, true, false, time.Hour, false},
		{"slow link suppressed", old, old - 50*g, false, false, time.Hour, false},
		{"over-capacity suppressed", old, old - 50*g, true, true, time.Hour, false},
		{"cooldown suppressed", old, old - 50*g, true, false, 5 * time.Minute, false},
		{"no prior sample", 0, 50 * g, true, false, time.Hour, false},
		{"small cache uses 2G floor", 4 * g, 4*g - 3*g, true, false, time.Hour, true},
		{"small cache under floor", 4 * g, 4*g - 1*g, true, false, time.Hour, false},
	}
	for _, c := range cases {
		if got := evictionRepairDecision(c.prev, c.cur, c.fast, c.over, c.since); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
