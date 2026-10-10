package main

import (
	"strings"
	"testing"
)

// H-8B: the farm's proxy secret travels in META_PASSWORD, never in argv,
// and never in a log line.
func TestH8BJmfarmMetaPasswordFromEnv(t *testing.T) {
	t.Setenv("META_PASSWORD", "farm-pw-not-a-secret")
	if got := metaWithEnvPassword("redis://farm@respproxy:6379/1"); got != "redis://farm:farm-pw-not-a-secret@respproxy:6379/1" {
		t.Fatalf("dial URL = %q", got)
	}
	if got := metaWithEnvPassword("redis://farm:inline@respproxy:6379/1"); got != "redis://farm:inline@respproxy:6379/1" {
		t.Fatalf("an inline password was overridden: %q", got)
	}
	if got := redactMeta("redis://farm:farm-pw-not-a-secret@respproxy:6379/1"); strings.Contains(got, "farm-pw") {
		t.Fatalf("redacted URL leaks the password: %q", got)
	}
	t.Setenv("META_PASSWORD", "")
	if got := metaWithEnvPassword("redis://farm@respproxy:6379/1"); got != "redis://farm@respproxy:6379/1" {
		t.Fatalf("no env password changed the URL: %q", got)
	}
}
