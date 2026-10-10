package main

import "testing"

func TestDefaultProducerVersionTracksExactReleaseBuild(t *testing.T) {
	const a = "35c4b320bac5efaa087a2c12773493172a20d2fb"
	const b = "5c7962bd5431e13953865d8a46ae0b53a3c78af3"
	av, bv := defaultProducerVersion(a), defaultProducerVersion(b)
	if av <= 1 || bv <= 1 || av == bv {
		t.Fatalf("release generations = %d, %d; want distinct values above legacy version 1", av, bv)
	}
	if defaultProducerVersion(a) != av {
		t.Fatal("the same immutable commit produced a different generation")
	}
	for _, bad := range []string{"", "dev", "not-a-40-character-release-commit-value!!", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"} {
		if got := defaultProducerVersion(bad); got != 1 {
			t.Errorf("development/invalid commit %q generation = %d, want legacy-safe 1", bad, got)
		}
	}
}
