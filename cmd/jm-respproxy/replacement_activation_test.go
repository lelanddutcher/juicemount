package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

func TestProxyCLIStageBirthConfig(t *testing.T) {
	for _, tc := range []struct {
		name, raw        string
		birth, singleton bool
		mode             respproxy.Mode
	}{
		{"legacy-absent", `{}`, false, true, respproxy.ModeEnforce},
		{"legacy-null", `{"stage_birth":null}`, false, true, respproxy.ModeEnforce},
		{"explicit-off", `{"stage_birth":false}`, false, true, respproxy.ModeEnforce},
		{"explicit-on", `{"stage_birth":true,"volume_db":3}`, true, true, respproxy.ModeEnforce},
		{"observe-override", `{"stage_birth":true,"mode":"observe"}`, true, true, respproxy.ModeObserve},
		{"no-singleton", `{"stage_birth":true,"singleton":false}`, true, false, respproxy.ModeEnforce},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := fileConfig{Mode: "enforce", Upstream: "127.0.0.1:1"}
			dec := json.NewDecoder(strings.NewReader(tc.raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&fc); err != nil {
				t.Fatal(err)
			}
			got, err := fc.proxyConfig()
			if err != nil {
				t.Fatal(err)
			}
			if got.StageBirth != tc.birth || got.Singleton != tc.singleton || got.Mode != tc.mode {
				t.Fatalf("mapping: birth=%t singleton=%t mode=%s", got.StageBirth, got.Singleton, got.Mode)
			}
			if tc.name == "explicit-on" && got.VolumeDB != 3 {
				t.Fatalf("volume DB=%d", got.VolumeDB)
			}
			p, err := respproxy.New(got)
			invalid := tc.birth && (!tc.singleton || tc.mode != respproxy.ModeEnforce)
			if (err != nil) != invalid {
				t.Fatalf("core admission=%v, want refusal=%t", err, invalid)
			}
			if p != nil {
				_ = p.Close()
			}
		})
	}
	for _, raw := range []string{`{"stage_birth":"true"}`, `{"stage_birth":1}`, `{"unknown_birth":true}`} {
		fc := fileConfig{Mode: "enforce"}
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&fc); err == nil {
			t.Fatalf("accepted malformed config %s", raw)
		}
	}
}

func TestProxyCLIReloadHandshakeRequiresSingleton(t *testing.T) {
	no := false
	if _, err := (fileConfig{Mode: "enforce", ReloadHandshake: true, Singleton: &no}).proxyConfig(); err == nil {
		t.Fatal("handshake without singleton was accepted")
	}
	if _, err := (fileConfig{Mode: "enforce", ReloadHandshake: true}).proxyConfig(); err != nil {
		t.Fatal(err)
	}
}
