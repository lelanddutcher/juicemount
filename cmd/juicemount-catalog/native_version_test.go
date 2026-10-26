package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/version"
)

func TestNativeHelperVersionWithoutProfileOrCatalog(t *testing.T) {
	var output, diagnostic bytes.Buffer
	code := run(context.Background(), []string{"native", "version"}, bytes.NewReader(nil), &output, &diagnostic)
	want, err := json.Marshal(struct {
		Version        int    `json:"version"`
		ProductVersion string `json:"product_version"`
		Commit         string `json:"commit"`
	}{1, version.Version, version.Commit})
	if err != nil || code != 0 || !bytes.Equal(output.Bytes(), append(want, '\n')) || diagnostic.Len() != 0 {
		t.Fatalf("native identity did not return exact live stamps: code=%d out=%s err=%s", code, output.Bytes(), diagnostic.Bytes())
	}
	for _, flag := range []string{"--catalog=", "--native-root=", "--endpoint=", "--library=", "--copy-id=", "--allow-loopback-http"} {
		t.Run(flag, func(t *testing.T) {
			output.Reset()
			diagnostic.Reset()
			code := run(context.Background(), []string{flag, "native", "version"}, bytes.NewReader(nil), &output, &diagnostic)
			if code != 3 || output.Len() != 0 {
				t.Fatal("identity accepted unrelated authority flags")
			}
			nativeTestDiagnostic(t, diagnostic.Bytes(), "invalid", false)
		})
	}
}

func TestNativeHelperVersionOutputFailureIsUncertain(t *testing.T) {
	var output shortDeliveryWriter
	var diagnostic bytes.Buffer
	code := run(context.Background(), []string{"native", "version"}, bytes.NewReader(nil), &output, &diagnostic)
	if code != 3 {
		t.Fatal("partial build identity output claimed success")
	}
	nativeTestDiagnostic(t, diagnostic.Bytes(), "uncertain", true)
}
