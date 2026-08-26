package main

import "testing"

func TestValidateLinkAdminKey(t *testing.T) {
	strong := "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name        string
		linkEnabled bool
		adminKey    string
		wantErr     bool
	}{
		{name: "local development may disable auth", linkEnabled: false, adminKey: ""},
		{name: "link refuses empty key", linkEnabled: true, adminKey: "", wantErr: true},
		{name: "link refuses whitespace key", linkEnabled: true, adminKey: "   ", wantErr: true},
		{name: "link refuses short key", linkEnabled: true, adminKey: "short-but-non-empty", wantErr: true},
		{name: "link accepts strong key", linkEnabled: true, adminKey: strong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateLinkAdminKey(tc.linkEnabled, tc.adminKey)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateLinkAdminKey() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
