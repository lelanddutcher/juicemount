package main

import "testing"

func TestValidateAdminKey(t *testing.T) {
	strong := "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name        string
		linkEnabled bool
		adminKey    string
		wantErr     bool
	}{
		{name: "local development may disable auth", linkEnabled: false, adminKey: ""},
		{name: "local development refuses whitespace auth", linkEnabled: false, adminKey: "   ", wantErr: true},
		{name: "local development refuses short supplied key", linkEnabled: false, adminKey: "short-but-non-empty", wantErr: true},
		{name: "local development refuses placeholder", linkEnabled: false, adminKey: "CHANGEME_ADMIN_KEY_01234567890123456789", wantErr: true},
		{name: "local development accepts strong supplied key", linkEnabled: false, adminKey: strong},
		{name: "link refuses empty key", linkEnabled: true, adminKey: "", wantErr: true},
		{name: "link refuses whitespace key", linkEnabled: true, adminKey: "   ", wantErr: true},
		{name: "link refuses short key", linkEnabled: true, adminKey: "short-but-non-empty", wantErr: true},
		{name: "link accepts strong key", linkEnabled: true, adminKey: strong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAdminKey(tc.linkEnabled, tc.adminKey)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateAdminKey() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
