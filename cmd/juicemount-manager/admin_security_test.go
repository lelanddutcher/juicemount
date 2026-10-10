package main

import "testing"

func TestValidateAdminKey(t *testing.T) {
	strong := "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name     string
		adminKey string
		wantErr  bool
	}{
		{name: "refuses empty key", adminKey: "", wantErr: true},
		{name: "refuses whitespace key", adminKey: "   ", wantErr: true},
		{name: "refuses short key", adminKey: "short-but-non-empty", wantErr: true},
		{name: "refuses placeholder", adminKey: "CHANGEME_ADMIN_KEY_01234567890123456789", wantErr: true},
		{name: "accepts strong key", adminKey: strong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAdminKey(tc.adminKey)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateAdminKey() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
