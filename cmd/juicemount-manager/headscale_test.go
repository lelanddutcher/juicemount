package main

import "testing"

func TestHeadscaleUserAlreadyExists(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  string
		want bool
	}{
		{name: "created", out: "User created", want: false},
		{name: "legacy already exists", out: "user jm already exists", want: true},
		{name: "grpc unique constraint", out: "Error: creating user: rpc error: code = Internal desc = creating user: constraint failed: UNIQUE constraint failed: users.name (2067)", want: true},
		{name: "unrelated constraint", out: "constraint failed: FOREIGN KEY constraint failed", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := headscaleUserAlreadyExists(tc.out); got != tc.want {
				t.Fatalf("headscaleUserAlreadyExists(%q) = %v, want %v", tc.out, got, tc.want)
			}
		})
	}
}
