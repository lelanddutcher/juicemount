//go:build !darwin || !cgo

package client

func dialXPC(service, requirement string) (Conn, error) { return nil, ErrUnsupported }
