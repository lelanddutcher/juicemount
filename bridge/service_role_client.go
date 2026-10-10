//go:build serviceclient

package main

// The desktop archive can only control the independent serving process.
// A missing service never falls back to owning storage in the GUI.
const serviceClientBuild = true
