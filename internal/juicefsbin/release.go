//go:build !jmdevjuicefs

package juicefsbin

// devAnyExplicitBinary is false in every build without the jmdevjuicefs tag,
// including every build-app.sh build.
const devAnyExplicitBinary = false
