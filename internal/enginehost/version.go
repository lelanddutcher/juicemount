package enginehost

// ExpectedJuiceFSVersion is the version the bundled juicefs reports in the
// hello (its `juicefs version` text without the "juicefs version " prefix,
// from third_party/juicefs/SOURCE.json). The supervisor refuses any other
// helper. A change of the -jmN suffix must change this too; a test checks.
const ExpectedJuiceFSVersion = "1.3.1+2025-12-02.e0032b2ae5e9-jm23"
