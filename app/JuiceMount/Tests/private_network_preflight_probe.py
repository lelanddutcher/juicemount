#!/usr/bin/env python3
"""Compile and exercise the production Redis preflight parser in isolation.

The parser is embedded in a SwiftUI executable target without a test target.
Extracting its exact source keeps this probe independent of an app launch,
network activity, package resolution, and user preferences.
"""

from pathlib import Path
import os
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "app/JuiceMount/Sources/JuiceMount/UI/OnboardingWindowView.swift"
BUILD = Path(os.environ["JM_PRIVATE_NETWORK_BUILD_CACHE"])
BUILD.mkdir(parents=True, exist_ok=True)

source = SOURCE.read_text()
start = source.index("    enum ParseResult {")
end = source.index("    /// Short TCP dial", start)
parser = source[start:end]

swift = """import Foundation

enum OnboardingPreflight {
""" + parser + """
}

@main
struct Probe {
    static func main() {
        var failures = 0
        func expectFailure(_ input: String, _ label: String) {
            switch OnboardingPreflight.parseRedisHostPort(input) {
            case .success(let target):
                print("FAIL \\(label): accepted \\(target.0):\\(target.1)")
                failures += 1
            case .failure:
                print("PASS \\(label)")
            }
        }
        expectFailure("redis://nas.tailnet:0/1", "zero port")
        expectFailure("redis://nas.tailnet:70000/1", "out-of-range port")
        expectFailure("http://nas.tailnet:6379/1", "unsupported HTTP scheme")

        switch OnboardingPreflight.parseRedisHostPort("redis://user:synthetic-secret@nas.tailnet:bad/1") {
        case .success:
            print("FAIL malformed URL: accepted invalid port")
            failures += 1
        case .failure(let detail):
            if detail.contains("synthetic-secret") {
                print("FAIL malformed URL: diagnostic exposed credentials")
                failures += 1
            } else {
                print("PASS malformed URL redaction")
            }
        }

        switch OnboardingPreflight.parseRedisHostPort("redis://nas.tailnet:6379/1") {
        case .success(let target) where target.0 == "nas.tailnet" && target.1 == 6379:
            print("PASS valid private Redis target")
        default:
            print("FAIL valid private Redis target")
            failures += 1
        }
        switch OnboardingPreflight.parseRedisHostPort("rediss://nas.tailnet:6379/1") {
        case .success(let target) where target.0 == "nas.tailnet" && target.1 == 6379:
            print("PASS valid TLS Redis syntax")
        default:
            print("FAIL valid TLS Redis syntax")
            failures += 1
        }
        switch OnboardingPreflight.parseRedisHostPort("nas.tailnet:6379/1") {
        case .success(let target) where target.0 == "nas.tailnet" && target.1 == 6379:
            print("PASS valid bare private Redis target")
        default:
            print("FAIL valid bare private Redis target")
            failures += 1
        }
        switch OnboardingPreflight.parseRedisHostPort("redis://[fd7a:115c:a1e0::1]:6379/1") {
        case .success(let target) where target.1 == 6379:
            print("PASS valid IPv6 private Redis target: \\(target.0)")
        default:
            print("FAIL valid IPv6 private Redis target")
            failures += 1
        }
        print("RESULT declared=8 executed=8 failures=\\(failures)")
        if failures != 0 { exit(1) }
    }
}
"""

probe = BUILD / "OnboardingPreflightProbe.swift"
binary = BUILD / "OnboardingPreflightProbe"
probe.write_text(swift)
subprocess.run([
    "swiftc", "-parse-as-library", "-module-cache-path", str(BUILD / "swift-module-cache"),
    str(probe), "-o", str(binary)
], check=True)
sys.exit(subprocess.run([str(binary)]).returncode)
