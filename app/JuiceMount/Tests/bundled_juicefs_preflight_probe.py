#!/usr/bin/env python3
"""Compile and exercise the onboarding BundledJuiceFS check in isolation.

The check is embedded in a SwiftUI executable target without a test target,
so its exact source (between the BEGIN/END BundledJuiceFS markers) is
extracted and driven against throwaway app directories. Nothing here runs a
real JuiceFS or touches a mount.

JM_PROBE_BUILD_DIR names a private writable directory (required).
"""

from pathlib import Path
import os
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "app/JuiceMount/Sources/JuiceMount/UI/OnboardingWindowView.swift"
BUILD = Path(os.environ["JM_PROBE_BUILD_DIR"]).resolve()
BUILD.mkdir(parents=True, exist_ok=True)
PINNED = "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9-jm1"

source = SOURCE.read_text()
start = source.index("// BEGIN BundledJuiceFS")
end = source.index("// END BundledJuiceFS")
probe = BUILD / "BundledJuiceFSProbe.swift"
probe.write_text("import Foundation\nimport Security\n\n" + source[start:end] + """
@main
struct Probe {
    static func main() {
        let args = CommandLine.arguments
        let expected = args[2].isEmpty ? nil : args[2]
        switch BundledJuiceFS.check(executable: URL(fileURLWithPath: args[1]), expectedVersion: expected) {
        case .success(let path): print("OK " + path)
        case .failure(let problem): print("FAIL " + problem.message)
        }
    }
}
""")
binary = BUILD / "BundledJuiceFSProbe"
subprocess.run(["swiftc", "-parse-as-library", "-module-cache-path", str(BUILD / "swift-module-cache"),
                str(probe), "-o", str(binary)], check=True)


def sign(path):
    subprocess.run(["codesign", "--force", "--sign", "-", str(path)], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def bundle(name, version=PINNED, signed=True, present=True):
    macos = BUILD / name / "Contents/MacOS"
    shutil.rmtree(BUILD / name, ignore_errors=True)
    macos.mkdir(parents=True)
    app = macos / "JuiceMount"
    shutil.copyfile("/usr/bin/true", app)
    app.chmod(0o755)
    sign(app)
    if present:
        engine = macos / "juicefs"
        engine.write_text("#!/bin/sh\necho '%s'\n" % version)
        engine.chmod(0o755)
        if signed:
            sign(engine)
    return app


def run(app, expected=PINNED, path=None):
    env = dict(os.environ)
    if path:
        env["PATH"] = path
    return subprocess.run([str(binary), str(app), expected or ""], env=env, text=True,
                          stdout=subprocess.PIPE, check=True).stdout.strip()


failures = 0


def expect(label, got, prefix):
    global failures
    if not got.startswith(prefix):
        print("FAIL %s: %s" % (label, got))
        failures += 1


good = bundle("good")
expect("pinned, same signer", run(good), "OK " + str(good.parent / "juicefs"))
# A juicefs on PATH (as Homebrew installs) must never stand in for the bundle.
on_path = BUILD / "path"
on_path.mkdir(exist_ok=True)
fake = on_path / "juicefs"
fake.write_text("#!/bin/sh\necho '%s'\n" % PINNED)
fake.chmod(0o755)
expect("missing bundle, juicefs on PATH", run(bundle("missing", present=False), path=str(on_path)),
       "FAIL The bundled JuiceFS engine is missing")
expect("unpinned version", run(bundle("unpinned", version="juicefs version 1.3.1+2025-12-02.e0032b2ae5e9")),
       "FAIL The bundled JuiceFS engine is not the version")
expect("unsigned engine", run(bundle("unsigned", signed=False)), "FAIL The bundled JuiceFS engine is not signed")
expect("no recorded version", run(good, expected=None), "FAIL This copy of JuiceMount does not record")
print("RESULT declared=5 failures=%d" % failures)
sys.exit(1 if failures else 0)
