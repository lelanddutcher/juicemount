#!/usr/bin/env python3
"""Refuse a packaged JuiceMount without the exact macOS 26.0 minimum.

Inspect every Mach-O slice directly. Sparkle is third-party code and has its
own deployment policy, so only the six first-party executables are listed.
"""

import plistlib
import struct
import sys
from pathlib import Path


EXECUTABLES = (
    "Contents/MacOS/JuiceMount",
    "Contents/MacOS/juicemount-service",
    "Contents/MacOS/juicemount-mount-probe",
    "Contents/MacOS/juicemount-unmountd",
    "Contents/MacOS/juicefs",
    "Contents/PlugIns/JuiceMountThumbnails.appex/Contents/MacOS/JuiceMountThumbnails",
)
PLISTS = (
    "Contents/Info.plist",
    "Contents/PlugIns/JuiceMountThumbnails.appex/Contents/Info.plist",
)
THIN_MAGICS = {
    b"\xce\xfa\xed\xfe": ("<", 28),  # 32-bit little endian
    b"\xfe\xed\xfa\xce": (">", 28),
    b"\xcf\xfa\xed\xfe": ("<", 32),  # 64-bit little endian
    b"\xfe\xed\xfa\xcf": (">", 32),
}
FAT_MAGICS = {
    b"\xca\xfe\xba\xbe": (">", 20),
    b"\xbe\xba\xfe\xca": ("<", 20),
    b"\xca\xfe\xba\xbf": (">", 32),
    b"\xbf\xba\xfe\xca": ("<", 32),
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def version(packed):
    return (packed >> 16, (packed >> 8) & 255, packed & 255)


def check_slice(data, start, size):
    require(size >= 32, "Mach-O slice is truncated")
    magic = data[start:start + 4]
    require(magic in THIN_MAGICS, "missing or invalid Mach-O slice")
    endian, header_size = THIN_MAGICS[magic]
    require(size >= header_size, "Mach-O header is truncated")
    cpu, subtype, filetype, ncmds, commands_size = struct.unpack_from(endian + "IIIII", data, start + 4)
    require(filetype == 2, "Mach-O slice is not an executable")
    require(ncmds > 0 and commands_size >= 8 * ncmds, "Mach-O load commands are missing or malformed")
    end = start + header_size + commands_size
    require(end <= start + size, "Mach-O load commands exceed slice")
    pos = start + header_size
    minimums = []
    for _ in range(ncmds):
        require(pos + 8 <= end, "Mach-O load command is truncated")
        command, length = struct.unpack_from(endian + "II", data, pos)
        require(length >= 8 and pos + length <= end, "Mach-O load command has invalid size")
        if command == 0x32:  # LC_BUILD_VERSION
            require(length >= 24, "LC_BUILD_VERSION is truncated")
            platform, minos = struct.unpack_from(endian + "II", data, pos + 8)
            require(platform == 1, "LC_BUILD_VERSION is not macOS")
            minimums.append(version(minos))
        elif command == 0x24:  # LC_VERSION_MIN_MACOSX
            require(length >= 16, "LC_VERSION_MIN_MACOSX is truncated")
            minimums.append(version(struct.unpack_from(endian + "I", data, pos + 8)[0]))
        pos += length
    require(pos == end, "Mach-O load-command size is inconsistent")
    require(len(minimums) == 1, "Mach-O slice has missing or conflicting minimum OS metadata")
    require(minimums[0] == (26, 0, 0),
            f"Mach-O minimum is {'.'.join(map(str, minimums[0]))}, expected 26.0.0")
    return cpu, subtype


def check_macho(path):
    data = path.read_bytes()
    require(len(data) >= 8, "file is too short for Mach-O")
    magic = data[:4]
    if magic in THIN_MAGICS:
        check_slice(data, 0, len(data))
        return
    require(magic in FAT_MAGICS, "file is not Mach-O")
    endian, arch_size = FAT_MAGICS[magic]
    count = struct.unpack_from(endian + "I", data, 4)[0]
    require(0 < count <= 32 and 8 + count * arch_size <= len(data), "fat architecture table is malformed")
    seen = set()
    regions = []
    for index in range(count):
        pos = 8 + index * arch_size
        cpu, subtype = struct.unpack_from(endian + "II", data, pos)
        if arch_size == 20:
            offset, size = struct.unpack_from(endian + "II", data, pos + 8)
        else:
            offset, size = struct.unpack_from(endian + "QQ", data, pos + 8)
        require(offset >= 8 + count * arch_size and size > 0 and offset + size <= len(data),
                f"fat slice {index} is outside the file")
        actual = check_slice(data, offset, size)
        require(actual == (cpu, subtype), f"fat slice {index} architecture does not match its table entry")
        require(actual not in seen, f"duplicate Mach-O architecture in slice {index}")
        seen.add(actual)
        regions.append((offset, offset + size))
    regions.sort()
    require(all(a[1] <= b[0] for a, b in zip(regions, regions[1:])), "fat Mach-O slices overlap")


def check_plist(path):
    with path.open("rb") as stream:
        value = plistlib.load(stream).get("LSMinimumSystemVersion")
    require(isinstance(value, str) and value in ("26.0", "26.0.0"),
            f"LSMinimumSystemVersion {value!r} is not exactly 26.0")


def check_app(app):
    failures = []
    for relative in PLISTS:
        try:
            check_plist(app / relative)
        except (OSError, ValueError, plistlib.InvalidFileException) as exc:
            failures.append(f"{relative}: {exc}")
    for relative in EXECUTABLES:
        try:
            check_macho(app / relative)
        except (OSError, ValueError, struct.error) as exc:
            failures.append(f"{relative}: {exc}")
    return failures


def main():
    if len(sys.argv) != 2:
        print("usage: verify-macos26-artifact.py JuiceMount.app", file=sys.stderr)
        return 2
    failures = check_app(Path(sys.argv[1]))
    if failures:
        for failure in failures:
            print(f"macOS 26 target failed: {failure}", file=sys.stderr)
        return 1
    print("minimum macOS 26.0: app, extension, and all first-party Mach-O slices")
    return 0


if __name__ == "__main__":
    sys.exit(main())
