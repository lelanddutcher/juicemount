#!/usr/bin/env python3
"""Read the exact Go version string symbols in a thin arm64 Mach-O service.

This never executes the input. Only thin arm64 services whose two stamp
headers are declared single-start format-6 chained fixups are supported.
Other layouts fail closed, including direct pointers and universal binaries.
"""
import mmap
import struct
import sys
from pathlib import Path

MAGIC_64_LE = 0xFEEDFACF
CPU_ARM64 = 0x0100000C
MH_EXECUTE = 2
LC_SEGMENT_64 = 0x19
LC_SYMTAB = 0x2
LC_DYLD_CHAINED_FIXUPS = 0x80000034
PREFIX = b"_github.com/lelanddutcher/juicemount/internal/version."
WANTED = {
    PREFIX + b"Version.str": ("Version", "bytes"),
    PREFIX + b"Commit.str": ("Commit", "bytes"),
    PREFIX + b"Version": ("Version", "header"),
    PREFIX + b"Commit": ("Commit", "header"),
}


class StampError(ValueError):
    pass


def unpack(fmt, data, offset):
    size = struct.calcsize(fmt)
    if offset < 0 or offset + size > len(data):
        raise StampError("truncated Mach-O structure")
    return struct.unpack_from(fmt, data, offset)


def bounded_end(start, length, limit):
    if start < 0 or length < 0 or start > limit or length > limit - start:
        raise StampError("Mach-O range exceeds file")
    return start + length


def chained_target(data, fixups, segments, image_base, header_offset):
    """Return (chain member, decoded VM target) for one file-backed header.

    Supports only DYLD_CHAINED_PTR_64_OFFSET on a single-start page. Other
    formats and multi-start pages refuse; no dyld execution or broad loader.
    """
    if fixups is None:
        return False, None
    if image_base is None:
        raise StampError("missing image base for chained fixups")
    dataoff, datasize = fixups
    version, starts_rel = unpack("<II", data, dataoff)
    if version != 0 or starts_rel >= datasize:
        raise StampError("unsupported chained-fixups header")
    starts = dataoff + starts_rel
    seg_count, = unpack("<I", data, starts)
    if seg_count == 0 or seg_count > len(segments) or starts + 4 + 4 * seg_count > dataoff + datasize:
        raise StampError("invalid chained segment table")
    # Current linkers omit trailing nonmapped __DWARF from seg_count.
    for name, vmaddr, _fileoff, _filesize in segments[seg_count:]:
        if name != b"__DWARF" or vmaddr != 0:
            raise StampError("unexplained segment omitted from fixup table")
    matches = [i for i, (_name, _vmaddr, fileoff, filesize) in enumerate(segments[:seg_count])
               if fileoff <= header_offset < fileoff + filesize]
    if len(matches) != 1:
        raise StampError("stamp header has no unique fixup segment")
    index = matches[0]
    _name, vmaddr, fileoff, filesize = segments[index]
    info_rel, = unpack("<I", data, starts + 4 + 4 * index)
    if info_rel == 0:
        return False, None
    info = starts + info_rel
    size, page_size, pointer_format, segment_offset, _max_valid, page_count = unpack("<IHHQIH", data, info)
    if size < 22 or bounded_end(info, size, dataoff + datasize) > dataoff + datasize:
        raise StampError("invalid chained page starts")
    if page_size not in (4096, 16384) or pointer_format != 6 or size < 22 + 2 * page_count:
        raise StampError("unsupported chained pointer page format")
    if vmaddr != image_base + segment_offset:
        raise StampError("chained segment offset differs from Mach-O segment")
    page_index = (header_offset - fileoff) // page_size
    if page_index >= page_count:
        raise StampError("stamp header outside declared chained pages")
    page_start, = unpack("<H", data, info + 22 + 2 * page_index)
    if page_start == 0xFFFF:
        return False, None
    if page_start & 0x8000 or page_start >= page_size:
        raise StampError("unsupported chained multi-start page")
    page_file = fileoff + page_index * page_size
    current = page_file + page_start
    found = None
    for _ in range(page_size // 4 + 1):
        if current < page_file or current + 8 > min(page_file + page_size, fileoff + filesize, len(data)):
            raise StampError("chained pointer escapes file-backed page")
        raw, = unpack("<Q", data, current)
        if current == header_offset:
            if raw >> 63 or (raw >> 44) & 0x7F or (raw >> 36) & 0xFF:
                raise StampError("stamp header has unsupported bind/high bits")
            found = image_base + (raw & ((1 << 36) - 1))
        next_delta = (raw >> 51) & 0xFFF
        if next_delta == 0:
            return found is not None, found
        current += next_delta * 4
    raise StampError("chained page exceeds bounded traversal")


def symbol_stamps(data):
    if len(data) < 32:
        raise StampError("truncated Mach-O header")
    magic, cpu, _subtype, kind, ncmds, commands_size, _flags, _reserved = unpack("<IiiIIIII", data, 0)
    if magic != MAGIC_64_LE or cpu != CPU_ARM64 or kind != MH_EXECUTE:
        raise StampError("unsupported Mach-O: require thin little-endian arm64 executable")
    if ncmds == 0 or ncmds > 4096:
        raise StampError("invalid load-command count")
    commands_end = bounded_end(32, commands_size, len(data))
    offset = 32
    sections = []
    symtab = None
    text_base = None
    fixups = None
    segments = []
    for _ in range(ncmds):
        cmd, cmdsize = unpack("<II", data, offset)
        if cmdsize < 8 or bounded_end(offset, cmdsize, commands_end) > commands_end:
            raise StampError("invalid load-command size")
        if cmd == LC_SEGMENT_64:
            if cmdsize < 72:
                raise StampError("truncated segment command")
            (_cmd, _size, name, vmaddr, _vmsize, fileoff, filesize,
             _maxprot, _initprot, nsects, _segflags) = unpack("<II16sQQQQiiII", data, offset)
            if name.rstrip(b"\0") == b"__TEXT":
                if text_base is not None:
                    raise StampError("duplicate __TEXT segment")
                text_base = vmaddr
            bounded_end(fileoff, filesize, len(data))
            segments.append((name.rstrip(b"\0"), vmaddr, fileoff, filesize))
            if nsects > (cmdsize - 72) // 80:
                raise StampError("truncated section table")
            for i in range(nsects):
                section_offset = offset + 72 + i * 80
                _sect, _seg, addr, size, payload_offset = unpack("<16s16sQQI", data, section_offset)
                sections.append((addr, size, payload_offset, fileoff, filesize))
        elif cmd == LC_SYMTAB:
            if cmdsize != 24 or symtab is not None:
                raise StampError("ambiguous symbol table")
            _cmd, _size, symoff, nsyms, stroff, strsize = unpack("<IIIIII", data, offset)
            bounded_end(symoff, nsyms * 16, len(data))
            bounded_end(stroff, strsize, len(data))
            symtab = (symoff, nsyms, stroff, strsize)
        elif cmd == LC_DYLD_CHAINED_FIXUPS:
            if cmdsize != 16 or fixups is not None:
                raise StampError("ambiguous chained-fixups command")
            _cmd, _size, dataoff, datasize = unpack("<IIII", data, offset)
            bounded_end(dataoff, datasize, len(data))
            fixups = (dataoff, datasize)
        offset += cmdsize
    if offset != commands_end or symtab is None:
        raise StampError("invalid or missing Mach-O load commands")
    symoff, nsyms, stroff, strsize = symtab
    stamps = {}
    locations = {}
    addresses = {}
    headers = {}
    for i in range(nsyms):
        name_index, kind, section_index, _desc, address = unpack("<IBBHQ", data, symoff + 16 * i)
        if name_index >= strsize:
            raise StampError("symbol name outside string table")
        name_start = stroff + name_index
        name_end = data.find(b"\0", name_start, stroff + strsize)
        if name_end < 0:
            raise StampError("unterminated symbol name")
        wanted = WANTED.get(data[name_start:name_end])
        if wanted is None:
            continue
        label, role = wanted
        key = (label, role)
        if key in addresses:
            raise StampError("duplicate version stamp symbol")
        if kind & 0x0E != 0x0E or section_index == 0 or section_index > len(sections):
            raise StampError("version stamp is not section-defined")
        addr, size, payload_offset, fileoff, filesize = sections[section_index - 1]
        if not (addr <= address < addr + size) or payload_offset == 0:
            raise StampError("version stamp outside file-backed section")
        value_offset = payload_offset + address - addr
        if not (fileoff <= value_offset < fileoff + filesize):
            raise StampError("version stamp outside declared segment bytes")
        addresses[key] = address
        if role == "header":
            if value_offset + 16 > min(payload_offset + size, fileoff + filesize, len(data)):
                raise StampError("truncated Go string header")
            headers[label] = unpack("<QQ", data, value_offset)
            locations[label + "Header"] = value_offset
        else:
            end = min(value_offset + 256, payload_offset + size, fileoff + filesize, len(data))
            terminator = data.find(b"\0", value_offset, end)
            if terminator < 0:
                raise StampError("unterminated version stamp bytes")
            value = data[value_offset:terminator]
            if not value or any(b < 0x21 or b > 0x7E for b in value):
                raise StampError("invalid version stamp bytes")
            stamps[label] = value.decode("ascii")
            locations[label] = value_offset
    if set(stamps) != {"Version", "Commit"} or set(headers) != {"Version", "Commit"}:
        raise StampError("required service stamp symbol missing")
    for label, value in stamps.items():
        _pointer, length = headers[label]
        string_address = addresses[(label, "bytes")]
        member, decoded = chained_target(data, fixups, segments, text_base, locations[label + "Header"])
        if not member:
            raise StampError("Go Version/Commit header is not a declared format-6 chained fixup")
        if decoded != string_address or length != len(value):
            raise StampError("Go Version/Commit header does not bind exact string bytes")
    return stamps, locations


def inspect(path):
    with Path(path).open("rb") as stream:
        if stream.seek(0, 2) == 0:
            raise StampError("empty service binary")
        stream.seek(0)
        with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as data:
            return symbol_stamps(data)


def main(argv):
    if len(argv) != 4 or not argv[2] or not argv[3]:
        print("usage: verify-service-stamp.py SERVICE VERSION COMMIT", file=sys.stderr)
        return 3
    try:
        stamps, _ = inspect(argv[1])
    except (OSError, StampError, UnicodeError) as exc:
        print(f"service stamp inspection refused: {exc}", file=sys.stderr)
        return 1
    if stamps != {"Version": argv[2], "Commit": argv[3]}:
        print("service Version/Commit symbols do not match expected identity", file=sys.stderr)
        return 1
    print("service Version/Commit symbols match expected identity")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
