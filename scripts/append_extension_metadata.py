#!/usr/bin/env -S python3 -I
"""Appends the DuckDB extension metadata footer to a loadable extension binary.

The Rust counterpart of `scripts/append_metadata.cmake` (what `build_loadable_extension_capi` runs after
linking): a Rust extension crate built as a cdylib becomes a `<name>.duckdb_extension` by copying the
`.so` and appending this footer, which `LOAD` reads (`ParsedExtensionMetaData`) and the signature check
covers.

Footer layout (`ParsedExtensionMetaData::FOOTER_SIZE` = 512 bytes, preceded by the WebAssembly custom
section header the engine skips over):

    1 byte   0x00                            custom section id
    2 bytes  0x93 0x04                       LEB128 section length (531)
    1 byte   0x10                            name length (16)
    16 bytes "duckdb_signature"              section name
    2 bytes  0x80 0x04                       LEB128 payload length (512)
    8 x 32   metadata fields, written in reverse order (field 8 first, field 1 last), NUL padded:
             1: "4" (magic)   2: platform   3: DuckDB version (CPP / C_STRUCT_UNSTABLE) or
             C API version (C_STRUCT)   4: extension version   5: ABI type   6-8: unused
    256 bytes signature (zeros: unsigned)

Usage:
    append_extension_metadata.py --extension demo_capi.duckdb_extension --platform linux_arm64
        --abi-type C_STRUCT_UNSTABLE --version <duckdb version tag> --extension-version v0.0.1

`--platform` defaults to the running platform in DuckDB's naming (linux_amd64, linux_arm64, osx_arm64,
...). `--abi-type C_STRUCT_UNSTABLE` with the DuckDB version tag of the engine (the release tag, else the source
id: `SELECT source_id FROM pragma_version()`, `ExtensionHelper::GetVersionDirectoryName()`) ties the extension to
that engine build, what upstream's `build_loadable_extension_capi_unstable` records; it is the only footer a C API
v2 extension can carry since no V2 function is stable yet (upstream #27410): `--abi-type C_STRUCT` with a 2.x C API
version such as `v2.0.0` is refused by the engine with "the V2 C API has no stable surface yet" (`C_STRUCT` is
for C API v1 extensions). A footer already present is replaced when `--replace` is given.
"""
import argparse
import os
import platform
import sys

FOOTER_SIZE = 512
SIGNATURE_SIZE = 256
FIELD_SIZE = 32
FIELD_COUNT = 8
MAGIC = "4"
ABI_TYPES = ("C_STRUCT", "C_STRUCT_UNSTABLE", "CPP")

# WebAssembly custom section header: id 0, length 531 (LEB128), name length 16, name, payload length 512.
SECTION_HEADER = b"\x00" + b"\x93\x04" + b"\x10" + b"duckdb_signature" + b"\x80\x04"


def duckdb_platform():
    system = {"Linux": "linux", "Darwin": "osx", "Windows": "windows"}.get(platform.system(), platform.system().lower())
    machine = platform.machine().lower()
    arch = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(machine, machine)
    return "%s_%s" % (system, arch)


def field(text):
    data = text.encode("utf-8")
    if len(data) > FIELD_SIZE:
        raise SystemExit("metadata field %r is longer than %d bytes" % (text, FIELD_SIZE))
    return data + b"\x00" * (FIELD_SIZE - len(data))


def build_footer(platform_name, version, extension_version, abi_type):
    fields = [MAGIC, platform_name, version, extension_version, abi_type, "", "", ""]
    assert len(fields) == FIELD_COUNT
    payload = b"".join(field(f) for f in reversed(fields)) + b"\x00" * SIGNATURE_SIZE
    assert len(payload) == FOOTER_SIZE
    return SECTION_HEADER + payload


def has_footer(data):
    if len(data) < FOOTER_SIZE:
        return False
    footer = data[-FOOTER_SIZE:]
    magic = footer[(FIELD_COUNT - 1) * FIELD_SIZE:FIELD_COUNT * FIELD_SIZE].rstrip(b"\x00")
    return magic == MAGIC.encode()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--extension", required=True, help="the .duckdb_extension file to append to (in place)")
    parser.add_argument("--platform", default=duckdb_platform(), help="DuckDB platform name (default: this machine)")
    parser.add_argument("--abi-type", default="C_STRUCT", choices=ABI_TYPES)
    parser.add_argument("--version", required=True,
                        help="C API version (C_STRUCT, e.g. v2.0.0) or DuckDB version tag (C_STRUCT_UNSTABLE / CPP)")
    parser.add_argument("--extension-version", required=True, help="what duckdb_extensions() reports")
    parser.add_argument("--replace", action="store_true", help="replace a footer that is already present")
    args = parser.parse_args()

    with open(args.extension, "rb") as f:
        data = f.read()
    if has_footer(data):
        if not args.replace:
            raise SystemExit("%s already carries a metadata footer (pass --replace to overwrite it)" % args.extension)
        data = data[:-(FOOTER_SIZE + len(SECTION_HEADER))] if data[-(FOOTER_SIZE + len(SECTION_HEADER)):-FOOTER_SIZE] == SECTION_HEADER else data[:-FOOTER_SIZE]
    footer = build_footer(args.platform, args.version, args.extension_version, args.abi_type)
    with open(args.extension, "wb") as f:
        f.write(data)
        f.write(footer)
    print("%s: appended %d bytes of metadata (%s, %s, %s, %s)" % (
        os.path.basename(args.extension), len(footer), args.platform, args.abi_type, args.version,
        args.extension_version))
    return 0


if __name__ == "__main__":
    sys.exit(main())
