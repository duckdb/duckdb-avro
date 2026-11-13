//! The extension metadata footer: what turns a built cdylib into a `<name>.duckdb_extension` the loaders accept.
//!
//! The Rust counterpart of `scripts/append_metadata.cmake` (what `build_loadable_extension_capi_unstable` runs
//! after linking) and of `scripts/append_extension_metadata.py`: the engine reads the last
//! [`ExtensionMetadata::FOOTER_SIZE`] bytes of the file (`ParsedExtensionMetaData`), and the signature check covers
//! the rest.
//!
//! Footer layout (512 bytes, preceded by the WebAssembly custom section header the engine skips over):
//!
//! ```text
//! 1 byte   0x00                custom section id
//! 2 bytes  0x93 0x04           LEB128 section length (531)
//! 1 byte   0x10                name length (16)
//! 16 bytes "duckdb_signature"  section name
//! 2 bytes  0x80 0x04           LEB128 payload length (512)
//! 8 x 32   metadata fields, written in reverse order (field 8 first, field 1 last), NUL padded:
//!          1: "4" (magic)  2: platform  3: DuckDB version (CPP / C_STRUCT_UNSTABLE) or C API version (C_STRUCT)
//!          4: extension version  5: ABI type  6-8: unused
//! 256 bytes signature (zeros: unsigned)
//! ```

use std::io::Write;
use std::path::Path;

/// The ABI an extension was built against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbiType {
    /// The C API with a `vX.Y.Z` C API version: loadable by any engine implementing that C API major version.
    CStruct,
    /// The C API pinned to one DuckDB version.
    CStructUnstable,
    /// The C++ ABI (never produced by a Rust extension).
    Cpp,
}

impl AbiType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AbiType::CStruct => "C_STRUCT",
            AbiType::CStructUnstable => "C_STRUCT_UNSTABLE",
            AbiType::Cpp => "CPP",
        }
    }
}

/// The fields of the footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionMetadata {
    /// DuckDB's platform name (`linux_amd64`, `linux_arm64`, `osx_arm64`, ...).
    pub platform: String,
    pub abi_type: AbiType,
    /// The C API version (`C_STRUCT`, e.g. `v2.0.0`) or the DuckDB version tag (`C_STRUCT_UNSTABLE` / `CPP`).
    pub version: String,
    /// What `duckdb_extensions()` reports as the extension version.
    pub extension_version: String,
}

impl ExtensionMetadata {
    pub const FOOTER_SIZE: usize = 512;
    pub const SIGNATURE_SIZE: usize = 256;
    pub const FIELD_SIZE: usize = 32;
    pub const FIELD_COUNT: usize = 8;
    pub const MAGIC: &'static str = "4";
    /// WebAssembly custom section header: id 0, length 531 (LEB128), name length 16, name, payload length 512.
    pub const SECTION_HEADER: &'static [u8] = b"\x00\x93\x04\x10duckdb_signature\x80\x04";

    /// A C API v2 extension pinned to one engine build (`C_STRUCT_UNSTABLE` with the engine's version tag, what
    /// `build_loadable_extension_capi_unstable` records) for the running platform. No V2 function is stable yet, so
    /// this is the only footer a V2 extension can carry: a `C_STRUCT` footer with a 2.x C API version is refused by
    /// the loader ("the V2 C API has no stable surface yet"). The tag is [`ExtensionMetadata::version_directory_name`]
    /// of the engine that is to load the extension.
    pub fn pinned(version_tag: &str, extension_version: &str) -> ExtensionMetadata {
        ExtensionMetadata {
            platform: current_platform(),
            abi_type: AbiType::CStructUnstable,
            version: version_tag.to_string(),
            extension_version: extension_version.to_string(),
        }
    }

    /// `ExtensionHelper::GetVersionDirectoryName()` of an engine reporting `library_version` and `source_id`
    /// (`pragma_version()`): the normalized release tag (`v`-prefixed) for a release, else the source id of the
    /// build. What a pinned (`C_STRUCT_UNSTABLE`) footer must carry to load into that engine.
    pub fn version_directory_name(library_version: &str, source_id: &str) -> String {
        if library_version.contains("-dev") {
            return source_id.to_string();
        }
        if !library_version.is_empty() && !library_version.starts_with('v') {
            return format!("v{library_version}");
        }
        library_version.to_string()
    }

    /// The footer bytes: the section header, the eight fields in reverse order and an all-zero (unsigned)
    /// signature. Errors when a field does not fit its 32 bytes.
    pub fn footer(&self) -> Result<Vec<u8>, String> {
        let fields = [
            ExtensionMetadata::MAGIC,
            self.platform.as_str(),
            self.version.as_str(),
            self.extension_version.as_str(),
            self.abi_type.as_str(),
            "",
            "",
            "",
        ];
        let mut footer = Vec::with_capacity(ExtensionMetadata::SECTION_HEADER.len() + ExtensionMetadata::FOOTER_SIZE);
        footer.extend_from_slice(ExtensionMetadata::SECTION_HEADER);
        for field in fields.iter().rev() {
            let bytes = field.as_bytes();
            if bytes.len() > ExtensionMetadata::FIELD_SIZE {
                return Err(format!("metadata field {field:?} is longer than {} bytes", ExtensionMetadata::FIELD_SIZE));
            }
            footer.extend_from_slice(bytes);
            footer.resize(footer.len() + ExtensionMetadata::FIELD_SIZE - bytes.len(), 0);
        }
        footer.resize(footer.len() + ExtensionMetadata::SIGNATURE_SIZE, 0);
        debug_assert_eq!(footer.len(), ExtensionMetadata::SECTION_HEADER.len() + ExtensionMetadata::FOOTER_SIZE);
        Ok(footer)
    }

    /// Whether `data` (a whole file) already ends in a metadata footer (the magic field reads "4").
    pub fn has_footer(data: &[u8]) -> bool {
        if data.len() < ExtensionMetadata::FOOTER_SIZE {
            return false;
        }
        let footer = &data[data.len() - ExtensionMetadata::FOOTER_SIZE..];
        let magic = &footer[(ExtensionMetadata::FIELD_COUNT - 1) * ExtensionMetadata::FIELD_SIZE
            ..ExtensionMetadata::FOOTER_SIZE - ExtensionMetadata::SIGNATURE_SIZE];
        let end = magic.iter().rposition(|&b| b != 0).map(|p| p + 1).unwrap_or(0);
        &magic[..end] == ExtensionMetadata::MAGIC.as_bytes()
    }

    /// Copies the cdylib at `library` to `extension` and appends this footer, replacing a footer the source
    /// already carries.
    pub fn write_extension(&self, library: &Path, extension: &Path) -> Result<(), String> {
        let mut data = std::fs::read(library).map_err(|e| format!("cannot read {}: {e}", library.display()))?;
        if ExtensionMetadata::has_footer(&data) {
            let header_start = data.len() - ExtensionMetadata::FOOTER_SIZE - ExtensionMetadata::SECTION_HEADER.len();
            let footer_start = data.len() - ExtensionMetadata::FOOTER_SIZE;
            if &data[header_start..footer_start] == ExtensionMetadata::SECTION_HEADER {
                data.truncate(header_start);
            } else {
                data.truncate(footer_start);
            }
        }
        self.write_extension_data(&data, extension)
    }

    /// Writes `data` followed by this footer to `extension` (creating the directory), the extension file of a
    /// payload that is not a shared library on disk (a stand-in for an extension linked into the binary).
    pub fn write_extension_data(&self, data: &[u8], extension: &Path) -> Result<(), String> {
        let footer = self.footer()?;
        if let Some(parent) = extension.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
        }
        let mut file =
            std::fs::File::create(extension).map_err(|e| format!("cannot write {}: {e}", extension.display()))?;
        file.write_all(data)
            .and_then(|_| file.write_all(&footer))
            .map_err(|e| format!("cannot write {}: {e}", extension.display()))
    }
}

/// `DuckDB::Platform()` for the running binary: `<os>_<arch>` in DuckDB's naming.
pub fn current_platform() -> String {
    let os = match std::env::consts::OS {
        "linux" => "linux",
        "macos" => "osx",
        "windows" => "windows",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };
    format!("{os}_{arch}")
}

// kept: the extension metadata footer byte layout and the version directory name
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_layout() {
        let metadata = ExtensionMetadata {
            platform: "linux_amd64".to_string(),
            abi_type: AbiType::CStruct,
            version: "v2.0.0".to_string(),
            extension_version: "v0.0.1".to_string(),
        };
        let footer = metadata.footer().unwrap();
        assert_eq!(footer.len(), ExtensionMetadata::SECTION_HEADER.len() + ExtensionMetadata::FOOTER_SIZE);
        assert!(footer.starts_with(ExtensionMetadata::SECTION_HEADER));
        let payload = &footer[ExtensionMetadata::SECTION_HEADER.len()..];
        let field = |i: usize| {
            let idx = ExtensionMetadata::FIELD_COUNT - 1 - i;
            let bytes = &payload[idx * 32..(idx + 1) * 32];
            let end = bytes.iter().rposition(|&b| b != 0).map(|p| p + 1).unwrap_or(0);
            std::str::from_utf8(&bytes[..end]).unwrap().to_string()
        };
        assert_eq!(field(0), "4");
        assert_eq!(field(1), "linux_amd64");
        assert_eq!(field(2), "v2.0.0");
        assert_eq!(field(3), "v0.0.1");
        assert_eq!(field(4), "C_STRUCT");
        assert!(payload[ExtensionMetadata::FOOTER_SIZE - ExtensionMetadata::SIGNATURE_SIZE..].iter().all(|&b| b == 0));
        assert!(ExtensionMetadata::has_footer(&footer));
        assert!(!ExtensionMetadata::has_footer(b"not an extension"));
        assert!(ExtensionMetadata { platform: "x".repeat(33), ..metadata }.footer().is_err());
    }

    #[test]
    fn pinned_footer_and_version_tag() {
        // a development version (upstream's shape, or with a build suffix after the number) pins to the source id
        assert_eq!(ExtensionMetadata::version_directory_name("v3.0.0-dev89829", "b5342cff61"), "b5342cff61");
        assert_eq!(ExtensionMetadata::version_directory_name("v3.0.0-dev89829-rs", "b5342cff61"), "b5342cff61");
        assert_eq!(ExtensionMetadata::version_directory_name("v2.0.0", "b5342cff61"), "v2.0.0");
        assert_eq!(ExtensionMetadata::version_directory_name("2.0.0", "b5342cff61"), "v2.0.0");
        let pinned = ExtensionMetadata::pinned("b5342cff61", "v0.0.1");
        assert_eq!(pinned.abi_type, AbiType::CStructUnstable);
        assert_eq!(pinned.version, "b5342cff61");
        assert_eq!(pinned.platform, current_platform());
        assert!(ExtensionMetadata::has_footer(&pinned.footer().unwrap()));
    }
}
