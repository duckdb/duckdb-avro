use std::path::{Path, PathBuf};

use avro_core::avro::codec::Codec;
use avro_core::avro::container::FileInfo;
use avro_core::avro::schema::Schema;

fn test_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test")
}

fn avro_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path == test_dir().join("corpus") {
            continue;
        }
        if path.is_dir() {
            avro_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "avro") {
            files.push(path);
        }
    }
}

fn all_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    avro_files(&test_dir(), &mut files);
    files.sort();
    files
}

#[test]
fn every_test_file_parses_and_decodes() {
    let files = all_files();
    assert!(files.len() >= 50, "found only {} files", files.len());
    let dir = test_dir();
    for required in [
        "iceberg",
        "sql/timestamp_millis.avro",
        "broken_record.avro",
        "recursive.avro",
    ] {
        assert!(
            files.iter().any(|f| f.starts_with(dir.join(required))),
            "{required} not found"
        );
    }

    let mut scratch = Vec::new();
    for path in &files {
        let data = std::fs::read(path).unwrap();
        let info = FileInfo::parse(&data).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let rendered = info.schema.to_json();
        let reparsed = Schema::parse_lenient(rendered.as_bytes()).unwrap();
        assert_eq!(reparsed.to_json(), rendered, "{}", path.display());

        let mut objects = 0;
        let mut decoded_bytes = 0;
        for block in &info.blocks {
            let decoded = info
                .decode_block(&data, block, &mut scratch)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            decoded_bytes += decoded.len();
            objects += block.object_count;
        }
        let codec = info.codec.name();
        println!(
            "{}: codec {codec}, {} blocks, {objects} objects, {decoded_bytes} bytes",
            path.strip_prefix(&dir).unwrap().display(),
            info.blocks.len()
        );
    }
}

#[test]
fn corpus_schemas_render_as_json_that_parses_back() {
    let mut files = Vec::new();
    let mut sources = vec![test_dir().join("corpus")];
    while let Some(dir) = sources.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                sources.push(path);
            } else if path.extension().is_some_and(|ext| ext == "avro") {
                files.push(path);
            }
        }
    }
    assert!(files.len() > 1300, "found only {} files", files.len());
    let mut parsed = 0;
    for path in &files {
        let data = std::fs::read(path).unwrap();
        let Ok(info) = FileInfo::parse(&data) else {
            continue;
        };
        let rendered = info.schema.to_json();
        serde_json::from_str::<serde_json::Value>(&rendered)
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let reparsed = Schema::parse_lenient(rendered.as_bytes())
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        assert_eq!(reparsed, info.schema, "{}", path.display());
        parsed += 1;
    }
    assert!(parsed > 1250, "parsed only {parsed} files");
}

#[test]
fn iceberg_manifests_keep_metadata_and_field_ids() {
    let mut iceberg = Vec::new();
    avro_files(&test_dir().join("iceberg"), &mut iceberg);
    assert!(!iceberg.is_empty());
    for path in iceberg {
        let data = std::fs::read(&path).unwrap();
        let info = FileInfo::parse(&data).unwrap();
        let keys: Vec<&str> = info.metadata.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"avro.schema"), "{}", path.display());
        assert!(
            info.schema.to_json().contains("\"field-id\":"),
            "{}",
            path.display()
        );
    }
}

#[test]
fn deflate_files_are_found() {
    let found = all_files().iter().any(|path| {
        let data = std::fs::read(path).unwrap();
        FileInfo::parse(&data).unwrap().codec == Codec::Deflate
    });
    assert!(found);
}
