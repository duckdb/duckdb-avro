use avro_core::avro::binary::{write_bytes, write_long};
use avro_core::avro::codec::Codec;
use avro_core::avro::container::{FileInfo, generate_sync, write_block, write_header};

const CODECS: [Codec; 6] = [
    Codec::Null,
    Codec::Deflate,
    Codec::Snappy,
    Codec::Lzma,
    Codec::Zstandard,
    Codec::Bzip2,
];

const SCHEMA: &str = r#"{"type":"record","name":"row","namespace":"test","fields":[{"name":"id","type":{"type":"long"},"field-id":1},{"name":"name","type":["null",{"type":"string"}]}]}"#;

fn encoded_rows(start: i64, count: i64) -> Vec<u8> {
    let mut out = Vec::new();
    for id in start..start + count {
        write_long(&mut out, id);
        write_long(&mut out, 1);
        write_bytes(&mut out, format!("row number {id}").as_bytes());
    }
    out
}

#[test]
fn written_files_parse_back_for_every_codec() {
    let metadata = vec![
        ("iceberg.schema".to_owned(), "{}".to_owned()),
        ("format-version".to_owned(), "2".to_owned()),
    ];
    let blocks: Vec<(u64, Vec<u8>)> = vec![
        (1, encoded_rows(0, 1)),
        (1000, encoded_rows(1, 1000)),
        (50_000, encoded_rows(1001, 50_000)),
    ];
    for codec in CODECS {
        let sync = generate_sync();
        let mut file = Vec::new();
        let mut scratch = Vec::new();
        write_header(&mut file, SCHEMA, codec, &metadata, &sync);
        let header_len = file.len();
        for (count, data) in &blocks {
            write_block(&mut file, *count, data, codec, &sync, &mut scratch).unwrap();
        }

        let info = FileInfo::parse(&file).unwrap();
        assert_eq!(info.codec, codec);
        assert_eq!(info.sync, sync);
        assert_eq!(info.schema.to_json(), SCHEMA);
        let mut expected_metadata = vec![
            ("avro.codec".to_owned(), codec.name().as_bytes().to_vec()),
            ("avro.schema".to_owned(), SCHEMA.as_bytes().to_vec()),
        ];
        expected_metadata.extend(
            metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.as_bytes().to_vec())),
        );
        assert_eq!(info.metadata, expected_metadata);
        assert_eq!(info.blocks.len(), blocks.len());
        assert!(info.blocks[0].offset > header_len);
        for (block, (count, data)) in info.blocks.iter().zip(&blocks) {
            assert_eq!(block.object_count, *count);
            let decoded = info.decode_block(&file, block, &mut scratch).unwrap();
            assert!(decoded == data.as_slice(), "{codec:?}");
        }
    }
}

#[test]
fn header_only_file_has_no_blocks() {
    let mut file = Vec::new();
    write_header(&mut file, "\"int\"", Codec::Null, &[], &generate_sync());
    let info = FileInfo::parse(&file).unwrap();
    assert!(info.blocks.is_empty());
    assert_eq!(info.schema.to_json(), r#"{"type":"int"}"#);
}
