//! End-to-end: write a container file with every codec, read it back block by block.

use avro_core::{Codec, Decoder, Encoder, Field, FileReader, FileWriter, Name, SchemaBuilder, SchemaKind, Value};

#[test]
fn write_then_read_every_codec() {
    let mut b = SchemaBuilder::new();
    let long = b.add_kind(SchemaKind::Long);
    let string = b.add_kind(SchemaKind::String);
    let null = b.add_kind(SchemaKind::Null);
    let opt = b.add_kind(SchemaKind::Union { branches: vec![null, string] });
    let root = b.add_kind(SchemaKind::Record {
        name: Name::simple("root"),
        fields: vec![Field::new("a", long), Field::new("b", opt)],
        is_error: false,
    });
    let schema = b.finish(root).unwrap();
    let rows = 3000;
    let row = |i: i64| {
        Value::Record(vec![
            Value::Long(i),
            if i % 3 == 0 {
                Value::Union(0, Box::new(Value::Null))
            } else {
                Value::Union(1, Box::new(Value::String(format!("row_{i}"))))
            },
        ])
    };
    for codec in Codec::ALL {
        let mut writer = FileWriter::new(Vec::new(), &schema, codec, &[("k".to_owned(), b"v".to_vec())]).unwrap();
        let mut enc = Encoder::new();
        for i in 0..rows {
            row(i).write(&mut enc, &schema, schema.root()).unwrap();
            if i % 1000 == 999 {
                writer.write_block(1000, &enc.take()).unwrap();
            }
        }
        let bytes = writer.finish().unwrap();
        let reader = FileReader::open(&bytes).unwrap();
        assert_eq!(reader.codec(), codec);
        assert_eq!(reader.blocks().len(), 3);
        assert_eq!(reader.header().metadata_value("k"), Some(&b"v"[..]));
        let mut i = 0;
        for block in reader.blocks() {
            let data = reader.decompress(block).unwrap();
            let mut dec = Decoder::new(&data);
            for _ in 0..block.row_count {
                assert_eq!(Value::read(&mut dec, &schema, schema.root()).unwrap(), row(i), "{}", codec.name());
                i += 1;
            }
            assert!(dec.is_empty());
        }
        assert_eq!(i, rows);
    }
}
