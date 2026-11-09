//! Writes a mixed-type file with every codec (plus a header-only file) into OUTDIR, reads each back with
//! `FileReader` and checks every row against what was written.
//!
//! `roundtrip OUTDIR [ROWS]`

use avro_core::{
    Codec, Decoder, Encoder, Field, FileReader, FileWriter, Name, Schema, SchemaBuilder, SchemaKind, SchemaNode, Value,
};

/// The schema the rows are generated for: nested record, array, map, union, fixed, enum and decimal bytes.
fn schema() -> Schema {
    let mut b = SchemaBuilder::new();
    let null = b.add_kind(SchemaKind::Null);
    let boolean = b.add_kind(SchemaKind::Boolean);
    let int = b.add_kind(SchemaKind::Int);
    let long = b.add_kind(SchemaKind::Long);
    let float = b.add_kind(SchemaKind::Float);
    let double = b.add_kind(SchemaKind::Double);
    let string = b.add_kind(SchemaKind::String);
    let bytes = b.add_kind(SchemaKind::Bytes);
    let date = b.add(SchemaNode::new(SchemaKind::Int).with_logical_type("date"));
    let ts = b.add(SchemaNode::new(SchemaKind::Long).with_logical_type("timestamp-micros").with_adjust_to_utc(true));
    let decimal = b.add(SchemaNode::new(SchemaKind::Bytes).with_decimal(18, 4));
    let fixed_decimal =
        b.add(SchemaNode::new(SchemaKind::Fixed { name: Name::simple("dec9"), size: 9 }).with_decimal(19, 4));
    let uuid =
        b.add(SchemaNode::new(SchemaKind::Fixed { name: Name::simple("uuid"), size: 16 }).with_logical_type("uuid"));
    let color = b.add_kind(SchemaKind::Enum {
        name: Name::new("Color", Some("example")),
        symbols: vec!["RED".into(), "GREEN".into(), "BLUE".into()],
        default: Some("RED".into()),
    });
    let opt_string = b.add_kind(SchemaKind::Union { branches: vec![null, string] });
    let opt_long_array = {
        let arr = b.add_kind(SchemaKind::Array { items: long, element_id: Some(21) });
        b.add_kind(SchemaKind::Union { branches: vec![null, arr] })
    };
    let inner = b.add_kind(SchemaKind::Record {
        name: Name::new("inner", Some("example")),
        fields: vec![Field::new("a", int).with_field_id(100), Field::new("b", opt_string).with_field_id(101)],
    });
    let inner_again = b.add_ref(Name::new("inner", Some("example")));
    let map = b.add_kind(SchemaKind::Map { values: inner_again, key_id: Some(31), value_id: Some(32) });
    let many = b.add_kind(SchemaKind::Union { branches: vec![int, string, inner, null, double] });
    let root = b.add_kind(SchemaKind::Record {
        name: Name::simple("root"),
        fields: vec![
            Field::new("id", long).with_field_id(1),
            Field::new("flag", boolean),
            Field::new("f", float),
            Field::new("d", double),
            Field::new("s", string),
            Field::new("blob", bytes),
            Field::new("day", date),
            Field::new("ts", ts),
            Field::new("dec", decimal),
            Field::new("fdec", fixed_decimal),
            Field::new("uuid", uuid),
            Field::new("color", color),
            Field::new("maybe", opt_string),
            Field::new("nums", opt_long_array),
            Field::new("by_name", map),
            Field::new("many", many),
        ],
    });
    b.finish(root).expect("valid schema")
}

/// Deterministic pseudo-random row `i`.
fn row(i: i64) -> Value {
    let x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 11;
    let text = |n: u64| {
        format!("row_{i}_{}", "αβγ🦆".chars().cycle().skip((n % 4) as usize).take((n % 6) as usize).collect::<String>())
    };
    let inner = |k: i64| {
        Value::Record(vec![
            Value::Int((k % 1000) as i32),
            if k % 3 == 0 {
                Value::Union(0, Box::new(Value::Null))
            } else {
                Value::Union(1, Box::new(Value::String(text(k as u64))))
            },
        ])
    };
    let mut dec = (i * 12345 - 50_000).to_be_bytes().to_vec();
    while dec.len() > 1 && ((dec[0] == 0 && dec[1] < 0x80) || (dec[0] == 0xff && dec[1] >= 0x80)) {
        dec.remove(0);
    }
    let mut fdec = vec![if i < 0 { 0xff } else { 0 }; 9];
    fdec[1..].copy_from_slice(&(i * 7).to_be_bytes());
    Value::Record(vec![
        Value::Long(i),
        Value::Boolean(i % 2 == 0),
        Value::Float(i as f32 / 7.0),
        Value::Double(f64::from_bits(x | 0x3ff0_0000_0000_0000) * if i % 5 == 0 { -1.0 } else { 1.0 }),
        Value::String(text(x)),
        Value::Bytes((0..(x % 20) as u8).collect()),
        Value::Int((i % 20_000) as i32),
        Value::Long(i * 1_000_003),
        Value::Bytes(dec),
        Value::Fixed(fdec),
        Value::Fixed(x.to_le_bytes().iter().chain(x.to_be_bytes().iter()).copied().collect()),
        Value::Enum((x % 3) as usize),
        if i % 4 == 0 {
            Value::Union(0, Box::new(Value::Null))
        } else {
            Value::Union(1, Box::new(Value::String(format!("m{i}"))))
        },
        if i % 7 == 0 {
            Value::Union(0, Box::new(Value::Null))
        } else {
            Value::Union(1, Box::new(Value::Array((0..(x % 5) as i64).map(|k| Value::Long(k * i)).collect())))
        },
        Value::Map((0..(x % 4) as i64).map(|k| (format!("k{k}"), inner(i + k))).collect()),
        match x % 5 {
            0 => Value::Union(0, Box::new(Value::Int(i as i32))),
            1 => Value::Union(1, Box::new(Value::String(text(x >> 3)))),
            2 => Value::Union(2, Box::new(inner(i))),
            3 => Value::Union(3, Box::new(Value::Null)),
            _ => Value::Union(4, Box::new(Value::Double(i as f64 * 0.25))),
        },
    ])
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(outdir) = args.first() else {
        eprintln!("usage: roundtrip OUTDIR [ROWS]");
        std::process::exit(2);
    };
    let rows: i64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(5000);
    std::fs::create_dir_all(outdir).expect("create OUTDIR");
    let schema = schema();
    let metadata = vec![
        ("writer".to_owned(), b"avro-core roundtrip".to_vec()),
        ("unicode".to_owned(), "ünïcödé 🦆".as_bytes().to_vec()),
    ];
    let mut failures = 0;
    for codec in Codec::ALL {
        let path = format!("{outdir}/roundtrip_{}.avro", codec.name());
        let mut writer = FileWriter::new(Vec::new(), &schema, codec, &metadata).expect("header");
        let mut enc = Encoder::new();
        let mut in_block = 0u64;
        for i in 0..rows {
            row(i).write(&mut enc, &schema, schema.root()).expect("encode");
            in_block += 1;
            // blocks of varying size, so the reader sees several
            if in_block >= 700 + (i as u64 % 300) {
                writer.write_block(in_block, enc.as_bytes()).expect("block");
                enc.clear();
                in_block = 0;
            }
        }
        writer.write_block(in_block, enc.as_bytes()).expect("block");
        let bytes = writer.finish().expect("finish");
        std::fs::write(&path, &bytes).expect("write file");
        // read back
        let reader = FileReader::open(&bytes).expect("open");
        assert_eq!(reader.codec(), codec);
        assert_eq!(reader.header().metadata_value("unicode"), Some("ünïcödé 🦆".as_bytes()));
        assert_eq!(reader.schema().to_json(), schema.to_json());
        let mut i = 0i64;
        let mut ok = true;
        for block in reader.blocks() {
            let data = reader.decompress(block).expect("decompress");
            let mut dec = Decoder::new(&data);
            for _ in 0..block.row_count {
                let got = Value::read(&mut dec, reader.schema(), reader.schema().root()).expect("decode");
                if got != row(i) {
                    eprintln!("{}: row {i} differs: {got:?}", codec.name());
                    ok = false;
                }
                i += 1;
            }
            ok &= dec.is_empty();
        }
        ok &= i == rows;
        // skipping every row lands exactly at the end of each block
        for block in reader.blocks() {
            let data = reader.decompress(block).expect("decompress");
            let mut dec = Decoder::new(&data);
            for _ in 0..block.row_count {
                dec.skip(reader.schema(), reader.schema().root()).expect("skip");
            }
            ok &= dec.is_empty();
        }
        println!(
            "{:<10} {:>8} bytes {:>3} blocks {:>6} rows read back: {}",
            codec.name(),
            bytes.len(),
            reader.blocks().len(),
            i,
            if ok { "OK" } else { "MISMATCH" }
        );
        failures += u32::from(!ok);
    }
    // a header-only file
    let path = format!("{outdir}/roundtrip_empty.avro");
    let bytes =
        FileWriter::new(Vec::new(), &schema, Codec::Deflate, &metadata).expect("header").finish().expect("finish");
    std::fs::write(&path, &bytes).expect("write file");
    let reader = FileReader::open(&bytes).expect("open");
    let ok = reader.blocks().is_empty() && reader.total_rows() == 0 && reader.codec() == Codec::Deflate;
    println!(
        "{:<10} {:>8} bytes {:>3} blocks {:>6} rows read back: {}",
        "empty",
        bytes.len(),
        0,
        0,
        if ok { "OK" } else { "MISMATCH" }
    );
    failures += u32::from(!ok);
    if failures > 0 {
        std::process::exit(1);
    }
}
