use std::path::PathBuf;

use avro_core::avro::{
    binary::{write_bytes, write_long},
    codec::Codec,
    container::{generate_sync, write_block, write_header},
};
use duckdb_neo::{
    Parameters, Result,
    connection::Connection,
    environment::{Environment, StorageLocation},
};

fn connect() -> Result<Connection> {
    let conn = Environment::new()?
        .open(StorageLocation::InMemory)?
        .connect()?;
    avro_core::register_functions!(&conn)?;
    Ok(conn)
}

struct TempFile(PathBuf);

impl TempFile {
    fn new(name: &str, schema: &str, blocks: &[(u64, Vec<u8>)]) -> TempFile {
        let sync = generate_sync();
        let mut out = Vec::new();
        write_header(&mut out, schema, Codec::Null, &[], &sync);
        let mut scratch = Vec::new();
        for (count, data) in blocks {
            write_block(&mut out, *count, data, Codec::Null, &sync, &mut scratch).unwrap();
        }
        let path =
            std::env::temp_dir().join(format!("avro_core_{name}_{}.avro", std::process::id()));
        std::fs::write(&path, out).unwrap();
        TempFile(path)
    }

    fn path(&self) -> &str {
        self.0.to_str().unwrap()
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn query_text(conn: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut result = conn.query(
        format!("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ({sql})"),
        Parameters::None,
    )?;
    let mut rows = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        for row in 0..chunk.row_count()? {
            let cells = (0..chunk.col_count())
                .map(|column| {
                    let vector = chunk.get_vector_at::<String>(column)?;
                    Ok(vector.reader()?.get(row)?.unwrap_or("NULL").to_owned())
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(cells.join("|"));
        }
    }
    Ok(rows)
}

fn record_schema(fields: &str) -> String {
    format!(r#"{{"type":"record","name":"root","fields":[{fields}]}}"#)
}

#[test]
fn duplicate_map_keys_keep_first_position_and_last_value() -> Result<()> {
    let schema = record_schema(r#"{"name":"m","type":{"type":"map","values":"long"}}"#);
    let mut data = Vec::new();
    write_long(&mut data, 3);
    for (key, value) in [("a", 1), ("b", 2), ("a", 3)] {
        write_bytes(&mut data, key.as_bytes());
        write_long(&mut data, value);
    }
    write_long(&mut data, 0);
    let file = TempFile::new("duplicate_keys", &schema, &[(1, data)]);
    let conn = connect()?;
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["{a=3, b=2}"]
    );
    Ok(())
}

#[test]
fn duplicate_map_keys_in_large_maps() -> Result<()> {
    let schema = record_schema(r#"{"name":"m","type":{"type":"map","values":"long"}}"#);
    let mut data = Vec::new();
    write_long(&mut data, 41);
    for index in 0..40 {
        write_bytes(&mut data, format!("k{index}").as_bytes());
        write_long(&mut data, index);
    }
    write_bytes(&mut data, b"k20");
    write_long(&mut data, 99);
    write_long(&mut data, 0);
    let file = TempFile::new("duplicate_keys_large", &schema, &[(1, data)]);
    let conn = connect()?;
    let sql = format!(
        "SELECT cardinality(m), m['k20'], m['k39'] FROM read_avro('{}')",
        file.path()
    );
    assert_eq!(query_text(&conn, &sql)?, ["40|99|39"]);
    Ok(())
}

#[test]
fn projection_skips_nested_fields() -> Result<()> {
    let schema = record_schema(concat!(
        r#"{"name":"skipped","type":{"type":"array","items":{"type":"map","values":["null","string"]}}},"#,
        r#"{"name":"choice","type":["null","int","string"]},"#,
        r#"{"name":"kept","type":"long"}"#
    ));
    let mut data = Vec::new();
    for row in 0..3i64 {
        // an array block with a byte size, which a skip can jump over
        let mut items = Vec::new();
        write_long(&mut items, 1);
        write_bytes(&mut items, b"key");
        write_long(&mut items, 1);
        write_bytes(&mut items, b"value");
        write_long(&mut items, 0);
        write_long(&mut data, -1);
        write_long(&mut data, items.len() as i64);
        data.extend_from_slice(&items);
        write_long(&mut data, 0);
        write_long(&mut data, 2);
        write_bytes(&mut data, b"text");
        write_long(&mut data, row * 10);
    }
    let file = TempFile::new("projection", &schema, &[(3, data)]);
    let conn = connect()?;
    let sql = format!("SELECT kept, choice FROM read_avro('{}')", file.path());
    assert_eq!(query_text(&conn, &sql)?, ["0|text", "10|text", "20|text"]);
    let sql = format!("SELECT kept FROM read_avro('{}')", file.path());
    assert_eq!(query_text(&conn, &sql)?, ["0", "10", "20"]);
    Ok(())
}

#[test]
fn root_record_in_nullable_union() -> Result<()> {
    let schema = format!(
        r#"["null",{}]"#,
        record_schema(r#"{"name":"a","type":"int"}"#)
    );
    let mut data = Vec::new();
    write_long(&mut data, 1);
    write_long(&mut data, 7);
    write_long(&mut data, 0);
    let file = TempFile::new("root_union", &schema, &[(2, data)]);
    let conn = connect()?;
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["7", "NULL"]
    );
    Ok(())
}

#[test]
fn malformed_blocks_are_errors() -> Result<()> {
    let schema = record_schema(
        r#"{"name":"a","type":"long"},{"name":"e","type":{"type":"enum","name":"E","symbols":["x"]}}"#,
    );
    let conn = connect()?;
    let cases: [(&str, u64, &[i64], &str); 4] = [
        (
            "trailing",
            1,
            &[1, 0, 5],
            "Avro file block contains trailing data",
        ),
        (
            "short",
            2,
            &[1, 0],
            "Avro file block ended before its declared object count",
        ),
        ("enum", 1, &[1, 3], "Enum value out of range"),
        (
            "truncated",
            1,
            &[1],
            "Avro file block ended before its declared object count",
        ),
    ];
    for (name, count, values, message) in cases {
        let mut data = Vec::new();
        values
            .iter()
            .for_each(|value| write_long(&mut data, *value));
        let file = TempFile::new(name, &schema, &[(count, data)]);
        let error = query_text(&conn, &format!("FROM read_avro('{}')", file.path())).unwrap_err();
        assert!(error.message.contains(message), "{name}: {}", error.message);
    }
    Ok(())
}

#[test]
fn union_of_only_nulls_reads_its_branch_index() -> Result<()> {
    let schema = record_schema(r#"{"name":"n","type":["null","null"]},{"name":"a","type":"long"}"#);
    let mut data = Vec::new();
    write_long(&mut data, 1);
    write_long(&mut data, 42);
    let file = TempFile::new("null_union", &schema, &[(1, data)]);
    let conn = connect()?;
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["NULL|42"]
    );
    Ok(())
}

fn read_error(conn: &Connection, sql: &str) -> String {
    query_text(conn, sql).unwrap_err().message
}

#[test]
fn block_counts_are_bounded_by_the_data() -> Result<()> {
    let schema = record_schema(concat!(
        r#"{"name":"nulls","type":{"type":"array","items":"null"}},"#,
        r#"{"name":"ints","type":{"type":"array","items":"int"}},"#,
        r#"{"name":"a","type":"long"}"#
    ));
    let conn = connect()?;
    let cases: [(&str, &[i64], &[i64], &str); 6] = [
        (
            "zero_width",
            &[1 << 40],
            &[],
            "Avro block count 1099511627776 exceeds the limit of 16777216 zero-size items per block",
        ),
        (
            "zero_width_blocks",
            &[1 << 23, 1 << 23, 1],
            &[],
            "Avro array or map holds more than 16777216 zero-size items",
        ),
        (
            "ints",
            &[],
            &[1 << 30],
            "Avro block count 1073741824 exceeds the remaining 2 bytes",
        ),
        ("overflow", &[], &[i64::MIN], "Avro block count overflow"),
        (
            "ints_max",
            &[],
            &[i64::MAX],
            "Avro block count 9223372036854775807 exceeds the remaining 2 bytes",
        ),
        (
            "ints_sized_max",
            &[],
            &[-i64::MAX],
            "Avro block count 9223372036854775807 exceeds the remaining 1 bytes",
        ),
    ];
    for (name, null_blocks, int_blocks, expected) in cases {
        let mut data = Vec::new();
        for blocks in [null_blocks, int_blocks] {
            for count in blocks {
                write_long(&mut data, *count);
            }
            write_long(&mut data, 0);
        }
        write_long(&mut data, 1);
        let file = TempFile::new(name, &schema, &[(1, data)]);
        for columns in ["*", "a"] {
            let message = read_error(
                &conn,
                &format!("SELECT {columns} FROM read_avro('{}')", file.path()),
            );
            assert!(message.contains(expected), "{name} {columns}: {message}");
        }
    }
    Ok(())
}

#[test]
fn schemas_that_expand_too_far_are_errors() -> Result<()> {
    let conn = connect()?;
    // Each record references the one before twice, doubling the size of the expanded type
    let mut branches =
        vec![r#"{"type":"record","name":"R0","fields":[{"name":"x","type":"int"}]}"#.to_owned()];
    for index in 1..40 {
        let previous = index - 1;
        branches.push(format!(
            r#"{{"type":"record","name":"R{index}","fields":[{{"name":"a","type":"R{previous}"}},{{"name":"b","type":"R{previous}"}}]}}"#
        ));
    }
    let file = TempFile::new("wide_schema", &format!("[{}]", branches.join(",")), &[]);
    let message = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
    assert!(
        message.contains("Avro schema expands into too many types"),
        "{message}"
    );

    let mut branches =
        vec![r#"{"type":"record","name":"R0","fields":[{"name":"x","type":"int"}]}"#.to_owned()];
    for index in 1..300 {
        let previous = index - 1;
        branches.push(format!(
            r#"{{"type":"record","name":"R{index}","fields":[{{"name":"a","type":"R{previous}"}}]}}"#
        ));
    }
    let file = TempFile::new("deep_schema", &format!("[{}]", branches.join(",")), &[]);
    let message = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
    assert!(
        message.contains("Avro schema is nested too deeply"),
        "{message}"
    );
    Ok(())
}

fn column_types(conn: &Connection, file: &TempFile) -> Result<Vec<String>> {
    query_text(
        conn,
        &format!(
            "SELECT column_type FROM (DESCRIBE FROM read_avro('{}'))",
            file.path()
        ),
    )
}

#[test]
fn unknown_logical_types_are_ignored() -> Result<()> {
    let schema = record_schema(concat!(
        r#"{"name":"v","type":{"type":"long","logicalType":"time-interval-micros"}},"#,
        r#"{"name":"s","type":{"type":"string","logicalType":"char"}},"#,
        r#"{"name":"b","type":{"type":"boolean","logicalType":"flag"}},"#,
        r#"{"name":"n","type":{"type":"null","logicalType":"date"}},"#,
        r#"{"name":"i","type":{"type":"int","logicalType":"decimal"}},"#,
        r#"{"name":"d","type":{"type":"double","logicalType":"decimal","precision":39}}"#
    ));
    let mut data = Vec::new();
    write_long(&mut data, 42);
    write_bytes(&mut data, b"abc");
    data.push(1);
    write_long(&mut data, 7);
    data.extend_from_slice(&0.5f64.to_le_bytes());
    let file = TempFile::new("unknown_logical", &schema, &[(1, data)]);
    let conn = connect()?;
    assert_eq!(
        column_types(&conn, &file)?,
        [
            "BIGINT", "VARCHAR", "BOOLEAN", "\"NULL\"", "INTEGER", "DOUBLE"
        ]
    );
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["42|abc|true|NULL|7|0.5"]
    );
    Ok(())
}

#[test]
fn known_logical_types_on_other_types_are_errors() -> Result<()> {
    let conn = connect()?;
    for (name, field_type, message) in [
        (
            "bytes_date",
            r#"{"type":"bytes","logicalType":"date"}"#,
            "Avro logical type 'date' is not supported on Avro type 'bytes'",
        ),
        (
            "string_date",
            r#"{"type":"string","logicalType":"date"}"#,
            "Avro logical type 'date' is not supported on Avro type 'string'",
        ),
        (
            "int_micros",
            r#"{"type":"int","logicalType":"timestamp-micros"}"#,
            "Avro logical type 'timestamp-micros' is not supported on Avro type 'int'",
        ),
        (
            "long_date",
            r#"{"type":"long","logicalType":"date"}"#,
            "Avro logical type 'date' is not supported on Avro type 'long'",
        ),
        (
            "double_decimal",
            r#"{"type":"double","logicalType":"decimal","precision":4}"#,
            "Avro logical type 'decimal' is not supported on Avro type 'double'",
        ),
        (
            "float_uuid",
            r#"{"type":"float","logicalType":"uuid"}"#,
            "Avro logical type 'uuid' is not supported on Avro type 'float'",
        ),
        (
            "bytes_uuid",
            r#"{"type":"bytes","logicalType":"uuid"}"#,
            "Avro logical type 'uuid' is not supported on Avro type 'bytes'",
        ),
        (
            "fixed_date",
            r#"{"type":"fixed","name":"f","size":4,"logicalType":"date"}"#,
            "Avro logical type 'date' is not supported on Avro type 'fixed'",
        ),
        (
            "fixed_uuid",
            r#"{"type":"fixed","name":"f","size":4,"logicalType":"uuid"}"#,
            "logical type is uuid, but size != 16",
        ),
        (
            "fixed_duration",
            r#"{"type":"fixed","name":"f","size":4,"logicalType":"duration"}"#,
            "Avro logical type 'duration' requires a fixed of size 12",
        ),
    ] {
        let schema = record_schema(&format!(r#"{{"name":"x","type":{field_type}}}"#));
        let file = TempFile::new(name, &schema, &[]);
        let error = query_text(&conn, &format!("FROM read_avro('{}')", file.path()))
            .map_or_else(|error| error.message, |rows| format!("{rows:?}"));
        assert!(error.contains(message), "{name}: {error}");
    }
    Ok(())
}

#[test]
fn invalid_decimals_are_read_as_bytes() -> Result<()> {
    let conn = connect()?;
    for (name, attributes, expected) in [
        ("string_precision", r#""precision":"5","scale":2"#, "BLOB"),
        ("no_precision", r#""scale":2"#, "BLOB"),
        ("zero_precision", r#""precision":0"#, "BLOB"),
        ("too_many_digits", r#""precision":39"#, "BLOB"),
        (
            "huge_precision",
            r#""precision":4294967306,"scale":2"#,
            "BLOB",
        ),
        ("scale_too_large", r#""precision":4,"scale":5"#, "BLOB"),
        ("negative_scale", r#""precision":4,"scale":-1"#, "BLOB"),
        (
            "string_scale",
            r#""precision":4,"scale":"2""#,
            "DECIMAL(4,0)",
        ),
        ("no_scale", r#""precision":38"#, "DECIMAL(38,0)"),
    ] {
        let schema = record_schema(&format!(
            r#"{{"name":"b","type":{{"type":"bytes","logicalType":"decimal",{attributes}}}}},{{"name":"f","type":{{"type":"fixed","name":"f","size":2,"logicalType":"decimal",{attributes}}}}}"#
        ));
        let mut data = Vec::new();
        write_bytes(&mut data, b"\x01\x2c");
        data.extend_from_slice(b"\x01\x2c");
        let file = TempFile::new(name, &schema, &[(1, data)]);
        assert_eq!(column_types(&conn, &file)?, [expected, expected], "{name}");
        let values = query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?;
        let value = if expected == "BLOB" { "\\x01," } else { "300" };
        assert_eq!(values, [format!("{value}|{value}")], "{name}");
    }
    Ok(())
}

fn duration(months: u32, days: u32, millis: u32) -> Vec<u8> {
    [months, days, millis]
        .iter()
        .flat_map(|part| part.to_le_bytes())
        .collect()
}

#[test]
fn durations_are_intervals() -> Result<()> {
    let schema = record_schema(
        r#"{"name":"d","type":["null",{"type":"fixed","name":"duration","size":12,"logicalType":"duration"}]}"#,
    );
    let mut data = Vec::new();
    write_long(&mut data, 1);
    data.extend(duration(14, 2, 3_723_004));
    write_long(&mut data, 0);
    write_long(&mut data, 1);
    data.extend(duration(0, 0, u32::MAX));
    let file = TempFile::new("duration", &schema, &[(3, data)]);
    let conn = connect()?;
    assert_eq!(column_types(&conn, &file)?, ["INTERVAL"]);
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        [
            "1 year 2 months 2 days 01:02:03.004",
            "NULL",
            "1193:02:47.295"
        ]
    );
    for (name, value, message) in [
        (
            "duration_months",
            duration(1 << 31, 0, 0),
            "duration months value 2147483648 is out of range for an INTERVAL",
        ),
        (
            "duration_days",
            duration(0, u32::MAX, 0),
            "duration days value 4294967295 is out of range for an INTERVAL",
        ),
    ] {
        let mut data = Vec::new();
        write_long(&mut data, 1);
        data.extend(value);
        let file = TempFile::new(name, &schema, &[(1, data)]);
        let error = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
        assert!(error.contains(message), "{name}: {error}");
    }
    Ok(())
}

#[test]
fn uuid_strings_are_parsed() -> Result<()> {
    let schema = record_schema(r#"{"name":"id","type":{"type":"string","logicalType":"uuid"}}"#);
    let mut data = Vec::new();
    write_bytes(&mut data, b"123e4567-e89b-12d3-a456-426614174000");
    write_bytes(&mut data, b"FFFFFFFF-0000-0000-0000-00000000000A");
    let file = TempFile::new("uuid_string", &schema, &[(2, data)]);
    let conn = connect()?;
    assert_eq!(column_types(&conn, &file)?, ["UUID"]);
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        [
            "123e4567-e89b-12d3-a456-426614174000",
            "ffffffff-0000-0000-0000-00000000000a"
        ]
    );
    for (name, text) in [
        ("uuid_short", "123e4567-e89b-12d3-a456-42661417400"),
        ("uuid_braces", "{23e4567-e89b-12d3-a456-42661417400}"),
        ("uuid_no_hyphens", "123e4567e89b12d3a4564266141740000000"),
        ("uuid_sign", "+23e4567-e89b-12d3-a456-426614174000"),
        ("uuid_hex_letter", "123e4567-e89b-12d3-a456-42661417400g"),
    ] {
        let mut data = Vec::new();
        write_bytes(&mut data, text.as_bytes());
        let file = TempFile::new(name, &schema, &[(1, data)]);
        let error = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
        assert!(
            error.contains(&format!("Avro uuid string '{text}' is not a UUID")),
            "{name}: {error}"
        );
    }
    Ok(())
}

#[test]
fn times_range_up_to_midnight() -> Result<()> {
    let schema = record_schema(concat!(
        r#"{"name":"ms","type":{"type":"int","logicalType":"time-millis"}},"#,
        r#"{"name":"us","type":{"type":"long","logicalType":"time-micros"}}"#
    ));
    let mut data = Vec::new();
    for value in [0, 0, 86_400_000, 86_400_000_000] {
        write_long(&mut data, value);
    }
    let file = TempFile::new("time_bounds", &schema, &[(2, data)]);
    let conn = connect()?;
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["00:00:00|00:00:00", "24:00:00|24:00:00"]
    );
    for (name, values, message) in [
        (
            "time_millis_high",
            [86_400_001, 0],
            "time-millis value 86400001 is out of range",
        ),
        (
            "time_millis_low",
            [-1, 0],
            "time-millis value -1 is out of range",
        ),
        (
            "time_micros_high",
            [0, 86_400_000_001],
            "time-micros value 86400000001 is out of range",
        ),
        (
            "time_micros_low",
            [0, -1],
            "time-micros value -1 is out of range",
        ),
    ] {
        let mut data = Vec::new();
        values
            .iter()
            .for_each(|value| write_long(&mut data, *value));
        let file = TempFile::new(name, &schema, &[(1, data)]);
        let error = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
        assert!(error.contains(message), "{name}: {error}");
    }
    Ok(())
}

#[test]
fn local_timestamps_have_no_time_zone() -> Result<()> {
    let schema = record_schema(concat!(
        r#"{"name":"ms","type":{"type":"long","logicalType":"local-timestamp-millis"}},"#,
        r#"{"name":"us","type":{"type":"long","logicalType":"local-timestamp-micros","adjust-to-utc":true}},"#,
        r#"{"name":"ns","type":{"type":"long","logicalType":"local-timestamp-nanos"}}"#
    ));
    let mut data = Vec::new();
    for value in [
        1_700_000_000_123,
        1_700_000_000_123_456,
        1_700_000_000_123_456_789,
    ] {
        write_long(&mut data, value);
    }
    let file = TempFile::new("local_timestamps", &schema, &[(1, data)]);
    let conn = connect()?;
    assert_eq!(
        column_types(&conn, &file)?,
        ["TIMESTAMP", "TIMESTAMP", "TIMESTAMP_NS"]
    );
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["2023-11-14 22:13:20.123|2023-11-14 22:13:20.123456|2023-11-14 22:13:20.123456789"]
    );
    Ok(())
}

#[test]
fn zero_count_blocks_are_read_through() -> Result<()> {
    let schema = record_schema(r#"{"name":"a","type":"long"}"#);
    let rows = |values: &[i64]| {
        let mut data = Vec::new();
        values
            .iter()
            .for_each(|value| write_long(&mut data, *value));
        data
    };
    let many: Vec<i64> = (0..3000).collect();
    let file = TempFile::new(
        "zero_count_blocks",
        &schema,
        &[
            (0, Vec::new()),
            (1, rows(&[1])),
            (0, Vec::new()),
            (0, Vec::new()),
            (3000, rows(&many)),
            (0, Vec::new()),
            (2, rows(&[2, 3])),
            (0, Vec::new()),
        ],
    );
    let conn = connect()?;
    let sql = format!("SELECT count(*), sum(a) FROM read_avro('{}')", file.path());
    assert_eq!(query_text(&conn, &sql)?, ["3003|4498506"]);
    let file = TempFile::new(
        "zero_count_trailing",
        &schema,
        &[(1, rows(&[1])), (0, rows(&[2]))],
    );
    let error = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
    assert!(
        error.contains("Avro file block contains trailing data"),
        "{error}"
    );
    Ok(())
}

#[test]
fn null_items_of_map_arrays_are_dropped() -> Result<()> {
    let entry = r#"{"type":"record","name":"kv","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}"#;
    let schema = record_schema(&format!(
        r#"{{"name":"m","type":{{"type":"array","logicalType":"map","items":["null",{entry}]}}}},{{"name":"l","type":{{"type":"array","items":["null","int"]}}}}"#
    ));
    let mut data = Vec::new();
    for items in [
        [Some((1, 2)), None, Some((3, 4))],
        [None, None, Some((5, 6))],
    ] {
        write_long(&mut data, 3);
        for item in items {
            match item {
                Some((key, value)) => {
                    write_long(&mut data, 1);
                    write_long(&mut data, key);
                    write_long(&mut data, value);
                }
                None => write_long(&mut data, 0),
            }
        }
        write_long(&mut data, 0);
        write_long(&mut data, 2);
        write_long(&mut data, 0);
        write_long(&mut data, 1);
        write_long(&mut data, 9);
        write_long(&mut data, 0);
    }
    let file = TempFile::new("map_array_nulls", &schema, &[(2, data)]);
    let conn = connect()?;
    assert_eq!(
        column_types(&conn, &file)?,
        ["MAP(INTEGER, BIGINT)", "INTEGER[]"]
    );
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["{1=2, 3=4}|[NULL, 9]", "{5=6}|[NULL, 9]"]
    );
    Ok(())
}

#[test]
fn duplicate_map_keys_move_nulls() -> Result<()> {
    let schema = record_schema(r#"{"name":"m","type":{"type":"map","values":["null","long"]}}"#);
    let mut data = Vec::new();
    for entries in [
        [("a", None), ("b", Some(2)), ("a", Some(3))],
        [("a", Some(1)), ("b", None), ("a", None)],
    ] {
        write_long(&mut data, 3);
        for (key, value) in entries {
            write_bytes(&mut data, key.as_bytes());
            match value {
                Some(value) => {
                    write_long(&mut data, 1);
                    write_long(&mut data, value);
                }
                None => write_long(&mut data, 0),
            }
        }
        write_long(&mut data, 0);
    }
    let file = TempFile::new("duplicate_keys_nulls", &schema, &[(2, data)]);
    let conn = connect()?;
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["{a=3, b=2}", "{a=NULL, b=NULL}"]
    );
    Ok(())
}

#[test]
fn duplicate_map_keys_toggle_nulls_on_the_hash_path() -> Result<()> {
    let schema = record_schema(concat!(
        r#"{"name":"m","type":{"type":"map","values":["null",{"type":"record","name":"value","fields":["#,
        r#"{"name":"x","type":["null","long"]},{"name":"s","type":["null","string"]}]}]}}"#
    ));
    let mut data = Vec::new();
    let keys = 4096;
    let updates = 8192;
    write_long(&mut data, keys + updates + 1);
    for key in 0..keys {
        write_bytes(&mut data, format!("k{key}").as_bytes());
        write_long(&mut data, 0);
    }
    for update in 0..updates {
        write_bytes(&mut data, b"k0");
        write_long(&mut data, update % 2);
        if update % 2 == 1 {
            write_long(&mut data, 1);
            write_long(&mut data, update);
            write_long(&mut data, 1);
            write_bytes(&mut data, b"last");
        }
    }
    write_bytes(&mut data, b"end");
    write_long(&mut data, 1);
    write_long(&mut data, 1);
    write_long(&mut data, 7);
    write_long(&mut data, 1);
    write_bytes(&mut data, b"new");
    write_long(&mut data, 0);
    let file = TempFile::new("duplicate_keys_hash_nulls", &schema, &[(1, data)]);
    let conn = connect()?;
    assert_eq!(
        query_text(
            &conn,
            &format!(
                "SELECT cardinality(m), m['k0'].x, m['k0'].s, m['end'].x, m['end'].s, \
                 list_count(map_values(m)), map_keys(m)[1], map_keys(m)[4096] \
                 FROM read_avro('{}')",
                file.path()
            ),
        )?,
        ["4097|8191|last|7|new|2|k0|k4095"]
    );
    Ok(())
}

#[test]
fn recursion_is_detected_by_schema_node() -> Result<()> {
    let schema = concat!(
        r#"{"type":"record","name":"a.R","fields":[{"name":"inner","type":"#,
        r#"{"type":"record","name":"b.R","fields":[{"name":"x","type":"int"}]}}]}"#
    );
    let mut data = Vec::new();
    write_long(&mut data, 5);
    let file = TempFile::new("same_simple_name", schema, &[(1, data)]);
    let conn = connect()?;
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["{'x': 5}"]
    );
    let schema = r#"{"type":"record","name":"a.R","fields":[{"name":"next","type":["null","R"]}]}"#;
    let file = TempFile::new("recursive", schema, &[]);
    let error = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
    assert!(
        error.contains("Recursive Avro types not supported: R"),
        "{error}"
    );
    Ok(())
}

#[test]
fn names_need_not_be_identifiers() -> Result<()> {
    let schema = concat!(
        r#"{"type":"error","name":"1-root","fields":[{"name":"a-b","type":"#,
        r#"{"type":"record","name":"x y","fields":[{"name":"\"c\"","type":"int"}]}}]}"#
    );
    let mut data = Vec::new();
    write_long(&mut data, 5);
    let file = TempFile::new("lenient_names", schema, &[(1, data)]);
    let conn = connect()?;
    let sql = format!(
        "SELECT column_name, column_type FROM (DESCRIBE FROM read_avro('{}'))",
        file.path()
    );
    assert_eq!(
        query_text(&conn, &sql)?,
        ["a-b|STRUCT(\"\"\"c\"\"\" INTEGER)"]
    );
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["{'\"c\"': 5}"]
    );
    Ok(())
}

#[test]
fn row_cut_off_at_a_value_boundary_ends_the_block_early() -> Result<()> {
    let schema = record_schema(r#"{"name":"a","type":"long"},{"name":"s","type":"string"}"#);
    let mut data = Vec::new();
    write_long(&mut data, 1);
    let file = TempFile::new("cut_off", &schema, &[(1, data)]);
    let conn = connect()?;
    let message = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
    assert!(
        message.contains("Avro file block ended before its declared object count"),
        "{message}"
    );

    let mut data = Vec::new();
    write_long(&mut data, 1);
    write_long(&mut data, 10);
    data.extend_from_slice(b"ab");
    let file = TempFile::new("cut_off_value", &schema, &[(1, data)]);
    let message = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
    assert!(
        message.contains("Cannot read string value: Cannot read 10 bytes from memory buffer"),
        "{message}"
    );
    Ok(())
}

#[test]
fn metadata_needs_only_the_header() -> Result<()> {
    let schema = record_schema(r#"{"name":"a","type":"long"}"#);
    let file = TempFile::new("metadata_header", &schema, &[(1, vec![2])]);
    let mut bytes = std::fs::read(file.path()).unwrap();
    bytes.extend_from_slice(b"garbage after the blocks");
    std::fs::write(file.path(), bytes).unwrap();
    let conn = connect()?;
    let sql = format!(
        "SELECT key FROM avro_metadata('{}') ORDER BY key",
        file.path()
    );
    assert_eq!(query_text(&conn, &sql)?, ["avro.codec", "avro.schema"]);
    let message = read_error(
        &conn,
        &format!("FROM avro_metadata('{}/missing')", file.path()),
    );
    assert!(!message.is_empty());
    std::fs::write(file.path(), b"not avro").unwrap();
    let message = read_error(&conn, &format!("FROM avro_metadata('{}')", file.path()));
    assert!(
        message.contains("Failed to read Avro file: Incorrect Avro container file magic number"),
        "{message}"
    );
    Ok(())
}

#[test]
fn infinite_timestamp_ms_round_trips() -> Result<()> {
    let conn = connect()?;
    let path = std::env::temp_dir().join(format!(
        "avro_core_timestamp_ms_{}.avro",
        std::process::id()
    ));
    let file = TempFile(path);
    conn.execute(
        format!(
            "COPY (FROM VALUES ('infinity'::TIMESTAMP_MS), ('-infinity'::TIMESTAMP_MS), \
             ('2024-01-02 03:04:05.678'::TIMESTAMP_MS) t(ts)) TO '{}' (FORMAT AVRO)",
            file.path()
        ),
        Parameters::None,
    )?;
    assert_eq!(
        query_text(&conn, &format!("FROM read_avro('{}')", file.path()))?,
        ["infinity", "-infinity", "2024-01-02 03:04:05.678"]
    );
    Ok(())
}

#[test]
fn timestamp_millis_extremes() -> Result<()> {
    let schema = record_schema(concat!(
        r#"{"name":"utc","type":{"type":"long","logicalType":"timestamp-millis"}},"#,
        r#"{"name":"local","type":{"type":"long","logicalType":"local-timestamp-millis"}}"#
    ));
    let largest = i64::MAX / 1000;
    let mut data = Vec::new();
    for value in [i64::MAX, -i64::MAX, -i64::MAX, i64::MAX, largest, -largest] {
        write_long(&mut data, value);
    }
    let file = TempFile::new("timestamp_millis_extremes", &schema, &[(3, data)]);
    let conn = connect()?;
    let sql = format!(
        "SELECT utc, local FROM read_avro('{}') WHERE isinf(utc)",
        file.path()
    );
    assert_eq!(
        query_text(&conn, &sql)?,
        ["infinity|-infinity", "-infinity|infinity"]
    );
    let sql = format!(
        "SELECT epoch_us(utc), epoch_us(local) FROM read_avro('{}') WHERE NOT isinf(utc)",
        file.path()
    );
    assert_eq!(
        query_text(&conn, &sql)?,
        ["9223372036854775000|-9223372036854775000"]
    );
    for (name, value) in [
        ("timestamp_millis_min", i64::MIN),
        ("timestamp_millis_overflow", largest + 1),
        ("timestamp_millis_underflow", -largest - 1),
    ] {
        let mut data = Vec::new();
        write_long(&mut data, 0);
        write_long(&mut data, value);
        let file = TempFile::new(name, &schema, &[(1, data)]);
        let error = read_error(&conn, &format!("FROM read_avro('{}')", file.path()));
        assert!(
            error.contains(&format!(
                "Overflow in multiplication of INT64 ({value} * 1000)!"
            )),
            "{name}: {error}"
        );
    }
    Ok(())
}

#[test]
fn nulls_and_dropped_map_items_span_chunks() -> Result<()> {
    let entry = r#"{"type":"record","name":"kv","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}"#;
    let schema = record_schema(&format!(
        concat!(
            r#"{{"name":"i","type":"long"}},"#,
            r#"{{"name":"m","type":{{"type":"array","logicalType":"map","items":["null",{entry}]}}}},"#,
            r#"{{"name":"l","type":{{"type":"array","items":["null","int"]}}}},"#,
            r#"{{"name":"d","type":{{"type":"map","values":["null","long"]}}}},"#,
            r#"{{"name":"n","type":["null","long"]}}"#
        ),
        entry = entry
    ));
    let nullable_long = |data: &mut Vec<u8>, value: Option<i64>| match value {
        Some(value) => {
            write_long(data, 1);
            write_long(data, value);
        }
        None => write_long(data, 0),
    };
    let rows = 5000i64;
    let mut blocks = Vec::new();
    for start in (0..rows).step_by(1000) {
        let mut data = Vec::new();
        for i in start..start + 1000 {
            write_long(&mut data, i);
            write_long(&mut data, 2);
            write_long(&mut data, 1);
            write_long(&mut data, i);
            write_long(&mut data, 2 * i);
            write_long(&mut data, 0);
            write_long(&mut data, 0);
            write_long(&mut data, 2);
            write_long(&mut data, 0);
            write_long(&mut data, 1);
            write_long(&mut data, i);
            write_long(&mut data, 0);
            let entries: &[(&str, Option<i64>)] = if i % 2 == 0 {
                &[("a", Some(i)), ("b", None), ("a", None)]
            } else {
                &[("a", None), ("a", Some(i))]
            };
            write_long(&mut data, entries.len() as i64);
            for (key, value) in entries {
                write_bytes(&mut data, key.as_bytes());
                nullable_long(&mut data, *value);
            }
            write_long(&mut data, 0);
            nullable_long(&mut data, (i % 3 != 0).then_some(i));
        }
        blocks.push((0, Vec::new()));
        blocks.push((1000, data));
    }
    let file = TempFile::new("chunks_with_nulls", &schema, &blocks);
    let conn = connect()?;
    let sql = format!(
        concat!(
            "SELECT count(*), sum(cardinality(m)), sum(m[i::INTEGER]), count(*) FILTER (l[1] IS NULL), ",
            "sum(l[2]), count(*) FILTER (d['a'] IS NULL), sum(d['a']), count(n), sum(n), ",
            "count(*) FILTER (n IS NULL AND i % 3 <> 0) FROM read_avro('{}')"
        ),
        file.path()
    );
    let odd_sum: i64 = (0..rows).filter(|i| i % 2 == 1).sum();
    let n_values: Vec<i64> = (0..rows).filter(|i| i % 3 != 0).collect();
    let expected = [
        rows,
        rows,
        rows * (rows - 1),
        rows,
        rows * (rows - 1) / 2,
        rows / 2,
        odd_sum,
        n_values.len() as i64,
        n_values.iter().sum(),
        0,
    ]
    .map(|value| value.to_string())
    .join("|");
    assert_eq!(query_text(&conn, &sql)?, [expected]);
    Ok(())
}
