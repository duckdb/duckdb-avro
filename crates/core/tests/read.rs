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
    for (name, nulls, ints) in [("zero_width", 1i64 << 40, 0i64), ("ints", 0, 1 << 30)] {
        let mut data = Vec::new();
        for count in [nulls, ints] {
            write_long(&mut data, count);
            if count != 0 {
                write_long(&mut data, 0);
            }
        }
        write_long(&mut data, 1);
        let file = TempFile::new(name, &schema, &[(1, data)]);
        for columns in ["*", "a"] {
            let message = read_error(
                &conn,
                &format!("SELECT {columns} FROM read_avro('{}')", file.path()),
            );
            assert!(
                message.contains("Avro block claims more items than its data holds"),
                "{name} {columns}: {message}"
            );
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

#[test]
fn temporal_logical_types_on_bytes_are_ignored() -> Result<()> {
    let schema = record_schema(r#"{"name":"b","type":{"type":"bytes","logicalType":"date"}}"#);
    let mut data = Vec::new();
    write_bytes(&mut data, b"\x01\x02");
    let file = TempFile::new("bytes_date", &schema, &[(1, data)]);
    let conn = connect()?;
    let sql = format!("SELECT typeof(b), hex(b) FROM read_avro('{}')", file.path());
    assert_eq!(query_text(&conn, &sql)?, ["BLOB|0102"]);
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
