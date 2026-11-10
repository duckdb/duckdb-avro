//! `COPY ... TO (FORMAT AVRO)`: the layout of the written files, their schema and RETURN_STATS.

use std::path::PathBuf;

use avro_core::avro::binary::{write_bytes, write_double, write_float, write_int, write_long};
use avro_core::avro::codec::Codec;
use avro_core::avro::container::FileInfo;
use duckdb_neo::{
    Parameters, Result,
    connection::Connection,
    environment::{Environment, StorageLocation},
    vector::Unknown,
};

fn connect() -> Result<Connection> {
    let conn = Environment::new()?
        .open(StorageLocation::InMemory)?
        .connect()?;
    avro_core::copy::copy_function().register(&conn)?;
    Ok(conn)
}

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("avro_copy_files_{}_{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn path(dir: &std::path::Path, file: &str) -> String {
    dir.join(file).to_str().unwrap().to_owned()
}

fn rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>> {
    let mut result = conn.query(sql, Parameters::None)?;
    let mut rows = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        for row in 0..chunk.row_count()? {
            let mut values = Vec::new();
            for column in 0..chunk.col_count() {
                let value = chunk
                    .get_vector_at::<Unknown>(column)?
                    .get_value_slow(row)?;
                values.push(if value.is_null()? {
                    "NULL".to_owned()
                } else {
                    value.dbg_string()?
                });
            }
            rows.push(values);
        }
    }
    Ok(rows)
}

fn copy(conn: &Connection, sql: &str) -> Result<()> {
    conn.execute(sql, Parameters::None).map(|_| ())
}

fn read_file(path: &str) -> (Vec<u8>, FileInfo) {
    let data = std::fs::read(path).unwrap();
    let info = FileInfo::parse(&data).unwrap();
    (data, info)
}

fn schema_json(path: &str) -> String {
    read_file(path).1.schema.to_json()
}

fn blocks(path: &str) -> Vec<(u64, Vec<u8>)> {
    let (data, info) = read_file(path);
    info.blocks
        .iter()
        .map(|block| {
            let mut scratch = Vec::new();
            let decoded = info
                .decode_block(&data, block, &mut scratch)
                .unwrap()
                .to_vec();
            (block.object_count, decoded)
        })
        .collect()
}

#[test]
fn written_statistics_match_file_sizes() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("stats");
    let cases = [
        ("SELECT range AS a FROM range(100)", "stats.avro", 100, 395),
        (
            "SELECT a FROM (SELECT 1 a) WHERE 1 = 0",
            "empty.avro",
            0,
            138,
        ),
        (
            "SELECT range AS a FROM range(10000)",
            "big.avro",
            10000,
            31983,
        ),
        (
            "SELECT range AS a, range::VARCHAR AS b FROM range(50)",
            "cols.avro",
            50,
            495,
        ),
    ];
    for (query, file, count, size) in cases {
        let target = path(&dir, file);
        let stats = rows(
            &conn,
            &format!("COPY ({query}) TO '{target}' (FORMAT AVRO, RETURN_STATS)"),
        )?;
        assert_eq!(stats.len(), 1);
        assert!(stats[0][0].ends_with(file), "{:?}", stats[0]);
        assert_eq!(stats[0][1], count.to_string());
        assert_eq!(stats[0][2], size.to_string(), "{file}");
        assert_eq!(
            std::fs::metadata(&target).unwrap().len(),
            size as u64,
            "{file}"
        );
        let objects: u64 = blocks(&target).iter().map(|(count, _)| count).sum();
        assert_eq!(objects, count);
    }
    Ok(())
}

fn copy_to(
    conn: &Connection,
    dir: &std::path::Path,
    file: &str,
    query: &str,
    options: &str,
) -> Result<String> {
    let target = path(dir, file);
    let separator = if options.is_empty() { "" } else { ", " };
    copy(
        conn,
        &format!("COPY ({query}) TO '{target}' (FORMAT AVRO{separator}{options})"),
    )?;
    Ok(target)
}

fn data(path: &str) -> Vec<u8> {
    blocks(path)
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect()
}

fn bytes(write: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut out = Vec::new();
    write(&mut out);
    out
}

const PRESENT: u8 = 2;
const NULL: u8 = 0;

#[test]
fn schema_names_records_after_field_ids() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("field_ids");
    let cases = [
        (
            "SELECT {'a': 21, 'b': NULL, 'c': 'test'} field1",
            "FIELD_IDS {'field1': {'a': 100, 'b': 200, 'c': 300, '__duckdb_field_id': 301}}, ROOT_NAME 'test'",
            r#"{"type":"record","name":"test","fields":[{"name":"field1","type":["null",{"type":"record","name":"r301","fields":[{"name":"a","type":["null",{"type":"int"}],"field-id":100},{"name":"b","type":"null","field-id":200},{"name":"c","type":["null",{"type":"string"}],"field-id":300}]}],"field-id":301}]}"#,
        ),
        (
            "SELECT {'partition': {'partition_value': 42}} data_file",
            "FIELD_IDS {'data_file': {'__duckdb_field_id': 2, 'partition': {'__duckdb_field_id': 102, 'partition_value': 1000}}}, ROOT_NAME 'manifest_entry'",
            r#"{"type":"record","name":"manifest_entry","fields":[{"name":"data_file","type":["null",{"type":"record","name":"r2","fields":[{"name":"partition","type":["null",{"type":"record","name":"r102","fields":[{"name":"partition_value","type":["null",{"type":"int"}],"field-id":1000}]}],"field-id":102}]}],"field-id":2}]}"#,
        ),
        (
            "SELECT [{'contains_null': true, 'contains_nan': true, 'lower_bound': 'blob blob', 'upper_bound': 'blob blob blob'}] partitions",
            "FIELD_IDS {'partitions': {'list': {'contains_null': 509, 'contains_nan': 518, 'lower_bound': 510, 'upper_bound': 511, '__duckdb_field_id': 508}, '__duckdb_field_id': 507}}, ROOT_NAME 'manifest_file'",
            r#"{"type":"record","name":"manifest_file","fields":[{"name":"partitions","type":["null",{"type":"array","element-id":508,"items":["null",{"type":"record","name":"r508","fields":[{"name":"contains_null","type":["null",{"type":"boolean"}],"field-id":509},{"name":"contains_nan","type":["null",{"type":"boolean"}],"field-id":518},{"name":"lower_bound","type":["null",{"type":"string"}],"field-id":510},{"name":"upper_bound","type":["null",{"type":"string"}],"field-id":511}]}]}],"field-id":507}]}"#,
        ),
        (
            "SELECT MAP {21: 'abc', 42: 'test'} field_map, ['a', 'b', 'c'] field_list, {'a': 21, 'b': True, 'c': 1.234::FLOAT} field_struct",
            "FIELD_IDS {'field_map': {'key': 0, 'value': 1, '__duckdb_field_id': 2, '__duckdb_nullable': false}, 'field_list': {'list': 3, '__duckdb_field_id': 4, '__duckdb_nullable': false}, 'field_struct': {'a': 5, 'b': 6, 'c': 7, '__duckdb_field_id': 507, '__duckdb_nullable': false}}, ROOT_NAME 'manifest_file'",
            r#"{"type":"record","name":"manifest_file","fields":[{"name":"field_map","type":{"type":"array","logicalType": "map","items":{"type":"record","name":"k0_v1","fields":[{"name":"key","type":["null",{"type":"int"}],"field-id":0},{"name":"value","type":["null",{"type":"string"}],"field-id":1}]}},"field-id":2},{"name":"field_list","type":{"type":"array","element-id":3,"items":["null",{"type":"string"}]},"field-id":4},{"name":"field_struct","type":{"type":"record","name":"r507","fields":[{"name":"a","type":["null",{"type":"int"}],"field-id":5},{"name":"b","type":["null",{"type":"boolean"}],"field-id":6},{"name":"c","type":["null",{"type":"float"}],"field-id":7}]},"field-id":507}]}"#,
        ),
        (
            "SELECT {'__duckdb_empty_struct_marker': NULL} empty_partition",
            "FIELD_IDS {'empty_partition': {'__duckdb_field_id': 102}}, ROOT_NAME 'test'",
            r#"{"type":"record","name":"test","fields":[{"name":"empty_partition","type":["null",{"type":"record","name":"r102","fields":[]}],"field-id":102}]}"#,
        ),
        (
            "SELECT 1 field1, 2 field2, 3 field3",
            "FIELD_IDS {'field1': {'__duckdb_field_id': 301, '__duckdb_nullable': false}, 'field2': 303}, ROOT_NAME 'test'",
            r#"{"type":"record","name":"test","fields":[{"name":"field1","type":{"type":"int"},"field-id":301},{"name":"field2","type":["null",{"type":"int"}],"field-id":303},{"name":"field3","type":["null",{"type":"int"}]}]}"#,
        ),
    ];
    for (index, (query, options, expected)) in cases.into_iter().enumerate() {
        let target = copy_to(&conn, &dir, &format!("{index}.avro"), query, options)?;
        assert_eq!(schema_json(&target), expected);
    }
    Ok(())
}

#[test]
fn schema_of_every_type() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("types");
    let target = copy_to(
        &conn,
        &dir,
        "types.avro",
        "SELECT MAP {21: 'abc'} m, [{'x': 1}] l, 1.5::DECIMAL(8,2) d, '550e8400-e29b-41d4-a716-446655440000'::UUID u, \
         DATE '2024-01-01' dt, TIME '12:00:00' t, TIMESTAMP '2024-01-01' ts, TIMESTAMPTZ '2024-01-01 00:00:00+00' tz, \
         TIMESTAMP_NS '2024-01-01' tns, TIMESTAMP_MS '2024-01-01' tms, 'x'::BLOB b, 1.0::DOUBLE f8, true bo, NULL n",
        "",
    )?;
    let expected = r#"{"type":"record","name":"root","fields":[{"name":"m","type":["null",{"type":"array","logicalType": "map","items":{"type":"record","name":"record0","fields":[{"name":"key","type":["null",{"type":"int"}]},{"name":"value","type":["null",{"type":"string"}]}]}}]},{"name":"l","type":["null",{"type":"array","items":["null",{"type":"record","name":"record1","fields":[{"name":"x","type":["null",{"type":"int"}]}]}]}]},{"name":"d","type":["null",{"type":"fixed","name":"d","size":4,"logicalType":"decimal","precision":8,"scale":2}]},{"name":"u","type":["null",{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}]},{"name":"dt","type":["null",{"type":"int","logicalType":"date"}]},{"name":"t","type":["null",{"type":"long","logicalType":"time-micros"}]},{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-micros"}]},{"name":"tz","type":["null",{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}]},{"name":"tns","type":["null",{"type":"long","logicalType":"timestamp-nanos"}]},{"name":"tms","type":["null",{"type":"long","logicalType":"timestamp-micros"}]},{"name":"b","type":["null",{"type":"bytes"}]},{"name":"f8","type":["null",{"type":"double"}]},{"name":"bo","type":["null",{"type":"boolean"}]},{"name":"n","type":"null"}]}"#;
    assert_eq!(schema_json(&target), expected);
    Ok(())
}

#[test]
fn null_type_is_never_wrapped_in_a_union() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("null_type");
    copy(
        &conn,
        "CREATE TABLE null_types (id INTEGER, u \"NULL\", s STRUCT(u \"NULL\", v INTEGER), a \"NULL\"[], m MAP(VARCHAR, \"NULL\"))",
    )?;
    copy(
        &conn,
        "INSERT INTO null_types VALUES (1, NULL, {'u': NULL, 'v': 42}, [NULL, NULL], MAP {'a': NULL}), \
         (2, NULL, NULL, [], MAP {}), (3, NULL, {'u': NULL, 'v': NULL}, NULL, NULL)",
    )?;
    let plain = copy_to(&conn, &dir, "plain.avro", "FROM null_types ORDER BY id", "")?;
    let with_ids = copy_to(
        &conn,
        &dir,
        "ids.avro",
        "FROM null_types ORDER BY id",
        "FIELD_IDS {'u': {'__duckdb_field_id': 10, '__duckdb_nullable': true}, 's': {'__duckdb_field_id': 102, 'u': 1000}, \
         'a': {'__duckdb_field_id': 20, 'list': 21}, 'm': {'__duckdb_field_id': 30, 'key': 31, 'value': 32}}",
    )?;
    for target in [&plain, &with_ids] {
        let schema: serde_json::Value = serde_json::from_str(&schema_json(target)).unwrap();
        let fields = &schema["fields"];
        assert_eq!(fields[1]["type"], "null");
        assert_eq!(fields[2]["type"][1]["fields"][0]["type"], "null");
        assert_eq!(fields[3]["type"][1]["items"], "null");
        assert_eq!(fields[4]["type"][1]["items"]["fields"][1]["type"], "null");
    }
    let schema: serde_json::Value = serde_json::from_str(&schema_json(&with_ids)).unwrap();
    assert_eq!(schema["fields"][2]["type"][1]["name"], "r102");
    assert_eq!(
        schema["fields"][2]["type"][1]["fields"][0]["field-id"],
        1000
    );

    let expected = bytes(|out| {
        // id 1, u, s {u, v 42}, a [NULL, NULL], m {a: NULL}
        out.extend([PRESENT]);
        write_int(out, 1);
        out.extend([PRESENT, PRESENT]);
        write_int(out, 42);
        out.extend([PRESENT]);
        write_long(out, 2);
        write_long(out, 0);
        out.extend([PRESENT]);
        write_long(out, 1);
        out.extend([PRESENT]);
        write_bytes(out, b"a");
        write_long(out, 0);
        // id 2, u, s NULL, a [], m {}
        out.extend([PRESENT]);
        write_int(out, 2);
        out.extend([NULL, PRESENT]);
        write_long(out, 0);
        out.extend([PRESENT]);
        write_long(out, 0);
        // id 3, u, s {u, v NULL}, a NULL, m NULL
        out.extend([PRESENT]);
        write_int(out, 3);
        out.extend([PRESENT, NULL, NULL, NULL]);
    });
    assert_eq!(data(&plain), expected);

    let required = copy_to(
        &conn,
        &dir,
        "required.avro",
        "SELECT u FROM null_types",
        "FIELD_IDS {'u': {'__duckdb_field_id': 10, '__duckdb_nullable': false}}",
    )?;
    assert_eq!(
        schema_json(&required),
        r#"{"type":"record","name":"root","fields":[{"name":"u","type":"null","field-id":10}]}"#
    );
    assert_eq!(blocks(&required), vec![(3, Vec::new())]);
    Ok(())
}

#[test]
fn sanitized_field_names() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("sanitize");
    let field_names = |target: &str| -> Vec<String> {
        let schema: serde_json::Value = serde_json::from_str(&schema_json(target)).unwrap();
        schema["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field["name"].as_str().unwrap().to_owned())
            .collect()
    };
    let target = copy_to(
        &conn,
        &dir,
        "names.avro",
        "SELECT 1 AS \"a b\", 2 AS \"1col\", 3 AS \"a-b\", 4 AS \"é\", 5 AS valid_1",
        "SANITIZE_FIELD_NAMES true",
    )?;
    assert_eq!(
        field_names(&target),
        ["a_x20b", "_1col", "a_x2Db", "_xC3_xA9", "valid_1"]
    );
    let target = copy_to(
        &conn,
        &dir,
        "bare.avro",
        "SELECT 1 AS \"a b\"",
        "SANITIZE_FIELD_NAMES",
    )?;
    assert_eq!(field_names(&target), ["a_x20b"]);

    let field_ids = |target: &str| -> Vec<i64> {
        let schema = schema_json(target);
        schema
            .split("\"field-id\":")
            .skip(1)
            .map(|rest| {
                rest.split(|c: char| !c.is_ascii_digit())
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .collect()
    };
    let target = copy_to(
        &conn,
        &dir,
        "ids.avro",
        "SELECT {'child name': 42} AS \"parent name\"",
        "SANITIZE_FIELD_NAMES true, FIELD_IDS {'parent name': {'__duckdb_field_id': 10, 'child name': 11}}",
    )?;
    assert_eq!(field_ids(&target), [11, 10]);
    assert_eq!(
        schema_json(&target),
        r#"{"type":"record","name":"root","fields":[{"name":"parent_x20name","type":["null",{"type":"record","name":"r10","fields":[{"name":"child_x20name","type":["null",{"type":"int"}],"field-id":11}]}],"field-id":10}]}"#
    );
    let target = copy_to(
        &conn,
        &dir,
        "nested_ids.avro",
        "SELECT [{'list child': 8}] AS items, MAP {'a b': {'map child': 9}} AS mapping",
        "SANITIZE_FIELD_NAMES true, FIELD_IDS {'items': {'__duckdb_field_id': 1, 'list': {'__duckdb_field_id': 2, 'list child': 3}}, \
         'mapping': {'__duckdb_field_id': 4, 'key': 5, 'value': {'__duckdb_field_id': 6, 'map child': 7}}}",
    )?;
    assert_eq!(field_ids(&target), [3, 1, 5, 7, 6, 4]);

    let target = copy_to(
        &conn,
        &dir,
        "fixed.avro",
        "SELECT 12.34::DECIMAL(8,2) AS \"decimal value\", '550e8400-e29b-41d4-a716-446655440000'::UUID AS \"uuid value\"",
        "SANITIZE_FIELD_NAMES true",
    )?;
    assert_eq!(
        schema_json(&target),
        r#"{"type":"record","name":"root","fields":[{"name":"decimal_x20value","type":["null",{"type":"fixed","name":"decimal_x20value","size":4,"logicalType":"decimal","precision":8,"scale":2}]},{"name":"uuid_x20value","type":["null",{"type":"fixed","name":"uuid_x20value","size":16,"logicalType":"uuid"}]}]}"#
    );
    copy_to(
        &conn,
        &dir,
        "scopes.avro",
        "SELECT {'a b': 1} AS first, {'a_x20b': 2} AS second",
        "SANITIZE_FIELD_NAMES true",
    )?;
    Ok(())
}

#[test]
fn values_of_every_type() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("values");
    let target = copy_to(
        &conn,
        &dir,
        "values.avro",
        "SELECT 'abc' s, '\\x01\\x02'::BLOB b, 2147483647 i, 9223372036854775807 l, 1.5::FLOAT f, 2.5::DOUBLE d, \
         true bo, NULL n, DATE '1970-01-02' dt, TIME '00:00:01' t, TIMESTAMP '1970-01-01 00:00:01' ts, \
         TIMESTAMP_MS '1970-01-01 00:00:01' tms, TIMESTAMP_NS '1970-01-01 00:00:00.000000001' tns, \
         TIMESTAMPTZ '1970-01-01 00:00:01+00' tz, '1970-01-01 00:00:00.000000002+00'::TIMESTAMPTZ_NS tzns, \
         '550e8400-e29b-41d4-a716-446655440000'::UUID u, -1.30::DECIMAL(4,2) d4, 12.23232::DECIMAL(18,5) d18, \
         -1000000000.3::DECIMAL(20,1) d20, [1, NULL] li, []::INTEGER[] le, MAP {'k': 7} m, \
         {'a': 1, 'b': NULL::VARCHAR} st, NULL::INTEGER ni",
        "",
    )?;
    let expected = bytes(|out| {
        out.push(PRESENT);
        write_bytes(out, b"abc");
        out.push(PRESENT);
        write_bytes(out, &[1, 2]);
        out.push(PRESENT);
        write_int(out, i32::MAX);
        out.push(PRESENT);
        write_long(out, i64::MAX);
        out.push(PRESENT);
        write_float(out, 1.5);
        out.push(PRESENT);
        write_double(out, 2.5);
        out.extend([PRESENT, 1]);
        out.push(PRESENT);
        write_int(out, 1);
        for micros_or_nanos in [1_000_000, 1_000_000, 1_000_000, 1, 1_000_000, 2] {
            out.push(PRESENT);
            write_long(out, micros_or_nanos);
        }
        out.push(PRESENT);
        out.extend([
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ]);
        out.push(PRESENT);
        out.extend((-130_i16).to_be_bytes());
        out.push(PRESENT);
        out.extend(1_223_232_i64.to_be_bytes());
        out.push(PRESENT);
        out.extend(&(-10_000_000_003_i128).to_be_bytes()[7..]);
        out.push(PRESENT);
        write_long(out, 2);
        out.push(PRESENT);
        write_int(out, 1);
        out.push(NULL);
        write_long(out, 0);
        out.push(PRESENT);
        write_long(out, 0);
        out.push(PRESENT);
        write_long(out, 1);
        out.push(PRESENT);
        write_bytes(out, b"k");
        out.push(PRESENT);
        write_int(out, 7);
        write_long(out, 0);
        out.extend([PRESENT, PRESENT]);
        write_int(out, 1);
        out.push(NULL);
        out.push(NULL);
    });
    assert_eq!(blocks(&target), vec![(1, expected)]);
    Ok(())
}

#[test]
fn required_fields_have_no_union_branch() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("required");
    let target = copy_to(
        &conn,
        &dir,
        "required.avro",
        "SELECT 1 field1, {'content': 1} field2, [1] field3",
        "FIELD_IDS {'field1': {'__duckdb_field_id': 1, '__duckdb_nullable': false}, \
         'field2': {'__duckdb_field_id': 2, '__duckdb_nullable': false, 'content': {'__duckdb_field_id': 3, '__duckdb_nullable': false}}, \
         'field3': {'__duckdb_field_id': 4, '__duckdb_nullable': false, 'list': {'__duckdb_field_id': 5, '__duckdb_nullable': false}}}",
    )?;
    let expected = bytes(|out| {
        write_int(out, 1);
        write_int(out, 1);
        write_long(out, 1);
        write_int(out, 1);
        write_long(out, 0);
    });
    assert_eq!(data(&target), expected);
    Ok(())
}

#[test]
fn timestamp_ms_is_written_as_micros() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("timestamp_ms");
    let target = copy_to(
        &conn,
        &dir,
        "ms.avro",
        "FROM (VALUES (TIMESTAMP_MS '1969-12-31 23:59:59.999'), (TIMESTAMP_MS '2024-01-15 12:34:56.789')) t(a)",
        "",
    )?;
    let expected = bytes(|out| {
        out.push(PRESENT);
        write_long(out, -1_000);
        out.push(PRESENT);
        write_long(out, 1_705_322_096_789_000);
    });
    assert_eq!(data(&target), expected);
    Ok(())
}

#[test]
fn constant_and_repeated_vectors() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("constant");
    let target = copy_to(
        &conn,
        &dir,
        "constant.avro",
        "SELECT {'a': 1, 'b': 'x'} s, [1, 2] l, MAP {'k': [3]} m, 'v' v, 5 i FROM range(3)",
        "",
    )?;
    let row = bytes(|out| {
        out.extend([PRESENT, PRESENT]);
        write_int(out, 1);
        out.push(PRESENT);
        write_bytes(out, b"x");
        out.push(PRESENT);
        write_long(out, 2);
        out.push(PRESENT);
        write_int(out, 1);
        out.push(PRESENT);
        write_int(out, 2);
        write_long(out, 0);
        out.push(PRESENT);
        write_long(out, 1);
        out.push(PRESENT);
        write_bytes(out, b"k");
        out.push(PRESENT);
        write_long(out, 1);
        out.push(PRESENT);
        write_int(out, 3);
        write_long(out, 0);
        write_long(out, 0);
        out.push(PRESENT);
        write_bytes(out, b"v");
        out.push(PRESENT);
        write_int(out, 5);
    });
    assert_eq!(data(&target), row.repeat(3));
    Ok(())
}

#[test]
fn codecs_compress_the_same_blocks() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("codecs");
    let query = "SELECT i a, ('row_' || i::VARCHAR) b FROM range(5000) t(i)";
    let plain = copy_to(&conn, &dir, "plain.avro", query, "")?;
    let plain_blocks = blocks(&plain);
    assert_eq!(read_file(&plain).1.codec, Codec::Null);
    for (option, codec) in [
        ("null", Codec::Null),
        ("deflate", Codec::Deflate),
        ("DEFLATE", Codec::Deflate),
        ("snappy", Codec::Snappy),
        ("zstandard", Codec::Zstandard),
        ("bzip2", Codec::Bzip2),
        ("lzma", Codec::Lzma),
        ("", Codec::Null),
    ] {
        let target = copy_to(
            &conn,
            &dir,
            &format!("{option}.avro"),
            query,
            &format!("CODEC '{option}'"),
        )?;
        let (_, info) = read_file(&target);
        assert_eq!(info.codec, codec);
        assert_eq!(
            info.metadata[0],
            ("avro.codec".to_owned(), codec.name().as_bytes().to_vec())
        );
        assert_eq!(blocks(&target), plain_blocks, "{option}");
    }
    Ok(())
}

#[test]
fn metadata_follows_the_schema() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("metadata");
    let target = copy_to(
        &conn,
        &dir,
        "metadata.avro",
        "SELECT 42 a",
        "METADATA {'key': 'value', 'key2': 42, 'key3': [1, 2], 'key4': NULL}, ROOT_NAME 'test'",
    )?;
    let (_, info) = read_file(&target);
    let keys = info
        .metadata
        .iter()
        .map(|(key, _)| key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        ["avro.codec", "avro.schema", "key", "key2", "key3", "key4"]
    );
    let values = info.metadata[2..]
        .iter()
        .map(|(_, value)| value.as_slice())
        .collect::<Vec<_>>();
    assert_eq!(values, [b"value".as_slice(), b"42", b"[1, 2]", b"NULL"]);
    Ok(())
}

#[test]
fn every_row_is_kept_across_block_boundaries() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("block_boundary");
    copy(&conn, "SET threads = 1")?;
    for length in [1, 8, 31, 1000] {
        let target = copy_to(
            &conn,
            &dir,
            &format!("boundary_{length}.avro"),
            &format!("SELECT repeat('x', {length}) AS s FROM range(8192)"),
            "",
        )?;
        let file_blocks = blocks(&target);
        assert_eq!(
            file_blocks.iter().map(|(count, _)| count).sum::<u64>(),
            8192
        );
        let row = bytes(|out| {
            out.push(PRESENT);
            write_bytes(out, "x".repeat(length).as_bytes());
        });
        let all = file_blocks
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect::<Vec<_>>();
        assert_eq!(all, row.repeat(8192));
    }
    let partitioned = dir.join("partitioned");
    copy(
        &conn,
        &format!(
            "COPY (SELECT range AS id, md5(range::VARCHAR) AS s, range % 30 AS p FROM range(50000)) TO '{}' (FORMAT AVRO, PARTITION_BY (p))",
            partitioned.to_str().unwrap()
        ),
    )?;
    let mut objects = 0;
    let mut files = 0;
    for partition in std::fs::read_dir(&partitioned).unwrap() {
        for file in std::fs::read_dir(partition.unwrap().path()).unwrap() {
            let file = file.unwrap().path();
            objects += blocks(file.to_str().unwrap())
                .iter()
                .map(|(count, _)| count)
                .sum::<u64>();
            files += 1;
        }
    }
    assert_eq!(files, 30);
    assert_eq!(objects, 50000);
    Ok(())
}

#[test]
fn every_chunk_is_one_block() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("block_sizes");
    for (file, query, expected) in [
        (
            "small_rows.avro",
            "SELECT range AS a FROM range(5000)",
            vec![2048, 2048, 904],
        ),
        // Rows larger than their size estimate once put every row in a block of its own
        (
            "large_rows.avro",
            "SELECT repeat('x', 126) AS s FROM range(10)",
            vec![10],
        ),
    ] {
        let target = copy_to(&conn, &dir, file, query, "")?;
        let counts = blocks(&target)
            .iter()
            .map(|(count, _)| *count)
            .collect::<Vec<_>>();
        assert_eq!(counts, expected, "{file}");
    }
    Ok(())
}
