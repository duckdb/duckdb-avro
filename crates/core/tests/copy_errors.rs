//! `COPY ... TO (FORMAT AVRO)`: the errors of invalid statements, options and data.

use std::path::PathBuf;

use duckdb_neo::{
    Parameters, Result,
    connection::Connection,
    environment::{Environment, StorageLocation},
    error::DuckDBError,
};

fn connect() -> Result<Connection> {
    let conn = Environment::new()?
        .open(StorageLocation::InMemory)?
        .connect()?;
    avro_core::copy::copy_function().register(&conn)?;
    Ok(conn)
}

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("avro_copy_errors_{}_{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn copy_error(
    conn: &Connection,
    dir: &std::path::Path,
    query: &str,
    options: &str,
) -> duckdb_neo::Error {
    let target = dir.join("out.avro");
    let sql = format!(
        "COPY ({query}) TO '{}' ({options})",
        target.to_str().unwrap()
    );
    match conn.execute(sql.as_str(), Parameters::None) {
        Ok(_) => panic!("expected an error from {sql}"),
        Err(error) => error,
    }
}

#[track_caller]
fn assert_error(error: &duckdb_neo::Error, code: DuckDBError, message: &str) {
    assert_eq!(error.code, code, "{}", error.message);
    assert!(
        error.message.contains(message),
        "{:?} does not contain {:?}",
        error.message,
        message
    );
}

fn error_of(conn: &Connection, sql: &str) -> duckdb_neo::Error {
    match conn.execute(sql, Parameters::None) {
        Ok(_) => panic!("expected an error from {sql}"),
        Err(error) => error,
    }
}

#[test]
fn unsupported_types() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("types");
    conn.execute(
        "CREATE TYPE my_enum AS ENUM ('a', 'abc', 'decently long enum value')",
        Parameters::None,
    )?;
    let cases = [
        (
            "SELECT 'decently long enum value'::my_enum a",
            "Not implemented Error: Can't convert logical type 'ENUM('a', 'abc', 'decently long enum value')' to Avro type",
        ),
        (
            "SELECT union_value(b := 'abc')::UNION(a INTEGER, b VARCHAR, c FLOAT) a",
            "Not implemented Error: Can't convert logical type 'UNION(a INTEGER, b VARCHAR, c FLOAT)' to Avro type",
        ),
        (
            "SELECT 1::TINYINT a",
            "Can't convert logical type 'TINYINT' to Avro type",
        ),
        (
            "SELECT [1, 2]::INTEGER[2] a",
            "Can't convert logical type 'INTEGER[2]' to Avro type",
        ),
        (
            "SELECT {'a': INTERVAL 1 DAY} a",
            "Can't convert logical type 'INTERVAL' to Avro type",
        ),
    ];
    for (query, message) in cases {
        assert_error(
            &copy_error(&conn, &dir, query, "FORMAT AVRO"),
            DuckDBError::QueryNotImplemented,
            message,
        );
    }
    Ok(())
}

#[test]
fn invalid_options() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("options");
    let cases = [
        (
            "NOT_RECOGNIZED, ROOT_NAME 'test'",
            DuckDBError::ConfigurationInvalid,
            "Invalid Configuration Error: The following option(s) are not recognized: key: \"not_recognized\"",
        ),
        (
            "NOT_RECOGNIZED 'with_value', ROOT_NAME 'test'",
            DuckDBError::ConfigurationInvalid,
            "Invalid Configuration Error: The following option(s) are not recognized: key: \"not_recognized\" with value: 'with_value'",
        ),
        (
            "A_OPTION 42, Z_OPTION",
            DuckDBError::ConfigurationInvalid,
            "The following option(s) are not recognized: key: \"a_option\" with value: '42', key: \"z_option\"",
        ),
        (
            "CODEC 'nope'",
            DuckDBError::InputInvalid,
            "Unknown codec nope",
        ),
        (
            "CODEC",
            DuckDBError::InputInvalid,
            "CODEC can not be provided without a value",
        ),
        (
            "CODEC 42",
            DuckDBError::InputInvalid,
            "'CODEC' is expected to be provided as VARCHAR (e.g. 'deflate', 'null')",
        ),
        (
            "METADATA",
            DuckDBError::InputInvalid,
            "METADATA can not be provided without a value",
        ),
        (
            "METADATA 'key'",
            DuckDBError::InputInvalid,
            "'METADATA' is expected to be provided as a STRUCT of key-value string metadata",
        ),
        (
            "FIELD_IDS",
            DuckDBError::InputInvalid,
            "FIELD_IDS can not be provided without a value",
        ),
        (
            "ROOT_NAME",
            DuckDBError::InputInvalid,
            "ROOT_NAME can not be provided without a value",
        ),
        (
            "ROOT_NAME 42",
            DuckDBError::InputInvalid,
            "'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name of the top level 'record'",
        ),
        (
            "ROOT_NAME 'invalid root'",
            DuckDBError::InputInvalid,
            "'invalid root' is not a valid Avro identifier\nThe identifier has to match the regex: [A-Za-z_][A-Za-z0-9_]*",
        ),
        (
            "ROOT_NAME ''",
            DuckDBError::InputInvalid,
            "Invalid Avro identifier",
        ),
        (
            "ROOT_NAME NULL",
            DuckDBError::InputInvalid,
            "'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name of the top level 'record'",
        ),
        (
            "CODEC NULL",
            DuckDBError::InputInvalid,
            "'CODEC' is expected to be provided as VARCHAR (e.g. 'deflate', 'null')",
        ),
        (
            "SANITIZE_FIELD_NAMES NULL",
            DuckDBError::InputInvalid,
            "SANITIZE_FIELD_NAMES requires a non-NULL BOOLEAN value",
        ),
        (
            "SANITIZE_FIELD_NAMES 'yes'",
            DuckDBError::InputInvalid,
            "SANITIZE_FIELD_NAMES requires a non-NULL BOOLEAN value",
        ),
    ];
    for (options, code, message) in cases {
        assert_error(
            &copy_error(
                &conn,
                &dir,
                "SELECT 42 a",
                &format!("FORMAT AVRO, {options}"),
            ),
            code,
            message,
        );
    }
    // Option names are matched case-insensitively.
    let target = dir.join("ok.avro");
    conn.execute(
        format!(
            "COPY (SELECT 1 a) TO '{}' (FORMAT AVRO, root_name 'x', Codec 'Snappy')",
            target.to_str().unwrap()
        )
        .as_str(),
        Parameters::None,
    )?;
    Ok(())
}

#[test]
fn invalid_names() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("names");
    let invalid = [
        ("SELECT range(10000)", "FORMAT AVRO"),
        ("SELECT 1 AS \"a b\"", "FORMAT AVRO"),
        (
            "SELECT 1 AS \"a b\"",
            "FORMAT AVRO, SANITIZE_FIELD_NAMES false",
        ),
    ];
    for (query, options) in invalid {
        assert_error(
            &copy_error(&conn, &dir, query, options),
            DuckDBError::InputInvalid,
            "Invalid Avro identifier",
        );
    }
    assert_error(
        &copy_error(&conn, &dir, "SELECT {'x': 1} AS \"a b\"", "FORMAT AVRO"),
        DuckDBError::InputInvalid,
        "'a b' is not a valid Avro identifier",
    );
    let collisions = [
        (
            "SELECT 1 AS \"a b\", 2 AS a_x20b",
            "SANITIZE_FIELD_NAMES maps both 'a b' and 'a_x20b' to 'a_x20b' in the same Avro record",
        ),
        (
            "SELECT {'1a': 1, '_1a': 2} AS nested",
            "SANITIZE_FIELD_NAMES maps both '1a' and '_1a' to '_1a' in the same Avro record",
        ),
    ];
    for (query, message) in collisions {
        assert_error(
            &copy_error(&conn, &dir, query, "SANITIZE_FIELD_NAMES true"),
            DuckDBError::QueryBinder,
            message,
        );
    }
    let duplicates = [
        (
            "SELECT {'value': 84} data_file",
            "FIELD_IDS {'data_file': {'__duckdb_field_id': 2, 'value': 3}}, ROOT_NAME 'r2'",
            "r2",
        ),
        ("SELECT {'x': 1} a, {'a': {'y': 2}} b", "FORMAT AVRO", "a"),
        ("SELECT 1.5::DECIMAL(4,1) root", "FORMAT AVRO", "root"),
        (
            "SELECT [{'x': 1}] a, 1 record0",
            "ROOT_NAME 'record0'",
            "record0",
        ),
    ];
    for (query, options, name) in duplicates {
        assert_error(
            &copy_error(&conn, &dir, query, options),
            DuckDBError::QueryBinder,
            &format!(
                "Binder Error: Avro schema by the name of '{name}' already exists, names of 'record', 'enum' and 'fixed' types have to be distinct"
            ),
        );
    }
    Ok(())
}

#[test]
fn invalid_field_ids() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("field_ids");
    let cases = [
        (
            "SELECT 1 a",
            "FIELD_IDS 42",
            DuckDBError::QueryBinder,
            "Expected FIELD_IDS to be a STRUCT, e.g., {col1: 42, col2: {__duckdb_field_id: 43, nested_col: 44}, col3: 44}",
        ),
        (
            "SELECT 1 a, 2 \"B\"",
            "FIELD_IDS {'c': 1}",
            DuckDBError::QueryBinder,
            "Column name \"c\" specified in FIELD_IDS not found. Consider using WRITE_PARTITION_COLUMNS if this column is a partition column. Available column names: [a, b]",
        ),
        (
            "SELECT 1 a, 2 b",
            "FIELD_IDS {'a': 1, 'B': 1}",
            DuckDBError::QueryBinder,
            "Duplicate field_id 1 found in FIELD_IDS",
        ),
        (
            "SELECT 1 a",
            "FIELD_IDS {'a': {'__duckdb_field_id': 1, 'nested': 2}}",
            DuckDBError::QueryBinder,
            "Column \"a\" with type \"INTEGER\" cannot have a nested FIELD_IDS specification",
        ),
        (
            "SELECT 1 __duckdb_field_id",
            "FIELD_IDS {'a': 1}",
            DuckDBError::QueryBinder,
            "Cannot have a column named \"__duckdb_field_id\" when writing FIELD_IDS",
        ),
        (
            "SELECT {'__duckdb_field_id': 1} a",
            "FIELD_IDS {'a': {'x': 2}}",
            DuckDBError::QueryBinder,
            "Cannot have column named \"__duckdb_field_id\" with FIELD_IDS",
        ),
        (
            "SELECT NULL field1",
            "FIELD_IDS {'field1': {'__duckdb_field_id': 301, '__duckdb_nullable': 'foo'}}",
            DuckDBError::InputInvalid,
            "Invalid Input Error: Failed to cast value:",
        ),
        (
            "SELECT 1 a",
            "FIELD_IDS {'a': 'x'}",
            DuckDBError::InputInvalid,
            "Failed to cast value:",
        ),
        (
            "SELECT 1 a",
            "FIELD_IDS {'a': {'__duckdb_nullable': false}}",
            DuckDBError::QueryBinder,
            "Field id of the field is not set",
        ),
    ];
    for (query, options, code, message) in cases {
        assert_error(&copy_error(&conn, &dir, query, options), code, message);
    }
    Ok(())
}

#[test]
fn null_into_required_field() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("required");
    let cases = [
        (
            "SELECT NULL::INTEGER field1",
            "FIELD_IDS {'field1': {'__duckdb_field_id': 301, '__duckdb_nullable': false}}",
            "Cannot insert NULL to non-nullable field of type INTEGER",
        ),
        (
            "SELECT 1 manifest_entry, {'content': 1} data_file_not_nullable, {'content': NULL::INTEGER} data_file_nullable",
            "FIELD_IDS {'manifest_entry': {'__duckdb_field_id': 0}, \
             'data_file_not_nullable': {'__duckdb_field_id': 2, '__duckdb_nullable': false, 'content': {'__duckdb_field_id': 3, '__duckdb_nullable': false}}, \
             'data_file_nullable': {'__duckdb_field_id': 4, 'content': {'__duckdb_field_id': 5, '__duckdb_nullable': false}}}",
            "Cannot insert NULL to non-nullable field of type INTEGER",
        ),
        (
            "SELECT [1, NULL]::BIGINT[] l",
            "FIELD_IDS {'l': {'__duckdb_field_id': 1, 'list': {'__duckdb_field_id': 2, '__duckdb_nullable': false}}}",
            "Cannot insert NULL to non-nullable field of type BIGINT",
        ),
        (
            "SELECT NULL::STRUCT(a INTEGER) s",
            "FIELD_IDS {'s': {'__duckdb_field_id': 1, '__duckdb_nullable': false}}",
            "Cannot insert NULL to non-nullable field of type STRUCT",
        ),
    ];
    for (query, options, message) in cases {
        assert_error(
            &copy_error(&conn, &dir, query, options),
            DuckDBError::InputInvalid,
            message,
        );
    }
    Ok(())
}

#[test]
fn decimal_width_is_checked_by_duckdb() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("decimal");
    let error = copy_error(
        &conn,
        &dir,
        "FROM (VALUES (-1.3::DECIMAL(10000,1)), (NULL)) t(decimal_col)",
        "FORMAT AVRO",
    );
    assert!(
        error
            .message
            .contains("Type \"DECIMAL\" does not accept type parameters"),
        "{}",
        error.message
    );
    Ok(())
}

#[test]
fn failed_copy_leaves_no_file() -> Result<()> {
    let conn = connect()?;
    let dir = test_dir("failed");
    let target = dir.join("out.avro");
    let sql = format!(
        "COPY (SELECT NULL::INTEGER field1) TO '{}' (FIELD_IDS {{'field1': {{'__duckdb_field_id': 301, '__duckdb_nullable': false}}}})",
        target.to_str().unwrap()
    );
    error_of(&conn, &sql);
    assert!(!target.exists());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn write_failure_in_header() -> Result<()> {
    let conn = connect()?;
    for _ in 0..3 {
        let error = error_of(
            &conn,
            "COPY (SELECT 42 a) TO '/dev/full' (FORMAT AVRO, CODEC 'deflate')",
        );
        assert!(
            error.message.contains("No space left on device"),
            "{}",
            error.message
        );
    }
    let dir = test_dir("after_failure");
    let target = dir.join("after.avro");
    conn.execute(
        format!(
            "COPY (SELECT 42 a) TO '{}' (FORMAT AVRO, CODEC 'deflate')",
            target.to_str().unwrap()
        )
        .as_str(),
        Parameters::None,
    )?;
    assert!(target.exists());
    Ok(())
}
