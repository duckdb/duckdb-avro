# The DuckDB Avro Extension
This repo contains a DuckDB community extension that enables DuckDB to *read* [Apache Avro (TM)](https://avro.apache.org) files. Avro is the (self-declared) "leading serialization format for record data". Avro is a self-describing *row-major* binary table format. This is in contrast to the (much more popular) Parquet format that is *columnar*. Its row-major design enables Avro - for example - to handle appends of a few rows somewhat efficiently. 

The extension also supports writing Avro files using `COPY ... TO` with `FORMAT AVRO`.

### Installation & Loading
Installation is simple through the DuckDB Community Extension repository, just type

```
INSTALL avro FROM community;
LOAD avro;
```
in a DuckDB instance near you. There is currently no build for WASM because of dependencies (sigh).

### The `read_avro` Function
The extension adds a single DuckDB function, `read_avro`. This function can be used like so:
```SQL
FROM read_avro('some_example_file.avro');
```
This function will expose the contents of the avro file as a DuckDB table. You can then use any arbitrary SQL constructs to further transform this table.


### Writing field names

By default, field names must be valid Avro identifiers. Use `SANITIZE_FIELD_NAMES true`
to convert column and nested struct field names when exporting:

```SQL
COPY (SELECT 42 AS "a key") TO 'output.avro'
    (FORMAT AVRO, SANITIZE_FIELD_NAMES true);
-- Read back as column a_x20key
```

Valid ASCII names stay unchanged. A leading digit gets an underscore (`1col` → `_1col`);
other invalid bytes become `_x` followed by uppercase hexadecimal (`a-b` → `a_x2Db`).
Non-ASCII names are escaped byte by byte using their UTF-8 encoding. This follows the
ASCII escaping convention in [Apache Iceberg's AvroSchemaUtil](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/avro/AvroSchemaUtil.java#L519).

The option defaults to `false` and requires a Boolean value. It applies recursively,
including structs inside lists and maps, but does not change map keys or `ROOT_NAME`.
`FIELD_IDS` must use the original input names. If two fields in the same record map to
the same output name, the export fails. Existing uniqueness checks for named Avro types
also apply. Exported names are not automatically restored when reading the file.

### File IO
The `read_avro` function is integrated into DuckDB's file system abstraction, meaning you can read Avro files directly from e.g. HTTP or S3 sources. For example

```SQL
FROM read_avro('http://blob.duckdb.org/data/userdata1.avro');
FROM read_avro('s3://my-example-bucket/some_example_file.avro');
```

should "just" work. 

You can also *glob* multiple files in a single read call or pass a list of files to the functions:

```SQL
FROM read_avro('some_example_file_*.avro');
FROM read_avro(['some_example_file_1.avro', 'some_example_file_2.avro']);
```

If the filenames somehow contain valuable information (as is unfortunately all-too-common), you can pass the `filename` argument to `read_avro`:

```SQL
FROM read_avro('some_example_file_*.avro', filename=true);
```
This will result in an additional column in the result set that contains the actual filename of the Avro file. 

### Schema Conversion
This extension automatically translates the Avro Schema to the DuckDB schema. *All* Avro types can be translated, except for *recursive type definitions*, which DuckDB does not support.

The type mapping is very straightforward except for Avro's "unique" way of handling `NULL`. Unlike other systems, Avro does not treat `NULL` as a possible value in a range of e.g. `INTEGER` but instead represents `NULL` as a union of the actual type with a special `NULL` type. This is different to DuckDB, where any value can be `NULL`. Of course DuckDB also supports `UNION` types, but this would be quite cumbersome to work with. 

This extension *simplifies* the Avro schema where possible: An Avro union of any type and the special null type is simplified to just the non-null type. For example, an Avro record of the union type ` ["int","null"]` becomes a DuckDB `INTEGER`, which just happens to be `NULL` sometimes. Similarly, an Avro union that contains only a single type is converted to the type it contains. For example, an Avro record of the union type ` ["int"]` also becomes a DuckDB `INTEGER`.

The extension also "flattens" the Avro schema. Avro defines tables as root-level "record" fields, which are the same as DuckDB `STRUCT` fields. For more convenient handling, this extension turns the entries of a single top-level record into top-level columns.

### Implementation
Internally, this extension uses the "official" [Apache Avro C API](https://avro.apache.org/docs/++version++/api/c/), albeit with some minor patching to allow reading of Avro files from memory.

### Limitations & Next Steps
- This extension currently does not make use of **parallelism** when reading either a single (large) Avro file or when reading a list of files. Adding support for parallelism in the latter case is on the roadmap. 

- There is currently no support for neither projection nor filter **pushdown**, but this is also planned at a later stage.

- There is currently no support for the WASM or the Windows-MinGW builds of DuckDB due to issues with the Avro library dependency (sigh again). We plan to fix this eventually.

- As mentioned above, DuckDB cannot express recursive type definitions that Avro has, this is unlikely to ever change.

- There is no support to allow users to provide a separate Avro schema file. This is unlikely to change, all Avro files we have seen so far had their schema embedded.

- There is currently no support for the `union_by_name` flag that other readers in DuckDB support. This is planned for the future.
