//! Running SQL: parsing into statements, executing them, and streaming the result chunks.

use crate::chunk::DataChunk;
use crate::collection::ColumnDataCollection;
use crate::context::{Connection, Context};
use crate::sys::*;
use crate::text::{bytes_arg, render, str_arg, str_to_string};
use crate::types::LogicalType;
use crate::value::Value;
use crate::{Error, Result};

/// `DUCKDB_V2_STATEMENT_TYPE`.
pub type StatementType = DUCKDB_V2_STATEMENT_TYPE;
/// `DUCKDB_V2_RESULT_TYPE` (`StatementReturnType`: `QUERY_RESULT` / `CHANGED_ROWS` / `NOTHING`).
pub type ResultType = DUCKDB_V2_RESULT_TYPE;

/// A parsed SQL statement (owned).
pub struct Statement {
    raw: duckdb_v2_sql_statement_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_sql_statement_handle` (`duckdb_v2.h`: "a single parsed SQL
// statement"; `statement_execute` "runs it without consuming it"). The parsed statement is a plain value without thread
// affinity, so it moves between threads as a whole; not `Sync`, the statement is prepared / executed / annotated (the
// shell's explain format) by one thread at a time.
unsafe impl Send for Statement {}

impl Statement {
    pub fn as_raw(&self) -> duckdb_v2_sql_statement_handle {
        self.raw
    }

    pub fn statement_type(&self) -> Result<StatementType> {
        let mut st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
        api_call!(duckdb_v2_sql_statement_get_type(self.raw, &mut st));
        Ok(st)
    }

    /// The statement's text.
    pub fn text(&self) -> Result<String> {
        let mut text = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_sql_statement_get_text(self.raw, &mut text));
        Ok(str_to_string(text))
    }

    pub fn parameter_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_sql_statement_get_parameter_count(self.raw, &mut count));
        Ok(count as usize)
    }

    /// The name of parameter `index`: the digits of a positional parameter (`$1`, `?` numbered in order), the
    /// identifier of a named one.
    pub fn parameter_name(&self, index: usize) -> Result<String> {
        let mut name = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_sql_statement_get_parameter_name(self.raw, index as idx_t, &mut name));
        Ok(str_to_string(name))
    }

    /// `SQLStatement::has_anonymous_parameters`: a positional (`?` / `$1`) parameter is present.
    pub fn has_anonymous_parameters(&self) -> Result<bool> {
        for i in 0..self.parameter_count()? {
            let name = self.parameter_name(i)?;
            if !name.is_empty() && name.bytes().all(|b| b.is_ascii_digit()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Binds `collection` to the statement as the table `name` (`duckdb_v2_statement_add_collection`): the rows
    /// are visible to the statement under that name when it executes, the way `ClientContext::Append` sees an
    /// appender's rows. `column_names` renames the columns (empty: the collection's).
    pub fn add_collection(&self, name: &str, collection: &ColumnDataCollection, column_names: &[&str]) -> Result<()> {
        let name = str_arg(name);
        let names: Vec<duckdb_v2_identifier_t> = column_names.iter().map(|n| str_arg(n)).collect();
        api_call!(duckdb_v2_statement_add_collection(
            self.raw,
            &name,
            collection.as_raw(),
            if names.is_empty() { std::ptr::null() } else { names.as_ptr() },
            names.len() as idx_t
        ));
        Ok(())
    }

    /// `SQLStatement::ToString()`: the statement rendered back to SQL (`duckdb_v2_sql_statement_to_sql`, Rust
    /// engine only).
    pub fn to_sql(&self) -> Result<String> {
        render(|buffer, capacity, needed| {
            api_call!(duckdb_v2_sql_statement_to_sql(self.raw, buffer, capacity, needed));
            Ok(())
        })
    }

    /// Takes ownership of a statement handle the API returned.
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_sql_statement_handle) -> Statement {
        Statement { raw }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_sql_statement_destroy(&mut self.raw));
    }
}

/// The statements of a parsed text, one `next` at a time (owned; `duckdb_v2_statement_iterator_handle`). A parse
/// error is reported by the `next` that reaches it, after the statements before it.
pub struct StatementIterator {
    raw: duckdb_v2_statement_iterator_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_statement_iterator_handle` (`duckdb_v2.h`: "an iterator over
// the statements of a SQL string"); `next` advances it through `&self`, so it is used by one thread at a time and moves
// between threads as a whole.
unsafe impl Send for StatementIterator {}

impl StatementIterator {
    /// Takes ownership of an iterator handle the API returned.
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live, and nothing else may
    /// destroy it (the wrapper destroys it on drop).
    pub unsafe fn from_raw(raw: duckdb_v2_statement_iterator_handle) -> StatementIterator {
        StatementIterator { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_statement_iterator_handle {
        self.raw
    }

    /// The next statement, `None` at the end (`duckdb_v2_statement_iterator_next`).
    pub fn next(&self) -> Result<Option<Statement>> {
        let mut raw: duckdb_v2_sql_statement_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_statement_iterator_next(self.raw, &mut raw));
        Ok(if raw.is_null() { None } else { Some(Statement { raw }) })
    }

    /// Every remaining statement.
    pub fn collect(&self) -> Result<Vec<Statement>> {
        let mut statements = Vec::new();
        while let Some(statement) = self.next()? {
            statements.push(statement);
        }
        Ok(statements)
    }
}

impl Drop for StatementIterator {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_statement_iterator_destroy(&mut self.raw));
    }
}

/// The skip flags and layout of the engine's JSON serializer of statements and plans (`JsonSerializer` +
/// `WRITE_PRETTY_FLAG`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SerializeFlags {
    pub skip_if_null: bool,
    pub skip_if_empty: bool,
    pub skip_if_default: bool,
    /// Pretty-printed output.
    pub format: bool,
}

impl Context {
    /// Whether the loading engine serializes statements and plans to JSON and renders statements back to SQL (the
    /// `duckdb_v2_context_*_json` entries and `duckdb_v2_sql_statement_to_sql`, newer engines only).
    pub fn statement_json_available() -> bool {
        let api = crate::api::api();
        api.duckdb_v2_context_serialize_sql_json.is_some()
            && api.duckdb_v2_context_serialize_plan_json.is_some()
            && api.duckdb_v2_context_deserialize_sql_json.is_some()
            && api.duckdb_v2_sql_statement_to_sql.is_some()
    }

    /// The SELECT statements of `sql` as a JSON array through the engine's `JsonSerializer`
    /// (`duckdb_v2_context_serialize_sql_json`; what `json_serialize_sql` returns).
    pub fn serialize_sql_json(&self, sql: &[u8], flags: SerializeFlags) -> Result<String> {
        let view = bytes_arg(sql);
        render(|buffer, capacity, needed| {
            api_call!(duckdb_v2_context_serialize_sql_json(
                self.as_raw(),
                &view,
                flags.skip_if_null,
                flags.skip_if_empty,
                flags.skip_if_default,
                flags.format,
                buffer,
                capacity,
                needed
            ));
            Ok(())
        })
    }

    /// The logical plans of the statements of `sql` as a JSON array (`duckdb_v2_context_serialize_plan_json`; what
    /// `json_serialize_plan` returns), optimized or not.
    pub fn serialize_plan_json(&self, sql: &[u8], optimize: bool, flags: SerializeFlags) -> Result<String> {
        let view = bytes_arg(sql);
        render(|buffer, capacity, needed| {
            api_call!(duckdb_v2_context_serialize_plan_json(
                self.as_raw(),
                &view,
                optimize,
                flags.skip_if_null,
                flags.skip_if_empty,
                flags.skip_if_default,
                flags.format,
                buffer,
                capacity,
                needed
            ));
            Ok(())
        })
    }

    /// The statements of a serialized statements array (`duckdb_v2_context_deserialize_sql_json`; what
    /// `json_deserialize_sql` parses), in order.
    pub fn deserialize_sql_json(&self, statements_json: &[u8]) -> Result<Vec<Statement>> {
        let view = bytes_arg(statements_json);
        let mut iterator: duckdb_v2_statement_iterator_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_context_deserialize_sql_json(self.as_raw(), &view, &mut iterator));
        StatementIterator { raw: iterator }.collect()
    }
}

/// A streaming query result (owned).
pub struct QueryResult {
    raw: duckdb_v2_result_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_result_handle` (`duckdb_v2.h`: "Single consumer: step from
// one thread at a time"), so `Send` and not `Sync`: the stream moves to another thread as a whole and is stepped there.
unsafe impl Send for QueryResult {}

/// The outcome of one [`QueryResult::step`].
pub enum StepStatus {
    /// A chunk of rows.
    Chunk(DataChunk),
    /// Nothing is ready yet: [`QueryResult::wait`] blocks until something is.
    Waiting,
    /// The result is complete.
    Finished,
    /// The statement was interrupted.
    Cancelled,
}

impl QueryResult {
    /// Takes ownership of a result handle the API returned.
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live, and nothing else may
    /// destroy it (the wrapper destroys it on drop).
    pub unsafe fn from_raw(raw: duckdb_v2_result_handle) -> QueryResult {
        QueryResult { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_result_handle {
        self.raw
    }

    /// The result columns as `(name, type)`. Before an expanding statement (one the engine binds while it runs) is
    /// bound, the error carries `DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED` ([`Error::is_metadata_deferred`]): read the
    /// schema again after the first chunk.
    pub fn schema(&self) -> Result<Vec<(String, LogicalType)>> {
        let mut schema: duckdb_v2_schema_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_result_get_schema(self.raw, &mut schema));
        let outcome = (|| {
            let mut count: idx_t = 0;
            api_call!(duckdb_v2_schema_get_count(schema, &mut count));
            let mut fields = Vec::with_capacity(count as usize);
            for i in 0..count {
                let mut name = duckdb_v2_str::EMPTY;
                let mut type_raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
                api_call!(duckdb_v2_schema_get_field(schema, i, &mut name, &mut type_raw));
                // the field's type is lent by the schema (valid until the schema is destroyed): copy it out
                let mut copy: duckdb_v2_logical_type_handle = std::ptr::null_mut();
                api_call!(duckdb_v2_logical_type_copy(type_raw, &mut copy));
                // SAFETY(ffi L8.I1): `duckdb_v2_logical_type_copy` returns an owned type handle.
                fields.push((str_to_string(name), unsafe { LogicalType::from_raw(copy) }));
            }
            Ok(fields)
        })();
        api_destroy!(duckdb_v2_schema_destroy(&mut schema));
        outcome
    }

    pub fn statement_type(&self) -> Result<StatementType> {
        let mut st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
        api_call!(duckdb_v2_result_get_statement_type(self.raw, &mut st));
        Ok(st)
    }

    /// `StatementProperties::return_type`: whether the statement returns rows, a changed-row count or nothing
    /// (deferred like [`QueryResult::schema`] for an expanding statement).
    pub fn result_type(&self) -> Result<ResultType> {
        let mut result_type = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
        api_call!(duckdb_v2_result_get_result_type(self.raw, &mut result_type));
        Ok(result_type)
    }

    /// One non-blocking step of a streaming result (`duckdb_v2_result_step`): a chunk when one is ready, else
    /// whether to [`QueryResult::wait`], and the end or the cancellation of the stream.
    pub fn step(&self) -> Result<StepStatus> {
        let mut chunk: duckdb_v2_data_chunk_handle = std::ptr::null_mut();
        let mut status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
        api_call!(duckdb_v2_result_step(self.raw, &mut chunk, &mut status));
        Ok(match status {
            DUCKDB_V2_RESULT_STEP_STATUS_CHUNK if !chunk.is_null() => {
                // SAFETY(ffi L8.I1): a CHUNK status hands out an owned chunk.
                StepStatus::Chunk(unsafe { DataChunk::from_raw(chunk) })
            }
            DUCKDB_V2_RESULT_STEP_STATUS_FINISHED => StepStatus::Finished,
            DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED => StepStatus::Cancelled,
            _ => StepStatus::Waiting,
        })
    }

    /// Blocks until the next [`QueryResult::step`] has something to report (`duckdb_v2_result_wait`).
    pub fn wait(&self) -> Result<()> {
        api_call!(duckdb_v2_result_wait(self.raw));
        Ok(())
    }

    /// Blocks for the next chunk; `None` at the end of the stream.
    pub fn fetch_chunk(&self) -> Result<Option<DataChunk>> {
        let mut chunk: duckdb_v2_data_chunk_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_result_fetch_chunk(self.raw, &mut chunk));
        // SAFETY(ffi L8.I1): the entry returns an owned chunk, or null at the end of the result.
        if chunk.is_null() { Ok(None) } else { Ok(Some(unsafe { DataChunk::from_raw(chunk) })) }
    }

    /// Consumes the rest of the stream, returning the number of rows it still had.
    pub fn drain(&self) -> Result<usize> {
        let mut rows: idx_t = 0;
        api_call!(duckdb_v2_result_drain(self.raw, &mut rows));
        Ok(rows as usize)
    }

    /// Every remaining row as values (small results only).
    pub fn collect_rows(&self) -> Result<Vec<Vec<Value>>> {
        let mut rows = Vec::new();
        while let Some(chunk) = self.fetch_chunk()? {
            let columns = chunk.column_count()?;
            for row in 0..chunk.size()? {
                let mut values = Vec::with_capacity(columns);
                for column in 0..columns {
                    values.push(chunk.vector(column)?.value(row)?);
                }
                rows.push(values);
            }
        }
        Ok(rows)
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_result_destroy(&mut self.raw));
    }
}

impl Connection {
    /// `ClientContext::IterateStatements`: the statements of `sql` as an iterator (`duckdb_v2_parse_sql`). The text
    /// is handed to the parser as it is (bytes that are not UTF-8 are its to reject); a NUL byte is an error. Parsing
    /// touches neither the catalog nor a transaction: the connection supplies the parser options and parser
    /// extensions.
    pub fn parse_statements(&self, sql: &[u8]) -> Result<StatementIterator> {
        let csql = std::ffi::CString::new(sql).map_err(|_| Error::invalid_input("the query contains a NUL byte"))?;
        let mut iterator: duckdb_v2_statement_iterator_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_parse_sql(self.as_raw(), csql.as_ptr(), &mut iterator));
        Ok(StatementIterator { raw: iterator })
    }

    /// Parses `sql` into its statements.
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>> {
        self.parse_statements(sql.as_bytes())?.collect()
    }

    /// `Connection::Prepare` of a parsed statement (`duckdb_v2_prepared_statement_create`); `require_cacheable`
    /// refuses a plan that would not be reused across executions.
    pub fn prepare(&self, statement: &Statement, require_cacheable: bool) -> Result<PreparedStatement> {
        let mut raw: duckdb_v2_prepared_statement_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_prepared_statement_create(self.as_raw(), statement.raw, require_cacheable, &mut raw));
        Ok(PreparedStatement { raw })
    }

    /// Executes one parsed statement with the Arrow C data interface as the output format
    /// (`duckdb_v2_statement_execute_arrow`): arrays of `batch_size` rows.
    pub fn execute_statement_arrow(
        &self,
        statement: &Statement,
        parameters: &[(&str, &Value)],
        batch_size: usize,
    ) -> Result<ArrowResult> {
        let names: Vec<duckdb_v2_identifier_t> = parameters.iter().map(|(n, _)| str_arg(n)).collect();
        let values: Vec<duckdb_v2_value_handle> = parameters.iter().map(|(_, v)| v.as_raw()).collect();
        let mut raw: duckdb_v2_arrow_result_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_statement_execute_arrow(
            self.as_raw(),
            statement.as_raw(),
            if names.is_empty() { std::ptr::null() } else { names.as_ptr() },
            if values.is_empty() { std::ptr::null() } else { values.as_ptr() },
            parameters.len() as idx_t,
            batch_size as idx_t,
            &mut raw
        ));
        Ok(ArrowResult { raw })
    }

    /// Executes one parsed statement with optional `(name, value)` parameters.
    pub fn execute_statement(&self, statement: &Statement, parameters: &[(&str, &Value)]) -> Result<QueryResult> {
        let names: Vec<duckdb_v2_identifier_t> = parameters.iter().map(|(n, _)| str_arg(n)).collect();
        let values: Vec<duckdb_v2_value_handle> = parameters.iter().map(|(_, v)| v.as_raw()).collect();
        let mut result: duckdb_v2_result_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_statement_execute(
            self.as_raw(),
            statement.as_raw(),
            if names.is_empty() { std::ptr::null() } else { names.as_ptr() },
            if values.is_empty() { std::ptr::null() } else { values.as_ptr() },
            parameters.len() as idx_t,
            &mut result
        ));
        Ok(QueryResult { raw: result })
    }

    /// Runs `sql` (one or more statements) and returns the result of the last one; the results of the
    /// earlier statements are drained.
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        let statements = self.parse(sql)?;
        let Some((last, rest)) = statements.split_last() else {
            return Err(crate::Error::invalid_input("No statement to execute"));
        };
        for statement in rest {
            self.execute_statement(statement, &[])?.drain()?;
        }
        self.execute_statement(last, &[])
    }

    /// Runs `sql` for its effect, draining every result.
    pub fn execute(&self, sql: &str) -> Result<()> {
        for statement in self.parse(sql)? {
            self.execute_statement(&statement, &[])?.drain()?;
        }
        Ok(())
    }
}

/// A statement bound and planned once, executable repeatedly (`PreparedStatement`, owned).
pub struct PreparedStatement {
    raw: duckdb_v2_prepared_statement_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_prepared_statement_handle` (`duckdb_v2.h`: "executable
// repeatedly ... keeps its connection's session alive"); it executes on that single-consumer session, so it is used by
// one thread at a time (`Send`, not `Sync`) and moves between threads as a whole.
unsafe impl Send for PreparedStatement {}

impl PreparedStatement {
    pub fn as_raw(&self) -> duckdb_v2_prepared_statement_handle {
        self.raw
    }

    /// `PreparedStatement::Execute` with `(name, value)` parameters (the name of a positional parameter is its
    /// number, `"1"` for `$1` / the first `?`); the result streams like [`Connection::execute_statement`]'s.
    pub fn execute(&self, parameters: &[(&str, &Value)]) -> Result<QueryResult> {
        let names: Vec<duckdb_v2_identifier_t> = parameters.iter().map(|(n, _)| str_arg(n)).collect();
        let values: Vec<duckdb_v2_value_handle> = parameters.iter().map(|(_, v)| v.as_raw()).collect();
        self.execute_raw(if names.is_empty() { std::ptr::null() } else { names.as_ptr() }, &values)
    }

    /// `PreparedStatement::Execute(values...)`: the parameters by position (`$1`, `$2`, ... / the `?`s in order).
    pub fn execute_positional(&self, parameters: &[&Value]) -> Result<QueryResult> {
        let values: Vec<duckdb_v2_value_handle> = parameters.iter().map(|v| v.as_raw()).collect();
        self.execute_raw(std::ptr::null(), &values)
    }

    fn execute_raw(
        &self,
        names: *const duckdb_v2_identifier_t,
        values: &[duckdb_v2_value_handle],
    ) -> Result<QueryResult> {
        let mut result: duckdb_v2_result_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_prepared_statement_execute(
            self.raw,
            names,
            if values.is_empty() { std::ptr::null() } else { values.as_ptr() },
            values.len() as idx_t,
            &mut result
        ));
        Ok(QueryResult { raw: result })
    }
}

impl Drop for PreparedStatement {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_prepared_statement_destroy(&mut self.raw));
    }
}

/// A result streamed as Arrow arrays (owned; [`Connection::execute_statement_arrow`]).
pub struct ArrowResult {
    raw: duckdb_v2_arrow_result_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_arrow_result_handle` (`duckdb_v2.h`: "Use it from one thread
// at a time"), so `Send` and not `Sync`.
unsafe impl Send for ArrowResult {}

impl ArrowResult {
    pub fn as_raw(&self) -> duckdb_v2_arrow_result_handle {
        self.raw
    }

    /// The next array of rows, `None` at the end (`duckdb_v2_arrow_result_fetch_array`; an array the producer did
    /// not fill - no `release` callback - is the end of the stream, as the Arrow C data interface defines it).
    pub fn fetch_array(&self) -> Result<Option<ArrowBatch>> {
        let mut batch = ArrowBatch {
            array: ArrowArray {
                length: 0,
                null_count: 0,
                offset: 0,
                n_buffers: 0,
                n_children: 0,
                buffers: std::ptr::null_mut(),
                children: std::ptr::null_mut(),
                dictionary: std::ptr::null_mut(),
                release: None,
                private_data: std::ptr::null_mut(),
            },
        };
        api_call!(duckdb_v2_arrow_result_fetch_array(self.raw, &mut batch.array));
        Ok(if batch.array.release.is_some() { Some(batch) } else { None })
    }
}

impl Drop for ArrowResult {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_arrow_result_destroy(&mut self.raw));
    }
}

/// One `ArrowArray` a result produced, released through its own callback when dropped.
pub struct ArrowBatch {
    array: ArrowArray,
}

impl ArrowBatch {
    /// `ArrowArray::length`: the rows of the batch.
    pub fn len(&self) -> usize {
        self.array.length.max(0) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The array itself (the buffers belong to the producer until the batch is dropped).
    pub fn array(&self) -> &ArrowArray {
        &self.array
    }
}

impl Drop for ArrowBatch {
    fn drop(&mut self) {
        if let Some(release) = self.array.release.take() {
            // SAFETY(ffi L8.I4): L8.I4 - `release` is the producer's callback for this very array, called once with it;
            // the producer nulls `release` itself, and `take` makes sure this is the only call.
            unsafe { release(&mut self.array) };
        }
    }
}
