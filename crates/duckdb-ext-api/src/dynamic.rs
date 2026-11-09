//! Dynamic-dispatch function builders: a Rust trait object per function (`Arc<dyn ScalarImpl>`,
//! `Arc<dyn TableImpl>`, plain `fn` pointers for casts and query pragmas) behind the static-dispatch traits of this
//! crate, for extensions that build their functions from data - tables of overloads, generated signatures, one
//! implementation shared by many names - rather than one type per callback.
//!
//! Every callback shape here is one of the crate's trait types (`DynScalar`, `DynTable`, ...) whose methods read the
//! implementation out of the function's user data and hand it a context struct built from the callback's info. Bind
//! data and states travel as `Box<dyn Any>` in the engine's opaque slots and are downcast on the way back.

use std::any::Any;
use std::sync::Arc;

use crate::cast::{CastExec, CastExecInfo, CastFunction, CastMode};
use crate::chunk::DataChunk;
use crate::context::{Context, Factory};
use crate::extension::Extension;
use crate::function::{ArgCounts, BindInfo, FunctionSignature, ParameterKind};
use crate::pragma::{PragmaFunction, PragmaQuery, PragmaQueryInfo};
use crate::query::Statement;
use crate::scalar::{
    ScalarBind, ScalarBindInfo, ScalarExec, ScalarExecInfo, ScalarFunction, ScalarInit, ScalarInitInfo,
    ScalarResolveTypes, ScalarResolveTypesInfo,
};
use crate::sys::*;
use crate::table::{
    MultiFileCombineSchema, MultiFileCombineSchemaInfo, OperatorResult, TableBind, TableBindInfo, TableBindReplace,
    TableBindReplaceInfo, TableClaimBatch, TableClaimBatchInfo, TableExec, TableExecInfo, TableFunction, TableInOut,
    TableInOutInfo, TableInitGlobal, TableInitGlobalInfo, TableInitLocal, TableInitLocalInfo, TableProgress,
    TableProgressInfo,
};
use crate::types::LogicalType;
use crate::value::Value;
use crate::vector::Vector;
use crate::{Error, Result};

// -------------------------------------------------------------------------------------------------
// Signatures
// -------------------------------------------------------------------------------------------------

/// The kind of a parameter of a dynamically built signature.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParamKind {
    Standard,
    PositionalOnly,
    Variadic,
    NamedOnly,
    NamedVariadic,
}

impl ParamKind {
    pub fn to_c(self) -> ParameterKind {
        match self {
            ParamKind::Standard => DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD,
            ParamKind::PositionalOnly => DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_ONLY,
            ParamKind::Variadic => DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_VARIADIC,
            ParamKind::NamedOnly => DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_ONLY,
            ParamKind::NamedVariadic => DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_VARIADIC,
        }
    }
}

/// A parameter of a signature.
pub struct Param {
    pub name: String,
    pub type_: LogicalType,
    pub kind: ParamKind,
    pub default: Option<Value>,
}

impl Param {
    pub fn new(name: &str, type_: LogicalType) -> Param {
        Param { name: name.to_string(), type_, kind: ParamKind::Standard, default: None }
    }

    pub fn variadic(name: &str, type_: LogicalType) -> Param {
        Param { name: name.to_string(), type_, kind: ParamKind::Variadic, default: None }
    }

    pub fn with_default(name: &str, type_: LogicalType, default: Value) -> Param {
        Param { name: name.to_string(), type_, kind: ParamKind::Standard, default: Some(default) }
    }

    pub fn named(name: &str, type_: LogicalType) -> Param {
        Param { name: name.to_string(), type_, kind: ParamKind::NamedOnly, default: None }
    }

    /// A named-only parameter with a default (what makes it optional for the binder).
    pub fn named_with_default(name: &str, type_: LogicalType, default: Value) -> Param {
        Param { name: name.to_string(), type_, kind: ParamKind::NamedOnly, default: Some(default) }
    }

    pub fn positional_only(name: &str, type_: LogicalType) -> Param {
        Param { name: name.to_string(), type_, kind: ParamKind::PositionalOnly, default: None }
    }
}

/// Adds `params` (and the return type) to a signature.
pub fn fill_signature(
    signature: &FunctionSignature,
    params: &[Param],
    return_type: Option<&LogicalType>,
) -> Result<()> {
    for param in params {
        signature.add_parameter_with(&param.name, &param.type_, param.default.as_ref(), param.kind.to_c())?;
    }
    if let Some(return_type) = return_type {
        signature.set_return_type(return_type)?;
    }
    Ok(())
}

// -------------------------------------------------------------------------------------------------
// Bind-time context
// -------------------------------------------------------------------------------------------------

/// The arguments of a call as the bind-time callbacks see them.
pub struct BindCtx {
    info: BindInfo,
    pub context: Context,
}

impl BindCtx {
    pub fn new(info: BindInfo, context: Context) -> BindCtx {
        BindCtx { info, context }
    }

    pub fn info(&self) -> &BindInfo {
        &self.info
    }

    pub fn arg_counts(&self) -> Result<ArgCounts> {
        self.info.arg_counts()
    }

    /// The total number of arguments (positional and named).
    pub fn arg_count(&self) -> Result<usize> {
        Ok(self.info.arg_counts()?.total())
    }

    pub fn arg_type(&self, index: usize) -> Result<LogicalType> {
        self.info.arg_type(index)
    }

    /// `BindFunctionInput::GetConstant`: the constant value of argument `index` (a binder error when the argument is
    /// not foldable).
    pub fn arg_value(&self, index: usize) -> Result<Value> {
        self.info.arg_value(index)
    }

    /// `BindFunctionInput::TryGetConstant`: `None` for a non-constant argument; the engine's "parameter not
    /// resolved" signal propagates.
    pub fn try_arg_value(&self, index: usize) -> Result<Option<Value>> {
        match self.info.arg_value(index) {
            Ok(value) => Ok(Some(value)),
            Err(e) if e.is_parameter_not_resolved() => Err(e),
            Err(_) => Ok(None),
        }
    }

    /// The name of argument `index` (the parameter name, or the alias of a `**kwargs` argument).
    pub fn arg_name(&self, index: usize) -> Result<String> {
        self.info.arg_name(index)
    }

    pub fn arg_index(&self, name: &str) -> Result<Option<usize>> {
        self.info.arg_index(name)
    }

    /// `arguments[index]->GetAlias()` ([`BindInfo::arg_alias`]); `None` when the loading engine has no alias accessor.
    pub fn arg_alias(&self, index: usize) -> Result<Option<String>> {
        self.info.arg_alias(index)
    }

    pub fn factory(&self) -> Result<Factory> {
        self.context.factory()
    }
}

// -------------------------------------------------------------------------------------------------
// Scalar functions
// -------------------------------------------------------------------------------------------------

/// Bind data of a dynamically dispatched scalar function: computed once per call site, shared by every thread;
/// the engine compares it when it compares expressions.
pub trait BindData: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn equals(&self, other: &dyn BindData) -> bool;
}

/// Per-thread state (`FunctionLocalState`), recreated for every execution; also the global and local states of a
/// dynamically dispatched table function.
pub type LocalState = Box<dyn Any + Send>;

/// The bind data as the engine stores it, comparing through [`BindData::equals`].
struct DynBindData(Box<dyn BindData>);

impl PartialEq for DynBindData {
    fn eq(&self, other: &DynBindData) -> bool {
        self.0.equals(&*other.0)
    }
}

/// The arguments of a scalar execution.
pub struct ExecCtx<'a> {
    info: &'a ScalarExecInfo<'a>,
    pub context: Context,
    /// The number of rows.
    pub count: usize,
}

impl<'a> ExecCtx<'a> {
    pub fn info(&self) -> &ScalarExecInfo<'a> {
        self.info
    }

    pub fn arg(&self, index: usize) -> Result<Vector> {
        self.info.arg(index)
    }

    pub fn arg_count(&self) -> Result<usize> {
        Ok(self.info.arg_counts()?.total())
    }

    pub fn result(&self) -> Result<Vector> {
        self.info.result()
    }

    /// The bind data of the call site, when the function set one of type `T`.
    pub fn bind_data<T: 'static>(&self) -> Option<&T> {
        self.info.bind_data::<DynBindData>().ok().flatten().and_then(|data| data.0.as_any().downcast_ref::<T>())
    }

    /// The thread-local state of the execution, when the function set one of type `T` (an exclusive slot: handed out
    /// once per callback, [`ScalarExecInfo::init_data`]).
    pub fn local<T: 'static>(&self) -> Option<&'a mut T> {
        self.info.init_data::<LocalState>().ok().flatten().and_then(|state| state.downcast_mut::<T>())
    }

    pub fn factory(&self) -> Result<Factory> {
        self.context.factory()
    }
}

/// The implementation of a scalar function (shared by all its overloads if they want).
pub trait ScalarImpl: Send + Sync + 'static {
    /// The return type of this call site, when it depends on the arguments (`ANY` return types).
    fn resolve_types(&self, _ctx: &BindCtx) -> Result<Option<LogicalType>> {
        Ok(None)
    }
    fn bind(&self, _ctx: &BindCtx) -> Result<Option<Box<dyn BindData>>> {
        Ok(None)
    }
    fn init(&self, _bind_data: Option<&dyn BindData>) -> Result<Option<LocalState>> {
        Ok(None)
    }
    fn exec(&self, ctx: &ExecCtx<'_>) -> Result<()>;
}

struct ScalarUserData {
    imp: Arc<dyn ScalarImpl>,
}

/// The callback type of every dynamically dispatched scalar function.
pub struct DynScalar;

fn missing_user_data(what: &str) -> Error {
    Error::internal(format!("dynamic {what}: the function has no implementation in its user data"))
}

impl ScalarResolveTypes for DynScalar {
    fn resolve_types(bind: &BindInfo, info: &ScalarResolveTypesInfo, context: &Context) -> Result<()> {
        let data = bind.user_data::<ScalarUserData>()?.ok_or_else(|| missing_user_data("scalar function"))?;
        if let Some(return_type) = data.imp.resolve_types(&BindCtx::new(*bind, *context))? {
            info.set_return_type(&return_type)?;
        }
        Ok(())
    }
}

impl ScalarBind for DynScalar {
    fn bind(bind: &BindInfo, info: &ScalarBindInfo, context: &Context) -> Result<()> {
        let data = bind.user_data::<ScalarUserData>()?.ok_or_else(|| missing_user_data("scalar function"))?;
        if let Some(bind_data) = data.imp.bind(&BindCtx::new(*bind, *context))? {
            info.set_bind_data(DynBindData(bind_data))?;
        }
        Ok(())
    }
}

impl ScalarInit for DynScalar {
    fn init(info: &ScalarInitInfo, _context: &Context) -> Result<()> {
        let data = info.user_data::<ScalarUserData>()?.ok_or_else(|| missing_user_data("scalar function"))?;
        let bind_data = info.bind_data::<DynBindData>()?.map(|data| &*data.0);
        if let Some(state) = data.imp.init(bind_data)? {
            info.set_init_data(state)?;
        }
        Ok(())
    }
}

impl ScalarExec for DynScalar {
    fn exec(info: &ScalarExecInfo, context: &Context) -> Result<()> {
        let data = info.user_data::<ScalarUserData>()?.ok_or_else(|| missing_user_data("scalar function"))?;
        let ctx = ExecCtx { info, context: *context, count: info.row_count()? };
        data.imp.exec(&ctx)
    }
}

/// One overload of a dynamically dispatched scalar function.
pub struct DynScalarFunction {
    pub name: String,
    pub params: Vec<Param>,
    pub return_type: LogicalType,
    /// `SetFallible()`: the function may raise a runtime error.
    pub fallible: bool,
    /// `FunctionNullHandling::SPECIAL_HANDLING`: the function sees NULL arguments.
    pub special_null_handling: bool,
    /// `SetRequiresExpressionNames()`: the bind reads the argument aliases ([`BindCtx::arg_alias`]).
    pub requires_expression_names: bool,
    pub imp: Arc<dyn ScalarImpl>,
}

impl DynScalarFunction {
    pub fn new(
        name: &str,
        params: Vec<Param>,
        return_type: LogicalType,
        imp: Arc<dyn ScalarImpl>,
    ) -> DynScalarFunction {
        DynScalarFunction {
            name: name.to_string(),
            params,
            return_type,
            fallible: false,
            special_null_handling: false,
            requires_expression_names: false,
            imp,
        }
    }

    pub fn fallible(mut self) -> DynScalarFunction {
        self.fallible = true;
        self
    }

    pub fn special_null_handling(mut self) -> DynScalarFunction {
        self.special_null_handling = true;
        self
    }

    /// `SetRequiresExpressionNames(true)`: the result depends on the argument aliases the bind reads.
    pub fn requires_expression_names(mut self) -> DynScalarFunction {
        self.requires_expression_names = true;
        self
    }

    /// The same overload under another name (an alias; overloads with default values are rebuilt explicitly).
    pub fn renamed(&self, name: &str) -> DynScalarFunction {
        DynScalarFunction {
            name: name.to_string(),
            params: self
                .params
                .iter()
                .map(|p| {
                    debug_assert!(p.default.is_none(), "aliases of overloads with defaults are built explicitly");
                    Param { name: p.name.clone(), type_: p.type_.clone(), kind: p.kind, default: None }
                })
                .collect(),
            return_type: self.return_type.clone(),
            fallible: self.fallible,
            special_null_handling: self.special_null_handling,
            requires_expression_names: self.requires_expression_names,
            imp: self.imp.clone(),
        }
    }

    /// Registers the overload (`loader.RegisterFunction(ScalarFunctionSet)`: a second overload of a registered name
    /// is merged into its set).
    pub fn register(&self, extension: &Extension, factory: Factory) -> Result<()> {
        let function = ScalarFunction::new(factory)?;
        function.set_name(&self.name)?;
        fill_signature(&function.signature()?, &self.params, Some(&self.return_type))?;
        function.set_user_data(ScalarUserData { imp: self.imp.clone() })?;
        function.set_property(
            DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
            if self.fallible {
                DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE
            } else {
                DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_INFALLIBLE
            },
        )?;
        if self.special_null_handling {
            function.set_property(
                DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
                DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL,
            )?;
        }
        if self.requires_expression_names {
            function.set_requires_expression_names(true)?;
        }
        function.set_resolve_types::<DynScalar>()?;
        function.set_bind::<DynScalar>()?;
        function.set_init::<DynScalar>()?;
        function.set_exec::<DynScalar>()?;
        extension.register_scalar_function(&function)
    }
}

// -------------------------------------------------------------------------------------------------
// Table functions
// -------------------------------------------------------------------------------------------------

/// The bind data of a dynamically dispatched table function, as the engine stores it.
struct TableBindData(Box<dyn Any + Send + Sync>);

/// The bind phase of a table function: declares the result columns.
pub struct TableBindCtx {
    pub args: BindCtx,
    info: TableBindInfo,
}

impl TableBindCtx {
    pub fn info(&self) -> &TableBindInfo {
        &self.info
    }

    pub fn add_column(&self, name: &str, type_: &LogicalType) -> Result<()> {
        self.info.add_result_column(name, type_)?;
        Ok(())
    }

    pub fn set_cardinality(&self, cardinality: u64, exact: bool) -> Result<()> {
        self.info.set_cardinality(cardinality as usize, exact)?;
        Ok(())
    }

    /// `TableFunctionFileBindInput::expected_names/types`: the schema this bind is expected to produce, when the
    /// multi-file scan already determined it.
    pub fn expected_columns(&self) -> Result<Option<Vec<(String, LogicalType)>>> {
        self.info.expected_columns()
    }

    /// `TableFunctionFileBindInput::expected_bind_data`: the bind data (of this same function) that determined the
    /// expected schema, if any.
    pub fn expected_bind_data(&self) -> Result<Option<&dyn Any>> {
        Ok(self.info.expected_bind_data::<TableBindData>()?.map(|data| &*data.0 as &dyn Any))
    }
}

/// The "combine schema" phase of a multi-file function: the bind data of the sampled files, and the combined schema
/// and bind data the function may set (`TableFunctionCombineSchemaInput`).
pub struct CombineSchemaCtx {
    info: MultiFileCombineSchemaInfo,
    pub context: Context,
}

impl CombineSchemaCtx {
    pub fn info(&self) -> &MultiFileCombineSchemaInfo {
        &self.info
    }

    /// The bind data of the sampled files, in file order.
    pub fn bind_data(&self) -> Result<Vec<&dyn Any>> {
        let count = self.info.bind_data_count()?;
        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let data = self
                .info
                .bind_data::<TableBindData>(i)?
                .ok_or_else(|| Error::internal("dynamic table function: a sampled file has no bind data"))?;
            result.push(&*data.0 as &dyn Any);
        }
        Ok(result)
    }

    pub fn union_by_name(&self) -> Result<bool> {
        self.info.union_by_name()
    }

    pub fn add_column(&self, name: &str, type_: &LogicalType) -> Result<()> {
        self.info.add_result_column(name, type_)?;
        Ok(())
    }
}

/// The global and local init phases.
pub struct TableInitCtx {
    pub context: Context,
    /// The columns the scan reads (indexes into the declared result columns, or the id of a virtual column), in
    /// output order.
    pub columns: Vec<usize>,
    /// The scan produces rows only (`count(*)`): `columns` holds the first declared column as the carrier of the rows,
    /// whose values need not be written ([`TableInitGlobalInfo::is_row_only`]). Known to the global init (the local
    /// init has the global state); `false` on a host without the entry.
    pub row_only: bool,
    /// The ids the engine gave the function's virtual columns, in declaration order
    /// ([`DynTableFunction::virtual_columns`]).
    pub virtual_column_ids: Vec<u64>,
}

/// The shared state behind an opaque slot holding a [`LocalState`], as `&T` (the global state: shared by every thread
/// of the scan, synchronized by the implementation).
fn state_shared<'a, T: 'static>(state: Option<&'a LocalState>, what: &str) -> Result<&'a T> {
    let state = state.ok_or_else(|| Error::internal(format!("dynamic table function: missing {what} state")))?;
    state
        .downcast_ref::<T>()
        .ok_or_else(|| Error::internal(format!("dynamic table function: {what} state of the wrong type")))
}

/// The exclusive state behind an opaque slot holding a [`LocalState`], as `&mut T` (a thread's local state).
fn state_ref<'a, T: 'static>(state: Option<&'a mut LocalState>, what: &str) -> Result<&'a mut T> {
    let state = state.ok_or_else(|| Error::internal(format!("dynamic table function: missing {what} state")))?;
    state
        .downcast_mut::<T>()
        .ok_or_else(|| Error::internal(format!("dynamic table function: {what} state of the wrong type")))
}

/// The execution phase.
pub struct TableExecCtx<'a> {
    info: &'a TableExecInfo<'a>,
    pub context: Context,
    /// The chunk to fill: write the columns and set its size; size 0 ends this thread's scan.
    pub output: DataChunk,
    pub columns: Vec<usize>,
}

impl<'a> TableExecCtx<'a> {
    pub fn info(&self) -> &TableExecInfo<'a> {
        self.info
    }

    /// The global state of the scan, shared by its threads (`&T`: the implementation synchronizes its own access).
    pub fn global<T: 'static>(&self) -> Result<&T> {
        state_shared(self.info.global_state::<LocalState>()?, "global")
    }

    /// This thread's local state (an exclusive slot: handed out once per callback, `&mut` for the callback's duration).
    pub fn local<T: 'static>(&self) -> Result<&'a mut T> {
        state_ref(self.info.local_state::<LocalState>()?, "local")
    }
}

/// The claim-batch phase: the states of the thread that asks for the next batch of the file.
pub struct TableClaimCtx<'a> {
    info: &'a TableClaimBatchInfo<'a>,
    pub context: Context,
}

impl<'a> TableClaimCtx<'a> {
    pub fn info(&self) -> &TableClaimBatchInfo<'a> {
        self.info
    }

    /// The global state of the scan, shared by its threads (`&T`: the implementation synchronizes its own access).
    pub fn global<T: 'static>(&self) -> Result<&T> {
        state_shared(self.info.global_state::<LocalState>()?, "global")
    }

    /// This thread's local state (an exclusive slot: handed out once per callback, `&mut` for the callback's duration).
    pub fn local<T: 'static>(&self) -> Result<&'a mut T> {
        state_ref(self.info.local_state::<LocalState>()?, "local")
    }
}

/// `OperatorResultType` of a table in-out function.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InOutResult {
    NeedMoreInput,
    HaveMoreOutput,
    Finished,
}

/// The in-out phase of a table in-out function: the input rows (the function's arguments), the output chunk and
/// the states.
/// The context of one in-out call. It carries no per-call copies of the projected columns or the virtual column
/// ids: an in-out function of a LATERAL join is called once per input row, and the implementations fix their column
/// layout in `init_global` (`TableInitCtx::columns` / `virtual_column_ids`); `info().column_index(i)` reads them on
/// demand.
pub struct TableInOutCtx<'a> {
    info: &'a TableInOutInfo<'a>,
    pub context: Context,
    pub input: DataChunk,
    pub output: DataChunk,
}

impl<'a> TableInOutCtx<'a> {
    pub fn info(&self) -> &TableInOutInfo<'a> {
        self.info
    }

    /// The global state of the scan, shared by its threads (`&T`: the implementation synchronizes its own access).
    pub fn global<T: 'static>(&self) -> Result<&T> {
        state_shared(self.info.global_state::<LocalState>()?, "global")
    }

    /// This thread's local state (an exclusive slot: handed out once per callback, `&mut` for the callback's duration).
    pub fn local<T: 'static>(&self) -> Result<&'a mut T> {
        state_ref(self.info.local_state::<LocalState>()?, "local")
    }
}

/// The implementation of a table function. The bind data and the global state are any `Send + Sync` state; the
/// local state any `Send` state.
pub trait TableImpl: Send + Sync + 'static {
    fn bind(&self, ctx: &TableBindCtx) -> Result<Box<dyn Any + Send + Sync>>;
    /// `table_function_combine_schema_t` of the multi-file wrapper around this function: combines the schemas of the
    /// sampled files (their bind data) into the schema of the scan, declaring its columns and returning the bind
    /// data every file is then read with; `None` leaves the files to their own binds.
    fn combine_schema(&self, _ctx: &CombineSchemaCtx) -> Result<Option<Box<dyn Any + Send + Sync>>> {
        Ok(None)
    }
    /// Returns the global state and the maximum number of threads.
    fn init_global(&self, ctx: &TableInitCtx, bind_data: &dyn Any) -> Result<(Box<dyn Any + Send + Sync>, usize)>;
    fn init_local(&self, ctx: &TableInitCtx, bind_data: &dyn Any, global: &dyn Any) -> Result<Box<dyn Any + Send>>;
    fn exec(&self, ctx: &TableExecCtx<'_>, bind_data: &dyn Any) -> Result<()>;
    /// `table_function_claim_batch_t` (`TableFunction::claim_batches`): assigns the next batch of the file to the
    /// thread's local state; false once the file has nothing left. Every `exec` then scans the claimed batch only
    /// and ends it with an empty chunk.
    fn claim_batch(&self, _ctx: &TableClaimCtx<'_>, _bind_data: &dyn Any) -> Result<bool> {
        Ok(false)
    }
    /// `table_in_out_function_t`, for a function registered with [`DynTableFunction::in_out`].
    fn in_out(&self, _ctx: &TableInOutCtx<'_>, _bind_data: &dyn Any) -> Result<InOutResult> {
        Err(Error::internal("dynamic table function: no in-out implementation"))
    }
    /// The scan progress in percent, when known.
    fn progress(&self, _bind_data: &dyn Any, _global: &dyn Any) -> Option<f64> {
        None
    }
}

struct TableUserData {
    imp: Arc<dyn TableImpl>,
    /// The ids of the declared virtual columns, in declaration order (empty without the entry).
    virtual_column_ids: Vec<u64>,
}

/// The callback type of every dynamically dispatched table function; also the `combine_schema` callback of the
/// multi-file wrapper around one (`MultiFileFunction::set_combine_schema::<DynTable>()`).
pub struct DynTable;

fn table_bind_data(data: Option<&TableBindData>) -> Result<&dyn Any> {
    data.map(|data| &*data.0 as &dyn Any)
        .ok_or_else(|| Error::internal("dynamic table function: the call has no bind data"))
}

impl TableBind for DynTable {
    fn bind(bind: &BindInfo, info: &TableBindInfo, context: &Context) -> Result<()> {
        let user_data = bind.user_data::<TableUserData>()?.ok_or_else(|| missing_user_data("table function"))?;
        let ctx = TableBindCtx { args: BindCtx::new(*bind, *context), info: *info };
        let data = user_data.imp.bind(&ctx)?;
        bind.set_unique_bind_data(TableBindData(data))
    }
}

impl MultiFileCombineSchema for DynTable {
    fn combine_schema(info: &MultiFileCombineSchemaInfo, context: &Context) -> Result<()> {
        let user_data = info.user_data::<TableUserData>()?.ok_or_else(|| missing_user_data("table function"))?;
        let ctx = CombineSchemaCtx { info: *info, context: *context };
        if let Some(data) = user_data.imp.combine_schema(&ctx)? {
            info.set_bind_data(TableBindData(data))?;
        }
        Ok(())
    }
}

impl TableInitGlobal for DynTable {
    fn init_global(info: &TableInitGlobalInfo, context: &Context) -> Result<()> {
        let user_data = info.user_data::<TableUserData>()?.ok_or_else(|| missing_user_data("table function"))?;
        let bind_data = table_bind_data(info.bind_data::<TableBindData>()?)?;
        let row_only =
            crate::api::api().duckdb_v2_table_function_init_global_is_row_only.is_some() && info.is_row_only()?;
        let ctx = TableInitCtx {
            context: *context,
            columns: info.column_indexes()?,
            row_only,
            virtual_column_ids: user_data.virtual_column_ids.clone(),
        };
        let (state, max_threads) = user_data.imp.init_global(&ctx, bind_data)?;
        info.set_global_state(state)?;
        info.set_max_threads(max_threads.max(1))
    }
}

impl TableInitLocal for DynTable {
    fn init_local(info: &TableInitLocalInfo, context: &Context) -> Result<()> {
        let user_data = info.user_data::<TableUserData>()?.ok_or_else(|| missing_user_data("table function"))?;
        let bind_data = table_bind_data(info.bind_data::<TableBindData>()?)?;
        let global = info
            .global_state::<LocalState>()?
            .ok_or_else(|| Error::internal("dynamic table function: missing global state"))?;
        let ctx = TableInitCtx {
            context: *context,
            columns: info.column_indexes()?,
            row_only: false,
            virtual_column_ids: user_data.virtual_column_ids.clone(),
        };
        let state = user_data.imp.init_local(&ctx, bind_data, &**global)?;
        info.set_local_state(state)
    }
}

impl TableExec for DynTable {
    fn exec(info: &TableExecInfo, context: &Context) -> Result<()> {
        let user_data = info.user_data::<TableUserData>()?.ok_or_else(|| missing_user_data("table function"))?;
        let bind_data = table_bind_data(info.bind_data::<TableBindData>()?)?;
        let ctx = TableExecCtx { info, context: *context, output: info.output()?, columns: info.column_indexes()? };
        user_data.imp.exec(&ctx, bind_data)
    }
}

impl TableClaimBatch for DynTable {
    fn claim_batch(info: &TableClaimBatchInfo, context: &Context) -> Result<()> {
        let user_data = info.user_data::<TableUserData>()?.ok_or_else(|| missing_user_data("table function"))?;
        let bind_data = table_bind_data(info.bind_data::<TableBindData>()?)?;
        let ctx = TableClaimCtx { info, context: *context };
        let claimed = user_data.imp.claim_batch(&ctx, bind_data)?;
        info.set_claimed(claimed)
    }
}

impl TableInOut for DynTable {
    fn in_out(info: &TableInOutInfo, context: &Context) -> Result<OperatorResult> {
        let user_data = info.user_data::<TableUserData>()?.ok_or_else(|| missing_user_data("table function"))?;
        let bind_data = table_bind_data(info.bind_data::<TableBindData>()?)?;
        let ctx = TableInOutCtx { info, context: *context, input: info.input()?, output: info.output()? };
        Ok(match user_data.imp.in_out(&ctx, bind_data)? {
            InOutResult::NeedMoreInput => DUCKDB_V2_OPERATOR_RESULT_NEED_MORE_INPUT,
            InOutResult::HaveMoreOutput => DUCKDB_V2_OPERATOR_RESULT_HAVE_MORE_OUTPUT,
            InOutResult::Finished => DUCKDB_V2_OPERATOR_RESULT_FINISHED,
        })
    }
}

impl TableProgress for DynTable {
    fn progress(info: &TableProgressInfo, _context: &Context) -> Result<()> {
        let user_data = info.user_data::<TableUserData>()?.ok_or_else(|| missing_user_data("table function"))?;
        let bind_data = table_bind_data(info.bind_data::<TableBindData>()?)?;
        let Some(global) = info.global_state::<LocalState>()? else {
            return Ok(());
        };
        if let Some(progress) = user_data.imp.progress(bind_data, &**global) {
            info.set_progress(progress / 100.0)?;
        }
        Ok(())
    }
}

/// A dynamically dispatched table function definition.
pub struct DynTableFunction {
    pub name: String,
    pub params: Vec<Param>,
    pub projection_pushdown: bool,
    /// A table in-out function (`TableImpl::in_out` instead of `exec`).
    pub in_out: bool,
    /// The function scans its input in batches a caller claims (`TableImpl::claim_batch`): the multi-file wrapper
    /// claims every batch itself and gives it a batch index of its own, so that the rows keep the file order
    /// (`TableFunction::claim_batch`, the C++ `MultiFileReader::TryInitializeScan`).
    pub claim_batches: bool,
    /// The virtual columns the function can produce next to its declared ones (`table_function_get_virtual_columns_t`,
    /// e.g. `json_each`'s `json` / `root` / `rowid`): declared with `duckdb_v2_table_function_add_virtual_column` when
    /// the loading engine has the entry, their ids reach the init and in-out phases as `virtual_column_ids`.
    pub virtual_columns: Vec<(String, LogicalType)>,
    pub imp: Arc<dyn TableImpl>,
}

impl DynTableFunction {
    pub fn new(name: &str, params: Vec<Param>, imp: Arc<dyn TableImpl>) -> DynTableFunction {
        DynTableFunction {
            name: name.to_string(),
            params,
            projection_pushdown: false,
            in_out: false,
            claim_batches: false,
            virtual_columns: Vec::new(),
            imp,
        }
    }

    /// Whether the loading engine supports table in-out functions through the C API (newer engines only).
    pub fn in_out_available() -> bool {
        crate::api::api().duckdb_v2_table_function_set_in_out_callback.is_some()
    }

    /// `loader.RegisterFunction(TableFunction)`.
    pub fn register(&self, extension: &Extension, factory: Factory) -> Result<()> {
        let function = TableFunction::new(factory)?;
        function.set_name(&self.name)?;
        fill_signature(&function.signature()?, &self.params, None)?;
        // the ids the engine assigns the virtual columns reach the callbacks through the user data
        let mut virtual_column_ids = Vec::with_capacity(self.virtual_columns.len());
        if crate::api::api().duckdb_v2_table_function_add_virtual_column.is_some() {
            for (name, type_) in &self.virtual_columns {
                virtual_column_ids.push(function.add_virtual_column(name, type_)?);
            }
        }
        function.set_user_data(TableUserData { imp: self.imp.clone(), virtual_column_ids })?;
        function.set_bind::<DynTable>()?;
        function.set_init_global::<DynTable>()?;
        function.set_init_local::<DynTable>()?;
        if self.in_out {
            function.set_in_out::<DynTable>()?;
        } else {
            function.set_exec::<DynTable>()?;
        }
        if self.claim_batches {
            function.set_claim_batch::<DynTable>()?;
        }
        function.set_progress::<DynTable>()?;
        function.set_projection_pushdown(self.projection_pushdown)?;
        extension.register_table_function(&function)
    }
}

// -------------------------------------------------------------------------------------------------
// Casts
// -------------------------------------------------------------------------------------------------

/// The arguments of a cast execution.
pub struct CastCtx {
    info: CastExecInfo,
    pub context: Context,
    pub input: Vector,
    pub output: Vector,
    pub count: usize,
    /// A `TRY_CAST`: failures become NULLs instead of errors.
    pub try_cast: bool,
}

impl CastCtx {
    pub fn info(&self) -> &CastExecInfo {
        &self.info
    }

    /// Raises the error this cast reports even under a `TRY_CAST`: the exception a C++ cast throws (a MAP key that
    /// cannot be NULL, a malformed document), where a reported error is otherwise the `CastParameters::error_message`
    /// the try cast swallows. A no-op when the loading engine has no such entry (the upstream library).
    pub fn raise_errors(&self) -> Result<()> {
        if crate::api::api().duckdb_v2_cast_function_exec_set_raise_error.is_none() {
            return Ok(());
        }
        self.info.set_raise_error(true)
    }
}

/// The body of a cast: a `TRY_CAST` keeps going after a failed row; a regular cast reports the error through `Err`.
pub type CastFn = fn(&CastCtx) -> Result<()>;

struct CastUserData {
    exec: CastFn,
}

/// The callback type of every dynamically dispatched cast.
pub struct DynCast;

impl CastExec for DynCast {
    fn exec(info: &CastExecInfo, context: &Context) -> Result<()> {
        let user_data = info.user_data::<CastUserData>()?.ok_or_else(|| missing_user_data("cast"))?;
        let ctx = CastCtx {
            info: *info,
            context: *context,
            input: info.input()?,
            output: info.output()?,
            count: info.row_count()?,
            try_cast: info.mode()? == DUCKDB_V2_CAST_MODE_TRY,
        };
        (user_data.exec)(&ctx)
    }
}

/// `loader.RegisterCastFunction(source, target, exec, cost)` with a plain function as the body.
pub fn register_cast(
    extension: &Extension,
    factory: Factory,
    source: &LogicalType,
    target: &LogicalType,
    cost: i64,
    exec: CastFn,
) -> Result<()> {
    let function = CastFunction::new(factory)?;
    function.set_source_type(source)?;
    function.set_target_type(target)?;
    function.set_implicit_cast_cost(cost)?;
    function.set_user_data(CastUserData { exec })?;
    function.set_exec::<DynCast>()?;
    extension.register_cast_function(&function)
}

/// The cast mode of a dynamically dispatched cast (re-exported for the bodies).
pub type DynCastMode = CastMode;

// -------------------------------------------------------------------------------------------------
// Query pragmas and bind-replaced table functions
// -------------------------------------------------------------------------------------------------

/// What a call is answered with: SQL text, or a statement already in hand (bound as it is by a table function,
/// rendered to SQL for a pragma).
pub enum Replacement {
    Sql(String),
    Statement(Statement),
}

/// The body of a query pragma or a replaced table function: the replacement for the call's arguments.
pub type QueryFn = fn(&BindCtx) -> Result<Replacement>;

struct QueryUserData {
    query: QueryFn,
}

/// The callback type of every dynamically dispatched query pragma / bind-replaced table function.
pub struct DynQuery;

fn query_of(bind: &BindInfo) -> Result<QueryFn> {
    Ok(bind.user_data::<QueryUserData>()?.ok_or_else(|| missing_user_data("query function"))?.query)
}

impl PragmaQuery for DynQuery {
    fn query(bind: &BindInfo, info: &PragmaQueryInfo, context: &Context) -> Result<()> {
        let query = query_of(bind)?;
        let sql = match query(&BindCtx::new(*bind, *context))? {
            Replacement::Sql(sql) => sql,
            Replacement::Statement(statement) => statement.to_sql()?,
        };
        info.set_query(&sql)
    }
}

impl TableBindReplace for DynQuery {
    fn bind_replace(bind: &BindInfo, info: &TableBindReplaceInfo, context: &Context) -> Result<()> {
        let query = query_of(bind)?;
        let replacement = query(&BindCtx::new(*bind, *context))?;
        if let Replacement::Statement(statement) = &replacement
            && TableBindReplaceInfo::set_statement_available()
        {
            // the statement itself: its expression locations and bound types stay what they were
            return info.set_statement(statement);
        }
        let sql = match replacement {
            Replacement::Sql(sql) => sql,
            Replacement::Statement(statement) => statement.to_sql()?,
        };
        info.set_query(&sql)
    }
}

/// `PragmaFunction::PragmaCall(name, function, signature)`: a pragma whose call runs the SQL `query` returns.
pub fn register_pragma_query(
    extension: &Extension,
    factory: Factory,
    name: &str,
    params: &[Param],
    query: QueryFn,
) -> Result<()> {
    let function = PragmaFunction::new(factory)?;
    function.set_name(name)?;
    fill_signature(&function.signature()?, params, None)?;
    function.set_user_data(QueryUserData { query })?;
    function.set_query::<DynQuery>()?;
    extension.register_pragma_function(&function)
}

/// A table function whose every call is bound as the subquery `query` returns (`TableFunction::bind_replace`).
/// newer engines only (see [`TableFunction::bind_replace_available`]).
pub fn register_table_replace(
    extension: &Extension,
    factory: Factory,
    name: &str,
    params: &[Param],
    query: QueryFn,
) -> Result<()> {
    if !TableFunction::bind_replace_available() {
        return Err(Error::not_implemented("bind replace is not available"));
    }
    let function = TableFunction::new(factory)?;
    function.set_name(name)?;
    fill_signature(&function.signature()?, params, None)?;
    function.set_user_data(QueryUserData { query })?;
    function.set_bind_replace::<DynQuery>()?;
    extension.register_table_function(&function)
}
