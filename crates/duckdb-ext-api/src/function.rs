//! What every function family shares: the signature, the documentation and the bind-time call site.

use crate::Result;
use crate::opaque::Opaque;
use crate::sys::*;
use crate::text::{str_arg, str_to_string};
use crate::types::LogicalType;
use crate::value::Value;

/// `DUCKDB_V2_FUNCTION_PARAMETER_KIND` (`POSITIONAL_ONLY`, `STANDARD`, `POSITIONAL_VARIADIC` (`*args`),
/// `NAMED_ONLY`, `NAMED_VARIADIC` (`**kwargs`)).
pub type ParameterKind = DUCKDB_V2_FUNCTION_PARAMETER_KIND;
/// `DUCKDB_V2_FUNCTION_PROPERTY_KEY`.
pub type PropertyKey = DUCKDB_V2_FUNCTION_PROPERTY_KEY;
/// `DUCKDB_V2_FUNCTION_PROPERTY_VALUE`.
pub type PropertyValue = DUCKDB_V2_FUNCTION_PROPERTY_VALUE;

/// A function's signature (borrowed from the function builder).
#[derive(Clone, Copy)]
pub struct FunctionSignature {
    raw: duckdb_v2_function_signature_handle,
}

impl FunctionSignature {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_function_signature_handle) -> FunctionSignature {
        FunctionSignature { raw }
    }

    /// Adds a parameter: `default` makes it optional; `kind` is how it may be passed.
    pub fn add_parameter_with(
        &self,
        name: &str,
        type_: &LogicalType,
        default: Option<&Value>,
        kind: ParameterKind,
    ) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_function_signature_add_parameter(
            self.raw,
            &name,
            type_.as_raw(),
            default.map(|v| v.as_raw()).unwrap_or(std::ptr::null_mut()),
            kind
        ));
        Ok(self)
    }

    /// A standard (positional or named) parameter.
    pub fn add_parameter(&self, name: &str, type_: &LogicalType) -> Result<&Self> {
        self.add_parameter_with(name, type_, None, DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD)
    }

    /// A standard parameter with a default value.
    pub fn add_optional_parameter(&self, name: &str, type_: &LogicalType, default: &Value) -> Result<&Self> {
        self.add_parameter_with(name, type_, Some(default), DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD)
    }

    /// A named-only parameter (`name := value`).
    pub fn add_named_parameter(&self, name: &str, type_: &LogicalType, default: Option<&Value>) -> Result<&Self> {
        self.add_parameter_with(name, type_, default, DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_ONLY)
    }

    /// `*args` of the given type.
    pub fn add_varargs(&self, name: &str, type_: &LogicalType) -> Result<&Self> {
        self.add_parameter_with(name, type_, None, DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_VARIADIC)
    }

    /// `**kwargs` of the given type.
    pub fn add_kwargs(&self, name: &str, type_: &LogicalType) -> Result<&Self> {
        self.add_parameter_with(name, type_, None, DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_VARIADIC)
    }

    pub fn set_return_type(&self, type_: &LogicalType) -> Result<&Self> {
        api_call!(duckdb_v2_function_signature_set_return_type(self.raw, type_.as_raw()));
        Ok(self)
    }
}

/// A function's documentation (borrowed from the function builder).
#[derive(Clone, Copy)]
pub struct FunctionDocs {
    raw: duckdb_v2_function_docs_handle,
}

impl FunctionDocs {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_function_docs_handle) -> FunctionDocs {
        FunctionDocs { raw }
    }

    pub fn set_description(&self, description: &str) -> Result<&Self> {
        let description = str_arg(description);
        api_call!(duckdb_v2_function_docs_set_description(self.raw, &description));
        Ok(self)
    }

    pub fn add_example(&self, example: &str, result: &str) -> Result<&Self> {
        let example = str_arg(example);
        let result = str_arg(result);
        api_call!(duckdb_v2_function_docs_add_example(self.raw, &example, &result));
        Ok(self)
    }

    pub fn add_category(&self, category: &str) -> Result<&Self> {
        let category = str_arg(category);
        api_call!(duckdb_v2_function_docs_add_category(self.raw, &category));
        Ok(self)
    }
}

/// The four parts of an argument list (see `duckdb_v2_function_bind_get_arg_count`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArgCounts {
    /// Positional-only and standard parameters.
    pub positional_fixed: usize,
    /// The arguments `*args` received.
    pub positional_variadic: usize,
    /// Named-only parameters.
    pub named_fixed: usize,
    /// The arguments `**kwargs` received.
    pub named_variadic: usize,
}

impl ArgCounts {
    pub fn total(&self) -> usize {
        self.positional_fixed + self.positional_variadic + self.named_fixed + self.named_variadic
    }
}

/// The call site a bind callback binds (borrowed): the arguments, the user data and the bind data slot.
#[derive(Clone, Copy)]
pub struct BindInfo {
    raw: duckdb_v2_function_bind_info_handle,
}

impl BindInfo {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_function_bind_info_handle) -> BindInfo {
        BindInfo { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_function_bind_info_handle {
        self.raw
    }

    pub fn arg_counts(&self) -> Result<ArgCounts> {
        let (mut a, mut b, mut c, mut d): (idx_t, idx_t, idx_t, idx_t) = (0, 0, 0, 0);
        api_call!(duckdb_v2_function_bind_get_arg_count(self.raw, &mut a, &mut b, &mut c, &mut d));
        Ok(ArgCounts {
            positional_fixed: a as usize,
            positional_variadic: b as usize,
            named_fixed: c as usize,
            named_variadic: d as usize,
        })
    }

    pub fn arg_type(&self, index: usize) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_function_bind_get_arg_type(self.raw, index as idx_t) -> LogicalType::from_raw))
    }

    /// The constant value of argument `index` (an error when it is not a constant).
    pub fn arg_value(&self, index: usize) -> Result<Value> {
        Ok(api_call_out!(duckdb_v2_function_bind_get_arg_value(self.raw, index as idx_t) -> Value::from_raw))
    }

    pub fn arg_name(&self, index: usize) -> Result<String> {
        let mut name = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_function_bind_get_arg_name(self.raw, index as idx_t, &mut name));
        Ok(str_to_string(name))
    }

    /// `Expression::GetAlias()` of the bound argument `index` (`duckdb_v2_function_bind_get_arg_alias`): the `:=`
    /// name, the parameter name of a default, the column name of a bare column reference, `""` for a positional
    /// expression without a name. newer engines only: `None` when the loading engine does not offer the entry.
    pub fn arg_alias(&self, index: usize) -> Result<Option<String>> {
        if crate::api::api().duckdb_v2_function_bind_get_arg_alias.is_none() {
            return Ok(None);
        }
        let mut alias = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_function_bind_get_arg_alias(self.raw, index as idx_t, &mut alias));
        Ok(Some(str_to_string(alias)))
    }

    /// The index of the argument named `name`, if the call has one.
    pub fn arg_index(&self, name: &str) -> Result<Option<usize>> {
        let name = str_arg(name);
        let mut index: idx_t = 0;
        let mut found = false;
        api_call!(duckdb_v2_function_bind_get_arg_index(self.raw, &name, &mut index, &mut found));
        Ok(if found { Some(index as usize) } else { None })
    }

    opaque_getter!(/// The user data set on the function builder, as `T`.
        user_data, duckdb_v2_function_bind_get_user_data);

    /// Stores bind data (compared by `PartialEq` when two call sites are compared). Scalar functions set
    /// theirs through [`crate::scalar::ScalarBindInfo::set_bind_data`] instead.
    pub fn set_bind_data<T: PartialEq + 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::with_equality(data);
        api_call!(duckdb_v2_function_bind_set_bind_data(self.raw, &mut opaque));
        Ok(())
    }

    /// Stores bind data that compares by identity (two call sites are equal only when they share the data): for
    /// functions whose bind data has no meaningful equality (table functions).
    pub fn set_unique_bind_data<T: 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_function_bind_set_bind_data(self.raw, &mut opaque));
        Ok(())
    }
}
