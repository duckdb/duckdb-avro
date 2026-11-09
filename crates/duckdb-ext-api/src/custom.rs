//! Custom types (an alias over a base type) and custom options (settings an extension adds).

use crate::Result;
use crate::context::{Context, Factory, SettingScope};
use crate::opaque::Opaque;
use crate::sys::*;
use crate::text::str_arg;
use crate::types::LogicalType;
use crate::value::Value;

/// A custom type under construction (owned until registered).
pub struct CustomType {
    raw: duckdb_v2_custom_type_handle,
}

impl CustomType {
    pub fn new(factory: Factory) -> Result<CustomType> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_custom_type_create(factory.as_raw(), &mut raw));
        Ok(CustomType { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_custom_type_handle {
        self.raw
    }

    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_custom_type_set_name(self.raw, &name));
        Ok(self)
    }

    /// The type the custom type is stored as.
    pub fn set_base_type(&self, type_: &LogicalType) -> Result<&Self> {
        api_call!(duckdb_v2_custom_type_set_base_type(self.raw, type_.as_raw()));
        Ok(self)
    }
}

impl Drop for CustomType {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_custom_type_destroy(&mut self.raw));
    }
}

/// Validates / transforms a new value of a custom option.
pub trait CustomOptionUpdate {
    fn update(info: &CustomOptionUpdateInfo, context: &Context) -> Result<()>;
}

trampoline!(
    update_trampoline,
    CustomOptionUpdate::update,
    CustomOptionUpdateInfo,
    duckdb_v2_custom_option_update_info_handle
);

/// A custom option under construction (owned until registered).
pub struct CustomOption {
    raw: duckdb_v2_custom_option_handle,
}

impl CustomOption {
    pub fn new(factory: Factory) -> Result<CustomOption> {
        let mut raw: duckdb_v2_custom_option_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_custom_option_create(factory.as_raw(), &mut raw));
        Ok(CustomOption { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_custom_option_handle {
        self.raw
    }

    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_custom_option_set_name(self.raw, &name));
        Ok(self)
    }

    pub fn set_type(&self, type_: &LogicalType) -> Result<&Self> {
        api_call!(duckdb_v2_custom_option_set_type(self.raw, type_.as_raw()));
        Ok(self)
    }

    pub fn set_default_value(&self, value: &Value) -> Result<&Self> {
        api_call!(duckdb_v2_custom_option_set_default_value(self.raw, value.as_raw()));
        Ok(self)
    }

    pub fn set_description(&self, description: &str) -> Result<&Self> {
        let description = str_arg(description);
        api_call!(duckdb_v2_custom_option_set_description(self.raw, &description));
        Ok(self)
    }

    pub fn set_default_scope(&self, scope: SettingScope) -> Result<&Self> {
        api_call!(duckdb_v2_custom_option_set_default_scope(self.raw, scope));
        Ok(self)
    }

    pub fn set_supports_scope(&self, scope: SettingScope, supported: bool) -> Result<&Self> {
        api_call!(duckdb_v2_custom_option_set_supports_scope(self.raw, scope, supported));
        Ok(self)
    }

    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_custom_option_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    pub fn set_update<F: CustomOptionUpdate>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_custom_option_set_update_callback(self.raw, Some(update_trampoline::<F>)));
        Ok(self)
    }
}

impl Drop for CustomOption {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_custom_option_destroy(&mut self.raw));
    }
}

/// The update callback's info.
#[derive(Clone, Copy)]
pub struct CustomOptionUpdateInfo {
    raw: duckdb_v2_custom_option_update_info_handle,
}

impl CustomOptionUpdateInfo {
    opaque_getter!(user_data, duckdb_v2_custom_option_update_get_user_data);

    /// The value being set.
    pub fn value(&self) -> Result<Value> {
        Ok(api_call_out!(duckdb_v2_custom_option_update_get_value(self.raw) -> Value::from_raw))
    }

    /// Replaces the value being set (normalization).
    pub fn set_value(&self, value: &Value) -> Result<()> {
        api_call!(duckdb_v2_custom_option_update_set_value(self.raw, value.as_raw()));
        Ok(())
    }

    pub fn scope(&self) -> Result<SettingScope> {
        let mut scope = DUCKDB_V2_SETTING_SCOPE_DEFAULT;
        api_call!(duckdb_v2_custom_option_update_get_scope(self.raw, &mut scope));
        Ok(scope)
    }

    /// Whether this is a `RESET` rather than a `SET`.
    pub fn is_reset(&self) -> Result<bool> {
        let mut reset = false;
        api_call!(duckdb_v2_custom_option_update_is_reset(self.raw, &mut reset));
        Ok(reset)
    }
}
