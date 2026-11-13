//! Secrets (`duckdb_v2_secret_*`, `duckdb_v2_secret_function_*`, `duckdb_v2_create_secret_info_*`, the lookups
//! through an opener or a context), extension log types (`duckdb_v2_extension_register_log_type`), connection-local
//! registered states (`duckdb_v2_context_get_or_create_registered_state`) and `duckdb_v2_parse_memory_limit`. See
//! the engine's C API additions.
//!
//! newer engines only: every entry here is an addition of this port, so a library without it answers
//! `Error::missing_entry` (the httpfs extension refuses to load its remote surface on such an engine).

use std::ffi::c_void;

use crate::Result;
use crate::context::{Context, Factory, LogLevel};
use crate::custom_file_system::FileOpener;
use crate::error::{Error, guard};
use crate::extension::Extension;
use crate::opaque::Opaque;
use crate::sys::*;
use crate::text::{str_arg, str_to_string};
use crate::types::LogicalType;
use crate::value::Value;

/// `DUCKDB_V2_SECRET_PERSIST_TYPE` (`DEFAULT` / `TEMPORARY` / `PERSISTENT` / `TRANSACTION`).
pub type SecretPersistType = DUCKDB_V2_SECRET_PERSIST_TYPE;

/// The text sink of the one-call getters: appends the text to the `String` behind `data`.
unsafe extern "C" fn collect_text(
    text: *const duckdb_v2_str,
    data: *mut c_void,
    _err: *mut duckdb_v2_error_info_handle,
) {
    if text.is_null() || data.is_null() {
        return;
    }
    // SAFETY(ffi): T4 - `data` is the `String` the one-call getter passed as the sink's user data, reached through no
    // other reference during the call.
    let out = unsafe { &mut *(data as *mut String) };
    // SAFETY(ffi): T3 - `text` is the engine's `duckdb_v2_str` view, valid for the sink call.
    out.push_str(&str_to_string(unsafe { *text }));
}

/// Collects the text a one-call sink entry point hands out.
fn collect(call: impl FnOnce(duckdb_v2_text_sink_fn, *mut c_void) -> Result<()>) -> Result<String> {
    let mut out = String::new();
    call(Some(collect_text), &mut out as *mut String as *mut c_void)?;
    Ok(out)
}

// -------------------------------------------------------------------------------------------------
// Secrets
// -------------------------------------------------------------------------------------------------

/// A key-value secret (owned): built by a create-secret callback, or returned by a lookup.
pub struct Secret {
    raw: duckdb_v2_secret_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_secret_handle` (`duckdb_v2.h`: a `KeyValueSecret` "built by
// an extension ... or returned by a lookup"), a plain value without thread affinity that moves between threads as a
// whole; the wrapper holds nothing else and destroys the handle once, through `&mut self`.
unsafe impl Send for Secret {}

impl Secret {
    /// `KeyValueSecret(type, provider, name)` without scope prefixes.
    pub fn new(secret_type: &str, provider: &str, name: &str) -> Result<Secret> {
        let (secret_type, provider, name) = (str_arg(secret_type), str_arg(provider), str_arg(name));
        let mut raw: duckdb_v2_secret_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_secret_create(&secret_type, &provider, &name, &mut raw));
        Ok(Secret { raw })
    }

    /// # Safety
    /// (ffi) `raw` must be a secret handle the C API returned and that is still live; the owned wrapper destroys it on
    /// drop, so nothing else may destroy it.
    pub unsafe fn from_raw(raw: duckdb_v2_secret_handle) -> Secret {
        Secret { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_secret_handle {
        self.raw
    }

    /// Adds a scope prefix (`s3://`, `hf://`, ...): the longest matching prefix wins a lookup.
    pub fn add_scope(&self, path: &str) -> Result<&Self> {
        let path = str_arg(path);
        api_call!(duckdb_v2_secret_add_scope(self.raw, &path));
        Ok(self)
    }

    /// `secret_map[key] = value` (case-insensitive keys).
    pub fn set_value(&self, key: &str, value: &Value) -> Result<&Self> {
        let key = str_arg(key);
        api_call!(duckdb_v2_secret_set_value(self.raw, &key, value.as_raw()));
        Ok(self)
    }

    /// `duckdb_secrets()` shows the key's value as `redacted`.
    pub fn set_redact_key(&self, key: &str) -> Result<&Self> {
        let key = str_arg(key);
        api_call!(duckdb_v2_secret_set_redact_key(self.raw, &key));
        Ok(self)
    }

    pub fn secret_type(&self) -> Result<String> {
        collect(|sink, data| {
            api_call!(duckdb_v2_secret_get_type(self.raw, sink, data));
            Ok(())
        })
    }

    pub fn provider(&self) -> Result<String> {
        collect(|sink, data| {
            api_call!(duckdb_v2_secret_get_provider(self.raw, sink, data));
            Ok(())
        })
    }

    pub fn name(&self) -> Result<String> {
        collect(|sink, data| {
            api_call!(duckdb_v2_secret_get_name(self.raw, sink, data));
            Ok(())
        })
    }

    /// The scope prefixes.
    pub fn scopes(&self) -> Result<Vec<String>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_secret_get_scope_count(self.raw, &mut count));
        (0..count)
            .map(|index| {
                collect(|sink, data| {
                    api_call!(duckdb_v2_secret_get_scope(self.raw, index, sink, data));
                    Ok(())
                })
            })
            .collect()
    }

    /// `KeyValueSecret::TryGetValue(key)`: `None` when the secret has no such key.
    pub fn value(&self, key: &str) -> Result<Option<Value>> {
        let key = str_arg(key);
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_secret_get_value(self.raw, &key, &mut raw));
        // SAFETY(ffi): the entry returns a copy of the value owned by the caller (null without the key); the owned
        // wrapper destroys it.
        Ok(if raw.is_null() { None } else { Some(unsafe { Value::from_raw(raw) }) })
    }

    /// The value of `key` as text, `None` when absent or NULL.
    pub fn value_str(&self, key: &str) -> Result<Option<String>> {
        match self.value(key)? {
            Some(value) if !value.is_null()? => Ok(Some(value.as_str()?)),
            _ => Ok(None),
        }
    }

    /// The keys, in the secret's (case-insensitively sorted) order.
    pub fn keys(&self) -> Result<Vec<String>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_secret_get_key_count(self.raw, &mut count));
        (0..count)
            .map(|index| {
                collect(|sink, data| {
                    api_call!(duckdb_v2_secret_get_key(self.raw, index, sink, data));
                    Ok(())
                })
            })
            .collect()
    }

    /// `(storage_mode, persist_type)` of a secret returned by a lookup (an empty storage for one that is not stored).
    pub fn storage(&self) -> Result<(String, SecretPersistType)> {
        let mut persist_type = DUCKDB_V2_SECRET_PERSIST_TYPE_DEFAULT;
        let storage = collect(|sink, data| {
            api_call!(duckdb_v2_secret_get_storage(self.raw, sink, data, &mut persist_type));
            Ok(())
        })?;
        Ok((storage, persist_type))
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_secret_destroy(&mut self.raw));
    }
}

// -------------------------------------------------------------------------------------------------
// Create-secret input
// -------------------------------------------------------------------------------------------------

/// The input of a create-secret function (`CreateSecretInput`), borrowed for the duration of the callback.
#[derive(Clone, Copy)]
pub struct CreateSecretInfo {
    raw: duckdb_v2_create_secret_info_handle,
}

impl CreateSecretInfo {
    /// # Safety
    /// (ffi) `raw` must be the live info handle the engine lent to the create-secret callback; the borrowed (`Copy`)
    /// wrapper must not be used past the callback.
    pub unsafe fn from_raw(raw: duckdb_v2_create_secret_info_handle) -> CreateSecretInfo {
        CreateSecretInfo { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_create_secret_info_handle {
        self.raw
    }

    opaque_getter!(
        /// The user data of the secret function (set with [`SecretFunction::set_user_data`]).
        user_data,
        duckdb_v2_create_secret_info_get_user_data
    );

    pub fn secret_type(&self) -> Result<String> {
        collect(|sink, data| {
            api_call!(duckdb_v2_create_secret_info_get_type(self.raw, sink, data));
            Ok(())
        })
    }

    pub fn provider(&self) -> Result<String> {
        collect(|sink, data| {
            api_call!(duckdb_v2_create_secret_info_get_provider(self.raw, sink, data));
            Ok(())
        })
    }

    pub fn name(&self) -> Result<String> {
        collect(|sink, data| {
            api_call!(duckdb_v2_create_secret_info_get_name(self.raw, sink, data));
            Ok(())
        })
    }

    /// The scope prefixes of the statement (empty when it gave none).
    pub fn scopes(&self) -> Result<Vec<String>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_create_secret_info_get_scope_count(self.raw, &mut count));
        (0..count)
            .map(|index| {
                collect(|sink, data| {
                    api_call!(duckdb_v2_create_secret_info_get_scope(self.raw, index, sink, data));
                    Ok(())
                })
            })
            .collect()
    }

    /// The named options of the statement, as spelled there.
    pub fn options(&self) -> Result<Vec<(String, Value)>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_create_secret_info_get_option_count(self.raw, &mut count));
        let mut result = Vec::with_capacity(count as usize);
        for index in 0..count {
            let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
            let name = collect(|sink, data| {
                api_call!(duckdb_v2_create_secret_info_get_option(self.raw, index, sink, data, &mut raw));
                Ok(())
            })?;
            // SAFETY(ffi): the entry returns a copy of the option's value owned by the caller; the owned wrapper
            // destroys it.
            result.push((name, unsafe { Value::from_raw(raw) }));
        }
        Ok(result)
    }

    /// The option named `name` (case-insensitively), if the statement gave it.
    pub fn option(&self, name: &str) -> Result<Option<Value>> {
        Ok(self.options()?.into_iter().find(|(key, _)| key.eq_ignore_ascii_case(name)).map(|(_, value)| value))
    }

    /// Hands the created secret to the engine (what the `create` trampoline does with the trait's return value).
    pub fn set_result(&self, secret: &Secret) -> Result<()> {
        api_call!(duckdb_v2_create_secret_info_set_result(self.raw, secret.as_raw()));
        Ok(())
    }
}

/// The input of a programmatic `CREATE SECRET` ([`Context::create_secret`]); replaces a secret of the same name.
pub struct OwnedCreateSecretInfo {
    raw: duckdb_v2_create_secret_info_handle,
}

impl OwnedCreateSecretInfo {
    pub fn new(secret_type: &str, provider: &str, name: &str) -> Result<OwnedCreateSecretInfo> {
        let (secret_type, provider, name) = (str_arg(secret_type), str_arg(provider), str_arg(name));
        let mut raw: duckdb_v2_create_secret_info_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_create_secret_info_create(&secret_type, &provider, &name, &mut raw));
        Ok(OwnedCreateSecretInfo { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_create_secret_info_handle {
        self.raw
    }

    /// The borrowed view (the getters).
    pub fn info(&self) -> CreateSecretInfo {
        CreateSecretInfo { raw: self.raw }
    }

    /// Where and how long the secret is stored (an empty storage is the default).
    pub fn set_storage(&self, storage_type: &str, persist_type: SecretPersistType) -> Result<&Self> {
        let storage_type = str_arg(storage_type);
        api_call!(duckdb_v2_create_secret_info_set_storage(self.raw, &storage_type, persist_type));
        Ok(self)
    }

    pub fn add_scope(&self, path: &str) -> Result<&Self> {
        let path = str_arg(path);
        api_call!(duckdb_v2_create_secret_info_add_scope(self.raw, &path));
        Ok(self)
    }

    pub fn set_option(&self, name: &str, value: &Value) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_create_secret_info_set_option(self.raw, &name, value.as_raw()));
        Ok(self)
    }
}

impl Drop for OwnedCreateSecretInfo {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_create_secret_info_destroy(&mut self.raw));
    }
}

// -------------------------------------------------------------------------------------------------
// Secret functions
// -------------------------------------------------------------------------------------------------

/// The body of a create-secret function: builds the secret from the statement's input.
pub trait CreateSecret {
    fn create(info: &CreateSecretInfo, context: &Context) -> Result<Secret>;
}

unsafe extern "C" fn create_trampoline<F: CreateSecret>(
    info: duckdb_v2_create_secret_info_handle,
    context: duckdb_v2_context_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - the engine lends the info and the context for the duration of the callback (L8.I1); the
        // borrowed wrappers never outlive it.
        let (info, context) = unsafe { (CreateSecretInfo::from_raw(info), Context::from_raw(context)) };
        let secret = F::create(&info, &context)?;
        info.set_result(&secret)
    });
}

/// A create-secret function under construction (owned until registered): the provider of a secret type.
pub struct SecretFunction {
    raw: duckdb_v2_secret_function_handle,
}

impl SecretFunction {
    pub fn new(factory: Factory, secret_type: &str, provider: &str) -> Result<SecretFunction> {
        let (secret_type, provider) = (str_arg(secret_type), str_arg(provider));
        let mut raw: duckdb_v2_secret_function_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_secret_function_create(factory.as_raw(), &secret_type, &provider, &mut raw));
        Ok(SecretFunction { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_secret_function_handle {
        self.raw
    }

    /// Data the create callback reads back with [`CreateSecretInfo::user_data`].
    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_secret_function_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    /// An option `CREATE SECRET` accepts for this provider; an undeclared option is a binder error.
    pub fn add_named_parameter(&self, name: &str, type_: &LogicalType) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_secret_function_add_named_parameter(self.raw, &name, type_.as_raw()));
        Ok(self)
    }

    pub fn set_create<F: CreateSecret>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_secret_function_set_create_callback(self.raw, Some(create_trampoline::<F>)));
        Ok(self)
    }
}

impl Drop for SecretFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_secret_function_destroy(&mut self.raw));
    }
}

impl Extension {
    /// `ExtensionLoader::RegisterSecretType`: a key-value secret type with its default provider.
    pub fn register_secret_type(&self, name: &str, default_provider: &str) -> Result<()> {
        let (name, default_provider) = (str_arg(name), str_arg(default_provider));
        api_call!(duckdb_v2_extension_register_secret_type(self.as_raw(), &name, &default_provider));
        Ok(())
    }

    /// `ExtensionLoader::RegisterFunction(CreateSecretFunction)`: the type must be registered first.
    pub fn register_secret_function(&self, function: &SecretFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_secret_function(self.as_raw(), function.as_raw()));
        Ok(())
    }

    /// `LogManager::RegisterLogType(LogType(name, level))`: an unstructured log type `enable_logging(name)` selects.
    pub fn register_log_type(&self, name: &str, level: LogLevel) -> Result<()> {
        let name = str_arg(name);
        api_call!(duckdb_v2_extension_register_log_type(self.as_raw(), &name, level));
        Ok(())
    }
}

// -------------------------------------------------------------------------------------------------
// Lookups, programmatic creation, registered states, memory limits
// -------------------------------------------------------------------------------------------------

impl FileOpener {
    /// `SecretManager::LookupSecret` in the opener's transaction: the secret of the type whose scope matches `path`
    /// best, or `None`.
    pub fn lookup_secret(&self, path: &str, secret_type: &str) -> Result<Option<Secret>> {
        let (path, secret_type) = (str_arg(path), str_arg(secret_type));
        let mut raw: duckdb_v2_secret_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_opener_lookup_secret(self.as_raw(), &path, &secret_type, &mut raw));
        Ok(if raw.is_null() { None } else { Some(Secret { raw }) })
    }
}

/// A connection-local state (`ClientContextState`) kept under a key by [`Context::registered_state`]; created once
/// per connection and destroyed with it.
pub trait RegisteredState: 'static {
    fn create() -> Self;

    /// `ClientContextState::QueryEnd`.
    fn query_end(&self) {}

    /// `ClientContextState::WriteProfilingInformation`: text appended to the `EXPLAIN ANALYZE` output.
    fn write_profiling_information(&self, _out: &mut String) {}
}

unsafe extern "C" fn state_create<T: RegisteredState>(_data: *mut c_void) -> *mut c_void {
    Box::into_raw(Box::new(T::create())) as *mut c_void
}

unsafe extern "C" fn state_destroy<T: RegisteredState>(state: *mut c_void) {
    if !state.is_null() {
        // SAFETY(ffi): the pointer came from `state_create::<T>`.
        drop(unsafe { Box::from_raw(state as *mut T) });
    }
}

unsafe extern "C" fn state_query_end<T: RegisteredState>(state: *mut c_void) {
    // SAFETY(ffi): T4 - `state` is the `T` of `state_create::<T>` registered with these callbacks, alive until
    // `state_destroy`.
    if let Some(state) = unsafe { Opaque::get::<T>(state) } {
        state.query_end();
    }
}

unsafe extern "C" fn state_profiling<T: RegisteredState>(
    state: *mut c_void,
    sink: duckdb_v2_text_sink_fn,
    sink_data: *mut c_void,
) {
    // SAFETY(ffi): T4 - `state` is the `T` of `state_create::<T>` registered with these callbacks, alive until
    // `state_destroy`.
    let (Some(state), Some(sink)) = (unsafe { Opaque::get::<T>(state) }, sink) else {
        return;
    };
    let mut out = String::new();
    state.write_profiling_information(&mut out);
    if !out.is_empty() {
        let view = str_arg(&out);
        let mut err: duckdb_v2_error_info_handle = std::ptr::null_mut();
        // SAFETY(ffi): the sink contract; the view is live for the call.
        unsafe { sink(&view, sink_data, &mut err) };
    }
}

impl Context {
    /// `SecretManager::LookupSecret` in the context's transaction.
    pub fn lookup_secret(&self, path: &str, secret_type: &str) -> Result<Option<Secret>> {
        let (path, secret_type) = (str_arg(path), str_arg(secret_type));
        let mut raw: duckdb_v2_secret_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_context_lookup_secret(self.as_raw(), &path, &secret_type, &mut raw));
        Ok(if raw.is_null() { None } else { Some(Secret { raw }) })
    }

    /// `SecretManager::CreateSecret(context, input)`: runs the create-secret function of the input's type and
    /// provider and stores the result.
    pub fn create_secret(&self, info: &OwnedCreateSecretInfo) -> Result<Secret> {
        let mut raw: duckdb_v2_secret_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_context_create_secret(self.as_raw(), info.as_raw(), &mut raw));
        if raw.is_null() {
            return Err(Error::internal("duckdb_v2_context_create_secret returned no secret"));
        }
        Ok(Secret { raw })
    }

    /// `ClientContext::registered_state->GetOrCreate<T>(key)`: the connection's state under `key`, created with
    /// `T::create()` the first time. The reference is valid as long as the connection (the state is destroyed when
    /// the connection closes), so callers must not keep it past the connection's life.
    pub fn registered_state<T: RegisteredState>(&self, key: &str) -> Result<&'static T> {
        let key = str_arg(key);
        let mut state: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_context_get_or_create_registered_state(
            self.as_raw(),
            &key,
            Some(state_create::<T>),
            std::ptr::null_mut(),
            Some(state_destroy::<T>),
            Some(state_query_end::<T>),
            Some(state_profiling::<T>),
            &mut state
        ));
        // SAFETY(ffi): the entry returns the registered state of this key, created by `state_create::<T>` (the `T`
        // named here) and kept alive by the context for the connection's life.
        unsafe { Opaque::get::<T>(state) }
            .ok_or_else(|| Error::internal("duckdb_v2_context_get_or_create_registered_state returned no state"))
    }
}

/// `DBConfig::ParseMemoryLimit` (`allow_maximum = false`: only a size) / `ParseMemoryLimitOrMaximum` (`-1` and
/// `maximum` are the maximum `u64`).
pub fn parse_memory_limit(text: &str, allow_maximum: bool) -> Result<u64> {
    let text = str_arg(text);
    let mut bytes: idx_t = 0;
    api_call!(duckdb_v2_parse_memory_limit(&text, allow_maximum, &mut bytes));
    Ok(bytes)
}
