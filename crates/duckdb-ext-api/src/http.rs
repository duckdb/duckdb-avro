//! HTTP providers an extension implements (`duckdb_v2_http_provider_*`: an `HTTPUtil` with the client callbacks
//! the engine's request loop drives), the parameter / request / response views, and the transport sessions a file
//! system issues requests through (`duckdb_v2_file_opener_create_http_session` + `duckdb_v2_http_session_*`). See
//! the engine's C API additions.
//!
//! The engine keeps the request loop (file access check, retries with backoff, the client pool, the `HTTP` log);
//! an [`HttpProviderImpl`] supplies the parameters of a request set and the [`HttpClient`]s that perform one
//! attempt each. A file system creates an [`HttpSession`] for an opener and a path and runs [`OwnedHttpRequest`]s
//! through it.

use std::ffi::c_void;

use crate::Result;
use crate::context::Context;
use crate::custom_file_system::FileOpener;
use crate::error::{Error, guard};
use crate::extension::Extension;
use crate::opaque::Opaque;
use crate::sys::*;
use crate::text::str_arg;

/// `DUCKDB_V2_HTTP_TRANSPORT_REUSE_POLICY` (`CLIENT_FREE`, `EPHEMERAL`, `SESSION_LOCAL`, `SHARED`).
pub type TransportReusePolicy = DUCKDB_V2_HTTP_TRANSPORT_REUSE_POLICY;
/// `DUCKDB_V2_HTTP_PARAM`: the fields of the engine's `HTTPParams`.
pub type HttpParam = DUCKDB_V2_HTTP_PARAM;

/// The method of a request (`RequestType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Put,
    Head,
    Delete,
    Post,
    Options,
}

impl HttpMethod {
    pub fn to_raw(self) -> DUCKDB_V2_HTTP_REQUEST_TYPE {
        match self {
            HttpMethod::Get => DUCKDB_V2_HTTP_REQUEST_TYPE_GET,
            HttpMethod::Put => DUCKDB_V2_HTTP_REQUEST_TYPE_PUT,
            HttpMethod::Head => DUCKDB_V2_HTTP_REQUEST_TYPE_HEAD,
            HttpMethod::Delete => DUCKDB_V2_HTTP_REQUEST_TYPE_DELETE,
            HttpMethod::Post => DUCKDB_V2_HTTP_REQUEST_TYPE_POST,
            HttpMethod::Options => DUCKDB_V2_HTTP_REQUEST_TYPE_OPTIONS,
        }
    }

    pub fn from_raw(raw: DUCKDB_V2_HTTP_REQUEST_TYPE) -> Result<HttpMethod> {
        Ok(match raw {
            DUCKDB_V2_HTTP_REQUEST_TYPE_GET => HttpMethod::Get,
            DUCKDB_V2_HTTP_REQUEST_TYPE_PUT => HttpMethod::Put,
            DUCKDB_V2_HTTP_REQUEST_TYPE_HEAD => HttpMethod::Head,
            DUCKDB_V2_HTTP_REQUEST_TYPE_DELETE => HttpMethod::Delete,
            DUCKDB_V2_HTTP_REQUEST_TYPE_POST => HttpMethod::Post,
            DUCKDB_V2_HTTP_REQUEST_TYPE_OPTIONS => HttpMethod::Options,
            other => return Err(Error::invalid_input(format!("'{}' is not an HTTP request type", other.0))),
        })
    }

    /// The method name as it goes on the request line.
    pub fn as_str(self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Put => "PUT",
            HttpMethod::Head => "HEAD",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Post => "POST",
            HttpMethod::Options => "OPTIONS",
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Text sinks
// -------------------------------------------------------------------------------------------------

unsafe extern "C" fn collect_text(
    text: *const duckdb_v2_str,
    data: *mut c_void,
    _err: *mut duckdb_v2_error_info_handle,
) {
    if text.is_null() || data.is_null() {
        return;
    }
    // SAFETY(ffi): T4 - `data` is the `Vec<String>` the one-call getter below passed as the sink's user data, reached
    // through no other reference during the call.
    let out = unsafe { &mut *(data as *mut Vec<String>) };
    // SAFETY(ffi): T3 - `text` is the engine's `duckdb_v2_str` view, valid for the sink call.
    out.push(crate::text::str_to_string(unsafe { *text }));
}

/// Runs a text-sink entry point and returns what the sink collected (one string per sink call).
fn collect(call: impl FnOnce(duckdb_v2_text_sink_fn, *mut c_void) -> Result<()>) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    call(Some(collect_text), &mut out as *mut Vec<String> as *mut c_void)?;
    Ok(out)
}

/// A text-sink entry point that produces exactly one text.
fn collect_one(call: impl FnOnce(duckdb_v2_text_sink_fn, *mut c_void) -> Result<()>) -> Result<String> {
    Ok(collect(call)?.pop().unwrap_or_default())
}

// -------------------------------------------------------------------------------------------------
// The opener: the transport session it opens
// -------------------------------------------------------------------------------------------------

impl FileOpener {
    /// `HTTPTransportManager::CreateSession(opener, info)`: runs the provider's `initialize_parameters` for this
    /// opener and `path`, and opens a client reuse scope for the requests issued through the session.
    pub fn create_http_session(&self, path: &str) -> Result<HttpSession> {
        let path = str_arg(path);
        let mut raw: duckdb_v2_http_session_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_opener_create_http_session(self.as_raw(), &path, &mut raw));
        Ok(HttpSession { raw })
    }
}

// -------------------------------------------------------------------------------------------------
// Parameters
// -------------------------------------------------------------------------------------------------

/// The parameters a set of requests runs under (`HTTPParams`), borrowed: from a callback, a session or a request.
/// The parameters a client callback receives are read-only; the setters fail on them.
#[derive(Clone, Copy)]
pub struct HttpParams {
    raw: duckdb_v2_http_params_handle,
}

impl HttpParams {
    /// # Safety
    /// (ffi) `raw` must be a live handle the C API lent for this type (a callback argument or a field of an owned
    /// wrapper); the borrowed (`Copy`) wrapper must not be used past the call that lent it.
    pub unsafe fn from_raw(raw: duckdb_v2_http_params_handle) -> HttpParams {
        HttpParams { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_http_params_handle {
        self.raw
    }

    pub fn get_uint(&self, param: HttpParam) -> Result<u64> {
        let mut value = 0u64;
        api_call!(duckdb_v2_http_params_get_uint(self.raw, param, &mut value));
        Ok(value)
    }

    pub fn set_uint(&self, param: HttpParam, value: u64) -> Result<()> {
        api_call!(duckdb_v2_http_params_set_uint(self.raw, param, value));
        Ok(())
    }

    pub fn get_bool(&self, param: HttpParam) -> Result<bool> {
        let mut value = false;
        api_call!(duckdb_v2_http_params_get_bool(self.raw, param, &mut value));
        Ok(value)
    }

    pub fn set_bool(&self, param: HttpParam, value: bool) -> Result<()> {
        api_call!(duckdb_v2_http_params_set_bool(self.raw, param, value));
        Ok(())
    }

    pub fn get_double(&self, param: HttpParam) -> Result<f64> {
        let mut value = 0f64;
        api_call!(duckdb_v2_http_params_get_double(self.raw, param, &mut value));
        Ok(value)
    }

    pub fn set_double(&self, param: HttpParam, value: f64) -> Result<()> {
        api_call!(duckdb_v2_http_params_set_double(self.raw, param, value));
        Ok(())
    }

    pub fn get_text(&self, param: HttpParam) -> Result<String> {
        collect_one(|sink, data| {
            api_call!(duckdb_v2_http_params_get_text(self.raw, param, sink, data));
            Ok(())
        })
    }

    pub fn set_text(&self, param: HttpParam, value: &str) -> Result<()> {
        let value = str_arg(value);
        api_call!(duckdb_v2_http_params_set_text(self.raw, param, &value));
        Ok(())
    }

    /// `http_timeout` in seconds.
    pub fn timeout_seconds(&self) -> Result<u64> {
        self.get_uint(DUCKDB_V2_HTTP_PARAM_TIMEOUT_SECONDS)
    }

    pub fn timeout_micros(&self) -> Result<u64> {
        self.get_uint(DUCKDB_V2_HTTP_PARAM_TIMEOUT_MICROS)
    }

    pub fn retries(&self) -> Result<u64> {
        self.get_uint(DUCKDB_V2_HTTP_PARAM_RETRIES)
    }

    pub fn retry_wait_ms(&self) -> Result<u64> {
        self.get_uint(DUCKDB_V2_HTTP_PARAM_RETRY_WAIT_MS)
    }

    pub fn retry_backoff(&self) -> Result<f64> {
        self.get_double(DUCKDB_V2_HTTP_PARAM_RETRY_BACKOFF)
    }

    pub fn keep_alive(&self) -> Result<bool> {
        self.get_bool(DUCKDB_V2_HTTP_PARAM_KEEP_ALIVE)
    }

    pub fn follow_location(&self) -> Result<bool> {
        self.get_bool(DUCKDB_V2_HTTP_PARAM_FOLLOW_LOCATION)
    }

    /// `(override_verify_ssl, verify_ssl)`: whether a secret overrode the certificate verification, and its value.
    pub fn verify_ssl(&self) -> Result<(bool, bool)> {
        Ok((self.get_bool(DUCKDB_V2_HTTP_PARAM_OVERRIDE_VERIFY_SSL)?, self.get_bool(DUCKDB_V2_HTTP_PARAM_VERIFY_SSL)?))
    }

    /// `(host, port, username, password)` of the HTTP proxy; an empty host means none.
    pub fn proxy(&self) -> Result<(String, u64, String, String)> {
        Ok((
            self.get_text(DUCKDB_V2_HTTP_PARAM_PROXY_HOST)?,
            self.get_uint(DUCKDB_V2_HTTP_PARAM_PROXY_PORT)?,
            self.get_text(DUCKDB_V2_HTTP_PARAM_PROXY_USERNAME)?,
            self.get_text(DUCKDB_V2_HTTP_PARAM_PROXY_PASSWORD)?,
        ))
    }

    /// `GetTransportReuseDomain()`: the provider-issued identity of transport-compatible parameters.
    pub fn transport_reuse_domain(&self) -> Result<u64> {
        self.get_uint(DUCKDB_V2_HTTP_PARAM_TRANSPORT_REUSE_DOMAIN)
    }

    pub fn set_transport_reuse_domain(&self, domain: u64) -> Result<()> {
        self.set_uint(DUCKDB_V2_HTTP_PARAM_TRANSPORT_REUSE_DOMAIN, domain)
    }

    /// A header every request under these parameters carries (`extra_headers`).
    pub fn set_extra_header(&self, name: &str, value: &str) -> Result<()> {
        let (name, value) = (str_arg(name), str_arg(value));
        api_call!(duckdb_v2_http_params_set_extra_header(self.raw, &name, &value));
        Ok(())
    }

    pub fn remove_extra_header(&self, name: &str) -> Result<()> {
        let name = str_arg(name);
        api_call!(duckdb_v2_http_params_remove_extra_header(self.raw, &name));
        Ok(())
    }

    /// The extra headers, ordered by name.
    pub fn extra_headers(&self) -> Result<Vec<(String, String)>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_http_params_get_extra_header_count(self.raw, &mut count));
        let mut result = Vec::with_capacity(count as usize);
        for index in 0..count {
            let mut pair = collect(|sink, data| {
                api_call!(duckdb_v2_http_params_get_extra_header(self.raw, index, sink, sink, data));
                Ok(())
            })?;
            let value = pair.pop().unwrap_or_default();
            let name = pair.pop().unwrap_or_default();
            result.push((name, value));
        }
        Ok(result)
    }

    /// Attaches the provider's own parameters (what a C++ `HTTPParams` subclass adds); cloned with the parameters.
    pub fn set_user_data<T: Clone + 'static>(&self, data: T) -> Result<()> {
        let ptr = Box::into_raw(Box::new(data)) as *mut c_void;
        api_call!(duckdb_v2_http_params_set_user_data(
            self.raw,
            ptr,
            Some(destroy_user_data::<T>),
            Some(clone_user_data::<T>)
        ));
        Ok(())
    }

    opaque_getter!(
        /// The provider's data set with [`HttpParams::set_user_data`] (with the same `T`).
        user_data,
        duckdb_v2_http_params_get_user_data
    );

    /// The mutable form of [`HttpParams::user_data`]: through the exclusive `&mut` of this view (an owned copy's
    /// `params()` view, or the view a provider callback is lent; the engine does not share the parameters while a
    /// callback runs).
    pub fn user_data_mut<T>(&mut self) -> Result<Option<&mut T>> {
        let mut data: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_http_params_get_user_data(self.raw, &mut data));
        // SAFETY(ffi L8.I10): the slot was filled by `set_user_data` with the `T` the caller names (or is empty) and the engine
        // keeps it alive with the parameters; `&mut self` is the one access path to this view's parameters for the
        // call, so no other reference to the value is live.
        Ok(unsafe { Opaque::get_mut::<T>(data) })
    }

    /// `HTTPParams::Clone()`: an owned copy (including the transport binding and the cloned user data).
    pub fn copy(&self) -> Result<OwnedHttpParams> {
        let mut raw: duckdb_v2_http_params_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_http_params_copy(self.raw, &mut raw));
        Ok(OwnedHttpParams { raw })
    }

    /// Whether the parameters' logger records the `HTTP` log type.
    pub fn should_log(&self) -> Result<bool> {
        let mut should_log = false;
        api_call!(duckdb_v2_http_params_should_log(self.raw, &mut should_log));
        Ok(should_log)
    }

    /// `HTTPParams::logger`: the client's logger the parameters were initialized with, for a provider's own log
    /// types (`None` without a client, or on an older host without the entry).
    pub fn logger(&self) -> Result<Option<crate::custom_file_system::Logger>> {
        if crate::api().duckdb_v2_http_params_get_logger.is_none() {
            return Ok(None);
        }
        let mut raw: duckdb_v2_logger_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_http_params_get_logger(self.raw, &mut raw));
        // SAFETY(ffi): the entry returns a logger handle owned by the caller (null without one); the owned wrapper
        // destroys it.
        Ok(if raw.is_null() { None } else { Some(unsafe { crate::custom_file_system::Logger::from_raw(raw) }) })
    }
}

unsafe extern "C" fn destroy_user_data<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        // SAFETY(ffi): the pointer came from `Box::into_raw` in `set_user_data` with this `T`.
        drop(unsafe { Box::from_raw(ptr as *mut T) });
    }
}

unsafe extern "C" fn clone_user_data<T: Clone>(ptr: *mut c_void) -> *mut c_void {
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY(ffi): as for `destroy_user_data`.
    let value = unsafe { &*(ptr as *const T) };
    Box::into_raw(Box::new(value.clone())) as *mut c_void
}

/// Parameters made by [`HttpParams::copy`] (owned).
pub struct OwnedHttpParams {
    raw: duckdb_v2_http_params_handle,
}

impl OwnedHttpParams {
    /// The borrowed view (valid while the owner is).
    pub fn params(&self) -> HttpParams {
        HttpParams { raw: self.raw }
    }
}

impl Drop for OwnedHttpParams {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_http_params_destroy(&mut self.raw));
    }
}

// -------------------------------------------------------------------------------------------------
// Requests
// -------------------------------------------------------------------------------------------------

/// The network measurements of the last attempt of a request.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HttpTiming {
    pub elapsed_seconds: f64,
    pub has_first_byte: bool,
    pub first_byte_seconds: f64,
    pub bytes_received: u64,
}

/// A request (`BaseRequest`), borrowed: the request a client performs, or the view of an [`OwnedHttpRequest`].
#[derive(Clone, Copy)]
pub struct HttpRequest {
    raw: duckdb_v2_http_request_handle,
}

impl HttpRequest {
    /// # Safety
    /// (ffi) `raw` must be a live handle the C API lent for this type (a callback argument or a field of an owned
    /// wrapper); the borrowed (`Copy`) wrapper must not be used past the call that lent it.
    pub unsafe fn from_raw(raw: duckdb_v2_http_request_handle) -> HttpRequest {
        HttpRequest { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_http_request_handle {
        self.raw
    }

    /// The parameters the request runs under.
    pub fn params(&self) -> Result<HttpParams> {
        let mut raw: duckdb_v2_http_params_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_http_request_get_params(self.raw, &mut raw));
        Ok(HttpParams { raw })
    }

    pub fn method(&self) -> Result<HttpMethod> {
        let mut raw = DUCKDB_V2_HTTP_REQUEST_TYPE_GET;
        api_call!(duckdb_v2_http_request_get_type(self.raw, &mut raw));
        HttpMethod::from_raw(raw)
    }

    pub fn url(&self) -> Result<String> {
        collect_one(|sink, data| {
            api_call!(duckdb_v2_http_request_get_url(self.raw, sink, data));
            Ok(())
        })
    }

    /// The path and query (`BaseRequest::path`).
    pub fn path(&self) -> Result<String> {
        collect_one(|sink, data| {
            api_call!(duckdb_v2_http_request_get_path(self.raw, sink, data));
            Ok(())
        })
    }

    /// `scheme://host[:port]`.
    pub fn proto_host_port(&self) -> Result<String> {
        collect_one(|sink, data| {
            api_call!(duckdb_v2_http_request_get_proto_host_port(self.raw, sink, data));
            Ok(())
        })
    }

    /// Sets a header (replacing an existing one).
    pub fn set_header(&self, name: &str, value: &str) -> Result<()> {
        let (name, value) = (str_arg(name), str_arg(value));
        api_call!(duckdb_v2_http_request_set_header(self.raw, &name, &value));
        Ok(())
    }

    pub fn remove_header(&self, name: &str) -> Result<()> {
        let name = str_arg(name);
        api_call!(duckdb_v2_http_request_remove_header(self.raw, &name));
        Ok(())
    }

    /// The headers in insertion order.
    pub fn headers(&self) -> Result<Vec<(String, String)>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_http_request_get_header_count(self.raw, &mut count));
        let mut result = Vec::with_capacity(count as usize);
        for index in 0..count {
            let mut pair = collect(|sink, data| {
                api_call!(duckdb_v2_http_request_get_header(self.raw, index, sink, sink, data));
                Ok(())
            })?;
            let value = pair.pop().unwrap_or_default();
            let name = pair.pop().unwrap_or_default();
            result.push((name, value));
        }
        Ok(result)
    }

    /// A header by (case-insensitive) name.
    pub fn header(&self, name: &str) -> Result<Option<String>> {
        let name_view = str_arg(name);
        let mut found = false;
        let value = collect(|sink, data| {
            api_call!(duckdb_v2_http_request_find_header(self.raw, &name_view, &mut found, sink, data));
            Ok(())
        })?;
        Ok(if found { Some(value.into_iter().next().unwrap_or_default()) } else { None })
    }

    /// The body of a PUT or POST (borrowed from the request).
    pub fn body(&self) -> Result<&[u8]> {
        let mut data: *const c_void = std::ptr::null();
        let mut size: idx_t = 0;
        api_call!(duckdb_v2_http_request_get_body(self.raw, &mut data, &mut size));
        if data.is_null() || size == 0 {
            return Ok(&[]);
        }
        // SAFETY(ffi): the body lives as long as the request does.
        Ok(unsafe { std::slice::from_raw_parts(data as *const u8, size as usize) })
    }

    /// The content type of a PUT.
    pub fn content_type(&self) -> Result<String> {
        collect_one(|sink, data| {
            api_call!(duckdb_v2_http_request_get_content_type(self.raw, sink, data));
            Ok(())
        })
    }

    /// Whether a POST is sent with the GET method.
    pub fn post_as_get(&self) -> Result<bool> {
        let mut flag = false;
        api_call!(duckdb_v2_http_request_get_post_as_get(self.raw, &mut flag));
        Ok(flag)
    }

    pub fn has_response_handler(&self) -> Result<bool> {
        let mut flag = false;
        api_call!(duckdb_v2_http_request_has_response_callback(self.raw, &mut flag));
        Ok(flag)
    }

    pub fn has_content_handler(&self) -> Result<bool> {
        let mut flag = false;
        api_call!(duckdb_v2_http_request_has_content_callback(self.raw, &mut flag));
        Ok(flag)
    }

    /// Invokes the response handler of a GET once the status and headers arrived; `false` stops the request.
    pub fn handle_response(&self, response: &HttpResponse) -> Result<bool> {
        let mut go_on = true;
        api_call!(duckdb_v2_http_request_handle_response(self.raw, response.raw, &mut go_on));
        Ok(go_on)
    }

    /// Invokes the content handler of a GET with a chunk of the body; `false` stops the request.
    pub fn handle_content(&self, data: &[u8]) -> Result<bool> {
        let mut go_on = true;
        api_call!(duckdb_v2_http_request_handle_content(
            self.raw,
            data.as_ptr() as *const c_void,
            data.len() as idx_t,
            &mut go_on
        ));
        Ok(go_on)
    }

    /// Records the time to first byte of the attempt.
    pub fn record_first_byte(&self) -> Result<()> {
        api_call!(duckdb_v2_http_request_record_first_byte(self.raw));
        Ok(())
    }

    pub fn set_bytes_received(&self, bytes: u64) -> Result<()> {
        api_call!(duckdb_v2_http_request_set_bytes_received(self.raw, bytes));
        Ok(())
    }

    /// The measurements of the last attempt.
    pub fn timing(&self) -> Result<HttpTiming> {
        let mut timing = HttpTiming::default();
        api_call!(duckdb_v2_http_request_get_timing(
            self.raw,
            &mut timing.elapsed_seconds,
            &mut timing.has_first_byte,
            &mut timing.first_byte_seconds,
            &mut timing.bytes_received
        ));
        Ok(timing)
    }
}

type ResponseHandler = Box<dyn FnMut(&HttpResponse) -> bool>;
type ContentHandler = Box<dyn FnMut(&[u8]) -> bool>;

#[expect(
    clippy::disallowed_methods,
    reason = "an FFI callback boundary: a Rust panic must not unwind into the engine's C caller (it is reported as the callback's failure)"
)]
unsafe extern "C" fn response_handler_trampoline(data: *mut c_void, response: duckdb_v2_http_response_handle) -> bool {
    if data.is_null() {
        return true;
    }
    // SAFETY(ffi): `data` points at the boxed handler the owning request keeps alive.
    let handler = unsafe { &mut *(data as *mut ResponseHandler) };
    let view = HttpResponse { raw: response };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(&view))).unwrap_or(false)
}

#[expect(
    clippy::disallowed_methods,
    reason = "an FFI callback boundary: a Rust panic must not unwind into the engine's C caller (it is reported as the callback's failure)"
)]
unsafe extern "C" fn content_handler_trampoline(data: *mut c_void, bytes: *const c_void, size: idx_t) -> bool {
    if data.is_null() {
        return true;
    }
    // SAFETY(ffi): T4 - `data` points at the boxed handler the owning request keeps alive for the request's life, and
    // the engine calls the handler from one thread at a time.
    let handler = unsafe { &mut *(data as *mut ContentHandler) };
    let chunk: &[u8] = if bytes.is_null() || size == 0 {
        &[]
    } else {
        // SAFETY(ffi): T3 - `bytes` is the engine's `size`-byte buffer, valid for the handler call.
        unsafe { std::slice::from_raw_parts(bytes as *const u8, size as usize) }
    };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(chunk))).unwrap_or(false)
}

/// A request created by the extension (`GetRequestInfo` / `PutRequestInfo` / ...), run through an [`HttpSession`].
pub struct OwnedHttpRequest {
    raw: duckdb_v2_http_request_handle,
    response_handler: Option<Box<ResponseHandler>>,
    content_handler: Option<Box<ContentHandler>>,
}

impl OwnedHttpRequest {
    /// A request of `method` against `url` under a copy of `params` (the parameters' extra headers are merged in).
    pub fn new(params: &HttpParams, method: HttpMethod, url: &str) -> Result<OwnedHttpRequest> {
        let url = str_arg(url);
        let mut raw: duckdb_v2_http_request_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_http_request_create(params.raw, method.to_raw(), &url, &mut raw));
        Ok(OwnedHttpRequest { raw, response_handler: None, content_handler: None })
    }

    /// The borrowed view (valid while the owner is).
    pub fn request(&self) -> HttpRequest {
        HttpRequest { raw: self.raw }
    }

    /// The body of a PUT or POST (copied).
    pub fn set_body(&self, body: &[u8]) -> Result<&Self> {
        api_call!(duckdb_v2_http_request_set_body(self.raw, body.as_ptr() as *const c_void, body.len() as idx_t));
        Ok(self)
    }

    pub fn set_content_type(&self, content_type: &str) -> Result<&Self> {
        let content_type = str_arg(content_type);
        api_call!(duckdb_v2_http_request_set_content_type(self.raw, &content_type));
        Ok(self)
    }

    /// `PostRequestInfo::send_post_as_get_request`.
    pub fn set_post_as_get(&self, post_as_get: bool) -> Result<&Self> {
        api_call!(duckdb_v2_http_request_set_post_as_get(self.raw, post_as_get));
        Ok(self)
    }

    /// Whether a failure after the retries is returned as a response instead of an error.
    pub fn set_try_request(&self, try_request: bool) -> Result<&Self> {
        api_call!(duckdb_v2_http_request_set_try_request(self.raw, try_request));
        Ok(self)
    }

    /// The response handler of a GET: called with the status and headers before the body; `false` stops.
    pub fn set_response_handler(&mut self, handler: impl FnMut(&HttpResponse) -> bool + 'static) -> Result<&mut Self> {
        let mut boxed: Box<ResponseHandler> = Box::new(Box::new(handler));
        let data = &mut *boxed as *mut ResponseHandler as *mut c_void;
        api_call!(duckdb_v2_http_request_set_response_callback(self.raw, Some(response_handler_trampoline), data));
        self.response_handler = Some(boxed);
        Ok(self)
    }

    /// The content handler of a GET: receives the body as it arrives (the response's body then stays empty).
    pub fn set_content_handler(&mut self, handler: impl FnMut(&[u8]) -> bool + 'static) -> Result<&mut Self> {
        let mut boxed: Box<ContentHandler> = Box::new(Box::new(handler));
        let data = &mut *boxed as *mut ContentHandler as *mut c_void;
        api_call!(duckdb_v2_http_request_set_content_callback(self.raw, Some(content_handler_trampoline), data));
        self.content_handler = Some(boxed);
        Ok(self)
    }
}

impl Drop for OwnedHttpRequest {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_http_request_destroy(&mut self.raw));
    }
}

// -------------------------------------------------------------------------------------------------
// Responses
// -------------------------------------------------------------------------------------------------

/// A response (`HTTPResponse`), borrowed: the response a client fills, the one a response handler sees, or the
/// view of an [`OwnedHttpResponse`].
#[derive(Clone, Copy)]
pub struct HttpResponse {
    raw: duckdb_v2_http_response_handle,
}

impl HttpResponse {
    /// # Safety
    /// (ffi) `raw` must be a live handle the C API lent for this type (a callback argument or a field of an owned
    /// wrapper); the borrowed (`Copy`) wrapper must not be used past the call that lent it.
    pub unsafe fn from_raw(raw: duckdb_v2_http_response_handle) -> HttpResponse {
        HttpResponse { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_http_response_handle {
        self.raw
    }

    pub fn set_status(&self, status: i32) -> Result<&Self> {
        api_call!(duckdb_v2_http_response_set_status(self.raw, status));
        Ok(self)
    }

    pub fn set_reason(&self, reason: &str) -> Result<&Self> {
        let reason = str_arg(reason);
        api_call!(duckdb_v2_http_response_set_reason(self.raw, &reason));
        Ok(self)
    }

    /// Appends a header (a repeated header keeps every value).
    pub fn add_header(&self, name: &str, value: &str) -> Result<&Self> {
        let (name, value) = (str_arg(name), str_arg(value));
        api_call!(duckdb_v2_http_response_add_header(self.raw, &name, &value));
        Ok(self)
    }

    pub fn append_body(&self, data: &[u8]) -> Result<&Self> {
        api_call!(duckdb_v2_http_response_append_body(self.raw, data.as_ptr() as *const c_void, data.len() as idx_t));
        Ok(self)
    }

    /// Marks the response as a failed request (the engine retries it).
    pub fn set_request_error(&self, error: &str) -> Result<&Self> {
        let error = str_arg(error);
        api_call!(duckdb_v2_http_response_set_request_error(self.raw, &error));
        Ok(self)
    }

    /// The status code (0 for a failed request).
    pub fn status(&self) -> Result<i32> {
        let mut status = 0i32;
        api_call!(duckdb_v2_http_response_get_status(self.raw, &mut status));
        Ok(status)
    }

    pub fn reason(&self) -> Result<String> {
        collect_one(|sink, data| {
            api_call!(duckdb_v2_http_response_get_reason(self.raw, sink, data));
            Ok(())
        })
    }

    /// The first value of a header by (case-insensitive) name.
    pub fn header(&self, name: &str) -> Result<Option<String>> {
        let name_view = str_arg(name);
        let mut found = false;
        let value = collect(|sink, data| {
            api_call!(duckdb_v2_http_response_find_header(self.raw, &name_view, &mut found, sink, data));
            Ok(())
        })?;
        Ok(if found { Some(value.into_iter().next().unwrap_or_default()) } else { None })
    }

    /// Every header entry (a repeated header once per value).
    pub fn headers(&self) -> Result<Vec<(String, String)>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_http_response_get_header_count(self.raw, &mut count));
        let mut result = Vec::with_capacity(count as usize);
        for index in 0..count {
            let mut pair = collect(|sink, data| {
                api_call!(duckdb_v2_http_response_get_header(self.raw, index, sink, sink, data));
                Ok(())
            })?;
            let value = pair.pop().unwrap_or_default();
            let name = pair.pop().unwrap_or_default();
            result.push((name, value));
        }
        Ok(result)
    }

    /// The body (empty when a content handler consumed it), borrowed from the response.
    pub fn body(&self) -> Result<&[u8]> {
        let mut data: *const c_void = std::ptr::null();
        let mut size: idx_t = 0;
        api_call!(duckdb_v2_http_response_get_body(self.raw, &mut data, &mut size));
        if data.is_null() || size == 0 {
            return Ok(&[]);
        }
        // SAFETY(ffi): the body lives as long as the response does.
        Ok(unsafe { std::slice::from_raw_parts(data as *const u8, size as usize) })
    }

    /// The request error of a failed request.
    pub fn request_error(&self) -> Result<Option<String>> {
        let mut has_error = false;
        let text = collect(|sink, data| {
            api_call!(duckdb_v2_http_response_get_request_error(self.raw, &mut has_error, sink, data));
            Ok(())
        })?;
        Ok(if has_error { Some(text.into_iter().next().unwrap_or_default()) } else { None })
    }

    /// Whether the status is a success (2xx or 304) and no request error is set.
    pub fn is_success(&self) -> Result<bool> {
        if self.request_error()?.is_some() {
            return Ok(false);
        }
        let status = self.status()?;
        Ok((200..300).contains(&status) || status == 304)
    }
}

/// A response returned by [`HttpSession::request`] (owned).
pub struct OwnedHttpResponse {
    raw: duckdb_v2_http_response_handle,
}

impl OwnedHttpResponse {
    /// The borrowed view (valid while the owner is).
    pub fn response(&self) -> HttpResponse {
        HttpResponse { raw: self.raw }
    }
}

impl Drop for OwnedHttpResponse {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_http_response_destroy(&mut self.raw));
    }
}

// -------------------------------------------------------------------------------------------------
// Sessions
// -------------------------------------------------------------------------------------------------

/// A session of the engine's HTTP transport manager (`HTTPTransportManager::Session`): the parameters the provider
/// initialized for an opener and a path, and the client reuse scope of the requests issued through it.
pub struct HttpSession {
    raw: duckdb_v2_http_session_handle,
}

impl HttpSession {
    pub fn as_raw(&self) -> duckdb_v2_http_session_handle {
        self.raw
    }

    /// The parameters of the session (borrowed, writable).
    pub fn params(&self) -> Result<HttpParams> {
        let mut raw: duckdb_v2_http_params_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_http_session_get_params(self.raw, &mut raw));
        Ok(HttpParams { raw })
    }

    /// Runs the request through the engine (file access check, client pool, retries, `HTTP` log) and returns the
    /// final response. The request's parameters must be a copy of the session's.
    pub fn request(&self, request: &OwnedHttpRequest) -> Result<OwnedHttpResponse> {
        let mut raw: duckdb_v2_http_response_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_http_session_request(self.raw, request.raw, &mut raw));
        Ok(OwnedHttpResponse { raw })
    }

    /// Drops the pooled clients so that the next request opens a fresh connection.
    pub fn invalidate(&self) -> Result<()> {
        api_call!(duckdb_v2_http_session_invalidate(self.raw));
        Ok(())
    }
}

impl Drop for HttpSession {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_http_session_destroy(&mut self.raw));
    }
}

// -------------------------------------------------------------------------------------------------
// Providers
// -------------------------------------------------------------------------------------------------

/// A connection to one `scheme://host[:port]` made by an [`HttpProviderImpl`]; performs one attempt per
/// [`HttpClient::request`]. Pooled by the engine when the provider's reuse policy allows it.
pub trait HttpClient: Send {
    /// `HTTPClient::Initialize(params)`: applies request-local settings before use (also to a pooled client).
    fn initialize(&mut self, _params: &HttpParams) -> Result<()> {
        Ok(())
    }

    /// `HTTPClient::CanReuse(params)`: whether this pooled client may serve requests under these parameters.
    fn can_reuse(&self, _params: &HttpParams) -> bool {
        false
    }

    /// `HTTPClient::Cleanup()`: removes request-local credentials and state before the client is pooled.
    fn cleanup(&mut self) -> Result<()> {
        Ok(())
    }

    /// `HTTPClient::Request(request)`: performs one attempt and fills `response`. A transport failure is reported
    /// with [`HttpResponse::set_request_error`] (the engine retries), not as an `Err`.
    fn request(&mut self, request: &HttpRequest, response: &HttpResponse) -> Result<()>;
}

/// An HTTP provider (`HTTPUtil`): the parameters of a request set and the clients that perform the attempts.
pub trait HttpProviderImpl: 'static {
    /// `HTTPUtil::InitializeParameters(opener, info)`: the engine filled the proxy settings and the logger; set
    /// the standard fields from the opener's settings and attach the provider's data. The parameters are the
    /// callback's alone while it runs (`&mut`: the one path to `HttpParams::user_data_mut`).
    fn initialize_parameters(
        &self,
        _opener: Option<FileOpener>,
        _path: Option<&str>,
        _params: &mut HttpParams,
    ) -> Result<()> {
        Ok(())
    }

    /// `HTTPUtil::InitializeClient(params, proto_host_port)`.
    fn create_client(&self, params: &HttpParams, proto_host_port: &str) -> Result<Box<dyn HttpClient>>;
}

type ClientBox = Box<dyn HttpClient>;

/// # Safety
/// (ffi) `data` is the provider data `HttpProvider::set_implementation::<P>` stored (the engine passes it to every
/// callback of that provider; L8.I1).
unsafe fn provider_of<'a, P: HttpProviderImpl>(data: *mut c_void) -> Result<&'a P> {
    // SAFETY(ffi): forwarded - the provider data was set by `set_implementation` with this `P`.
    unsafe { Opaque::get::<P>(data) }.ok_or_else(|| Error::internal("the HTTP provider has no implementation data"))
}

/// # Safety
/// (ffi) `client` is a client `create_client_trampoline` handed out (`Box::into_raw` of a `ClientBox`) and
/// `client_destroy_trampoline` has not freed; the engine calls a client's operations from one thread at a time
/// (L8.I1).
unsafe fn client_of<'a>(client: *mut c_void) -> &'a mut ClientBox {
    // SAFETY(ffi): forwarded - the client pointer came from `Box::into_raw` in `create_client_trampoline`.
    unsafe { &mut *(client as *mut ClientBox) }
}

unsafe extern "C" fn initialize_parameters_trampoline<P: HttpProviderImpl>(
    provider_data: *mut c_void,
    opener: duckdb_v2_file_opener_handle,
    path: *const duckdb_v2_str,
    params: duckdb_v2_http_params_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `provider_data` is this provider's `P` (`provider_of`).
        let provider = unsafe { provider_of::<P>(provider_data) }?;
        // SAFETY(ffi): a borrowed opener handle the engine lends for the duration of the callback.
        let opener = if opener.is_null() { None } else { Some(unsafe { FileOpener::from_raw(opener) }) };
        // SAFETY(ffi): a live view for the duration of the callback.
        let path = if path.is_null() { None } else { Some(crate::text::str_to_string(unsafe { *path })) };
        provider.initialize_parameters(opener, path.as_deref(), &mut HttpParams { raw: params })
    });
}

unsafe extern "C" fn create_client_trampoline<P: HttpProviderImpl>(
    provider_data: *mut c_void,
    params: duckdb_v2_http_params_handle,
    proto_host_port: *const duckdb_v2_str,
    out_client: *mut *mut c_void,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `provider_data` is this provider's `P` (`provider_of`).
        let provider = unsafe { provider_of::<P>(provider_data) }?;
        // SAFETY(ffi): a live view for the duration of the callback.
        let origin = crate::text::str_to_string(unsafe { *proto_host_port });
        let client = provider.create_client(&HttpParams { raw: params }, &origin)?;
        let boxed: Box<ClientBox> = Box::new(client);
        // SAFETY(ffi): the engine checked `out_client`.
        unsafe { *out_client = Box::into_raw(boxed) as *mut c_void };
        Ok(())
    });
}

unsafe extern "C" fn client_initialize_trampoline(
    _provider_data: *mut c_void,
    client: *mut c_void,
    params: duckdb_v2_http_params_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `client` is a live client of this provider (`client_of`); `params` is lent for the call.
    guard(err, || unsafe { client_of(client) }.initialize(&HttpParams { raw: params }));
}

#[expect(
    clippy::disallowed_methods,
    reason = "an FFI callback boundary: a Rust panic must not unwind into the engine's C caller (it is reported as the callback's failure)"
)]
unsafe extern "C" fn client_can_reuse_trampoline(
    _provider_data: *mut c_void,
    client: *mut c_void,
    params: duckdb_v2_http_params_handle,
) -> bool {
    // SAFETY(ffi): T4 - `client` is a live client of this provider (`client_of`); `params` is lent for the call.
    let client = unsafe { client_of(client) };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| client.can_reuse(&HttpParams { raw: params })))
        .unwrap_or(false)
}

unsafe extern "C" fn client_cleanup_trampoline(
    _provider_data: *mut c_void,
    client: *mut c_void,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `client` is a live client of this provider (`client_of`).
    guard(err, || unsafe { client_of(client) }.cleanup());
}

unsafe extern "C" fn client_destroy_trampoline(_provider_data: *mut c_void, client: *mut c_void) {
    if !client.is_null() {
        // SAFETY(ffi): the pointer came from `Box::into_raw` in `create_client_trampoline`; destroyed once.
        drop(unsafe { Box::from_raw(client as *mut ClientBox) });
    }
}

unsafe extern "C" fn client_request_trampoline(
    _provider_data: *mut c_void,
    client: *mut c_void,
    request: duckdb_v2_http_request_handle,
    response: duckdb_v2_http_response_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `client` is a live client of this provider (`client_of`); the request and response handles are
    // lent for the call.
    guard(err, || unsafe { client_of(client) }.request(&HttpRequest { raw: request }, &HttpResponse { raw: response }));
}

/// An HTTP provider description under construction (owned until published).
pub struct HttpProvider {
    raw: duckdb_v2_http_provider_handle,
}

impl HttpProvider {
    /// `name` is `HTTPUtil::GetName()` (e.g. "HTTPFS").
    pub fn new(factory: crate::context::Factory, name: &str) -> Result<HttpProvider> {
        let name = str_arg(name);
        let mut raw: duckdb_v2_http_provider_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_http_provider_create(factory.as_raw(), &name, &mut raw));
        Ok(HttpProvider { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_http_provider_handle {
        self.raw
    }

    /// Installs the implementation: its data becomes the provider's user data and its callbacks are registered.
    pub fn set_implementation<P: HttpProviderImpl>(&self, provider: P) -> Result<&Self> {
        let mut opaque = Opaque::new(provider);
        api_call!(duckdb_v2_http_provider_set_user_data(self.raw, &mut opaque));
        api_call!(duckdb_v2_http_provider_set_initialize_parameters_callback(
            self.raw,
            Some(initialize_parameters_trampoline::<P>)
        ));
        api_call!(duckdb_v2_http_provider_set_create_client_callback(self.raw, Some(create_client_trampoline::<P>)));
        api_call!(duckdb_v2_http_provider_set_client_initialize_callback(self.raw, Some(client_initialize_trampoline)));
        api_call!(duckdb_v2_http_provider_set_client_can_reuse_callback(self.raw, Some(client_can_reuse_trampoline)));
        api_call!(duckdb_v2_http_provider_set_client_cleanup_callback(self.raw, Some(client_cleanup_trampoline)));
        api_call!(duckdb_v2_http_provider_set_client_destroy_callback(self.raw, Some(client_destroy_trampoline)));
        api_call!(duckdb_v2_http_provider_set_client_request_callback(self.raw, Some(client_request_trampoline)));
        Ok(self)
    }

    /// `HTTPUtil::GetTransportReusePolicy()` (the default is EPHEMERAL).
    pub fn set_transport_reuse_policy(&self, policy: TransportReusePolicy) -> Result<&Self> {
        api_call!(duckdb_v2_http_provider_set_transport_reuse_policy(self.raw, policy));
        Ok(self)
    }

    /// A request header whose value the `HTTP` log always replaces by "redacted".
    pub fn add_redacted_header(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_http_provider_add_redacted_header(self.raw, &name));
        Ok(self)
    }
}

impl Drop for HttpProvider {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_http_provider_destroy(&mut self.raw));
    }
}

impl Extension {
    /// `DBConfig::SetHTTPUtil`: makes the provider the database's HTTP provider.
    pub fn set_http_provider(&self, provider: &HttpProvider) -> Result<()> {
        api_call!(duckdb_v2_extension_set_http_provider(self.as_raw(), provider.raw));
        Ok(())
    }
}

impl Context {
    /// `DBConfig::SetHTTPUtil` from a callback's context (a setting switching implementations).
    pub fn set_http_provider(&self, provider: &HttpProvider) -> Result<()> {
        api_call!(duckdb_v2_context_set_http_provider(self.as_raw(), provider.raw));
        Ok(())
    }

    /// `DBConfig::GetHTTPUtil().GetName()` of the context's database.
    pub fn http_provider_name(&self) -> Result<String> {
        collect_one(|sink, data| {
            api_call!(duckdb_v2_context_get_http_provider_name(self.as_raw(), sink, data));
            Ok(())
        })
    }
}
