//! The crate's single error type.

use std::fmt;

/// The class of an [`Error`]; the embedding layer maps it to its own error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Malformed input: a corrupt container file, an undecodable block, an invalid schema.
    InvalidInput,
    /// Valid input the crate does not support (an unknown codec, an unsupported feature).
    NotImplemented,
    /// An I/O error of the sink a [`crate::FileWriter`] writes to.
    Io,
}

/// Error of every fallible operation in this crate: a kind plus a human-readable message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    /// An error of the given kind.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Error { kind, message: message.into() }
    }

    /// An [`ErrorKind::InvalidInput`] error.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::InvalidInput, message)
    }

    /// An [`ErrorKind::NotImplemented`] error.
    pub fn not_implemented(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::NotImplemented, message)
    }

    /// `Error::new(e.kind(), format!("{context}: {e}"))`: the same kind, the message prefixed. Under `cfg(kani)` (the
    /// proof harnesses, `make kani`) the message is `context` alone, like the macros below.
    #[cfg(not(kani))]
    pub fn with_context(context: &str, e: &Error) -> Self {
        Error::new(e.kind, format!("{context}: {}", e.message))
    }

    #[cfg(kani)]
    pub fn with_context(context: &str, e: &Error) -> Self {
        Error::new(e.kind, context)
    }

    /// The error class.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The message without any kind prefix.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::new(ErrorKind::Io, e.to_string())
    }
}

/// `Result` with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// `Error::invalid_input(format!(..))` for the decoder's error paths. Under `cfg(kani)` (the proof harnesses, `make
/// kani`) the message is the bare format string: `core::fmt` is far beyond what CBMC explores in reasonable time, and
/// no harness checks message text; the error kind and the control flow are what they prove.
#[cfg(not(kani))]
macro_rules! invalid_input {
    ($($arg:tt)*) => {
        $crate::error::Error::invalid_input(format!($($arg)*))
    };
}
#[cfg(kani)]
macro_rules! invalid_input {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {{
        $(let _ = &$arg;)*
        $crate::error::Error::invalid_input($fmt)
    }};
}
pub(crate) use invalid_input;

/// `Error::not_implemented(format!(..))`, with the same `cfg(kani)` rule as [`invalid_input!`].
#[cfg(not(kani))]
macro_rules! not_implemented {
    ($($arg:tt)*) => {
        $crate::error::Error::not_implemented(format!($($arg)*))
    };
}
#[cfg(kani)]
macro_rules! not_implemented {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {{
        $(let _ = &$arg;)*
        $crate::error::Error::not_implemented($fmt)
    }};
}
pub(crate) use not_implemented;

/// `format!(..)` for an error message, with the same `cfg(kani)` rule as [`invalid_input!`]: the bare format string.
#[cfg(not(kani))]
macro_rules! message {
    ($($arg:tt)*) => {
        format!($($arg)*)
    };
}
#[cfg(kani)]
macro_rules! message {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {{
        $(let _ = &$arg;)*
        String::from($fmt)
    }};
}
pub(crate) use message;
