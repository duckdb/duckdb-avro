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
