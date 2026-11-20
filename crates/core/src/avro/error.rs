use std::fmt;

/// An error raised while reading or writing Avro; the message is what users see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvroError {
    pub message: String,
}

impl AvroError {
    pub fn new(message: impl Into<String>) -> Self {
        AvroError {
            message: message.into(),
        }
    }

    /// Prepends `prefix` to the message.
    pub fn prefixed(self, prefix: &str) -> Self {
        AvroError {
            message: format!("{prefix}{}", self.message),
        }
    }
}

impl fmt::Display for AvroError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AvroError {}

pub type Result<T> = std::result::Result<T, AvroError>;
