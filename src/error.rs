use color_eyre::eyre::Report;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorCode {
    InternalError,
    IoError,
    StorageError,
    SerializationError,
    CollectionNotFound,
    CollectionAlreadyExists,
    UniqueConstraintViolation,
    ProtocolError,
    InvalidOperation,
    InvalidInput,
    NotFound,
    InvalidKey,
    InvalidValue,
    InvalidDefinition,
    QueryError,
    SchemaError,
}

#[derive(Debug)]
pub struct AqlError {
    pub code: ErrorCode,
    pub message: String,
    pub source: Option<Report>,
}

impl AqlError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    pub fn from_error<E>(code: ErrorCode, message: impl Into<String>, err: E) -> Self
    where
        E: Into<Report>,
    {
        Self {
            code,
            message: message.into(),
            source: Some(err.into()),
        }
    }

    // Convenience constructors
    pub fn invalid_operation(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidOperation, msg)
    }

    pub fn schema_error(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::SchemaError, msg)
    }
}

impl fmt::Display for AqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:?}] {}", self.code, self.message)
    }
}

impl std::error::Error for AqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|r| r.as_ref() as &(dyn std::error::Error + 'static))
    }
}

// Result alias
pub type Result<T> = std::result::Result<T, AqlError>;

impl From<std::io::Error> for AqlError {
    fn from(err: std::io::Error) -> Self {
        Self::from_error(ErrorCode::IoError, "IO Error", err)
    }
}

impl From<sled::Error> for AqlError {
    fn from(err: sled::Error) -> Self {
        Self::from_error(ErrorCode::StorageError, "Storage Error", err)
    }
}

impl From<serde_json::Error> for AqlError {
    fn from(err: serde_json::Error) -> Self {
        Self::from_error(ErrorCode::SerializationError, "Serialization Error", err)
    }
}

impl From<bincode::Error> for AqlError {
    fn from(err: bincode::Error) -> Self {
        Self::from_error(ErrorCode::SerializationError, "Bincode Error", err)
    }
}

impl From<std::time::SystemTimeError> for AqlError {
    fn from(err: std::time::SystemTimeError) -> Self {
        Self::from_error(ErrorCode::InternalError, "System Time Error", err)
    }
}

impl From<zip::result::ZipError> for AqlError {
    fn from(err: zip::result::ZipError) -> Self {
        Self::from_error(ErrorCode::InternalError, "Zip Error", err)
    }
}

impl From<csv::Error> for AqlError {
    fn from(err: csv::Error) -> Self {
        Self::from_error(ErrorCode::InternalError, "CSV Error", err)
    }
}
