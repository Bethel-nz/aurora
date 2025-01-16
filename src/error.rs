use std::time::SystemTimeError;
use thiserror::Error;
use zip::result::ZipError;

#[derive(Error, Debug)]
pub enum AuroraError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] sled::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("System time error: {0}")]
    SystemTime(#[from] SystemTimeError),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Invalid key: {0}")]
    InvalidKey(String),

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error("Zip error: {0}")]
    Zip(#[from] ZipError),

    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
}

impl From<Box<bincode::ErrorKind>> for AuroraError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        AuroraError::Protocol(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, AuroraError>;
