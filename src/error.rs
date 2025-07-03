use bincode::Error as BincodeError;
use csv::Error as CsvError;
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

    #[error("Bincode serialization error: {0}")]
    Bincode(#[from] BincodeError),

    #[error("IO error: {0}")]
    IoError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("Unique constraint violation on field '{0}' with value '{1}'")]
    UniqueConstraintViolation(String, String),

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
    Csv(#[from] CsvError),
}

pub type Result<T> = std::result::Result<T, AuroraError>;
