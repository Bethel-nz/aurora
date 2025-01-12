use thiserror::Error;

#[derive(Error, Debug)]
pub enum AuroraError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] sled::Error),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

pub type Result<T> = std::result::Result<T, AuroraError>;
