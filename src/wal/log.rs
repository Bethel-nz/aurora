use crate::error::{AuroraError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: u64,
    pub operation: Operation,
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub enum Operation {
    Put,
    Delete,
    BeginTx,
    CommitTx,
    RollbackTx,
}

pub struct WriteAheadLog {
    file: BufWriter<File>,
    _path: PathBuf,
}

impl WriteAheadLog {
    /// Create or open a Write-Ahead Log file
    ///
    /// Opens existing WAL files in append mode, preserving their contents
    /// for crash recovery. WAL entries are automatically replayed on database
    /// startup and cleared after successful checkpoint.
    pub fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);
        let wal_path = path.with_extension("wal");

        // Open in append mode - preserves existing WAL entries for recovery
        let file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)  // Never truncates existing WAL on open
                .open(&wal_path)?,
        );

        Ok(Self {
            file,
            _path: wal_path,
        })
    }

    pub fn append(&mut self, operation: Operation, key: &str, value: Option<&[u8]>) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| AuroraError::Protocol(e.to_string()))?
            .as_secs();

        let entry = LogEntry {
            timestamp,
            operation,
            key: key.to_string(),
            value: value.map(|v| v.to_vec()),
        };

        let serialized =
            bincode::serialize(&entry).map_err(|e| AuroraError::Protocol(e.to_string()))?;
        let len = serialized.len() as u32;

        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&serialized)?;
        self.file.flush()?;

        Ok(())
    }

    pub fn recover(&mut self) -> Result<Vec<LogEntry>> {
        let mut file = self.file.get_ref();
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    let mut buffer = vec![0u8; len];
                    reader.read_exact(&mut buffer)?;
                    let entry: LogEntry = bincode::deserialize(&buffer)
                        .map_err(|e| AuroraError::Protocol(e.to_string()))?;
                    entries.push(entry);
                }
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(entries)
    }

    pub fn truncate(&mut self) -> Result<()> {
        let file = self.file.get_mut();
        file.set_len(0)?;
        file.sync_all()?;
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_log_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let log_path = temp_dir.path().join("test.wal");
        let mut wal = WriteAheadLog::new(log_path.to_str().unwrap())?;

        // Test append operations
        wal.append(Operation::Put, "test_key", Some(b"test_value"))?;
        wal.append(Operation::BeginTx, "", None)?;
        wal.append(Operation::CommitTx, "", None)?;

        // Test recovery
        let entries = wal.recover()?;
        assert_eq!(entries.len(), 3);

        assert!(matches!(entries[0].operation, Operation::Put));
        assert_eq!(entries[0].key, "test_key");
        assert_eq!(entries[0].value.as_ref().unwrap(), b"test_value");

        assert!(matches!(entries[1].operation, Operation::BeginTx));
        assert!(matches!(entries[2].operation, Operation::CommitTx));

        Ok(())
    }

    #[test]
    fn test_truncate() -> Result<()> {
        let temp_dir = tempdir()?;
        let log_path = temp_dir.path().join("test.wal");
        let mut wal = WriteAheadLog::new(log_path.to_str().unwrap())?;

        // Write some entries
        wal.append(Operation::Put, "key1", Some(b"value1"))?;
        wal.append(Operation::Put, "key2", Some(b"value2"))?;

        // Truncate the log
        wal.truncate()?;

        // Verify log is empty
        let entries = wal.recover()?;
        assert_eq!(entries.len(), 0);

        Ok(())
    }

    #[test]
    fn test_transaction_operations() -> Result<()> {
        let temp_dir = tempdir()?;
        let log_path = temp_dir.path().join("test.wal");
        let mut wal = WriteAheadLog::new(log_path.to_str().unwrap())?;

        // Test transaction sequence
        wal.append(Operation::BeginTx, "", None)?;
        wal.append(Operation::Put, "tx_key1", Some(b"tx_value1"))?;
        wal.append(Operation::Put, "tx_key2", Some(b"tx_value2"))?;
        wal.append(Operation::CommitTx, "", None)?;

        let entries = wal.recover()?;
        assert_eq!(entries.len(), 4);

        assert!(matches!(entries[0].operation, Operation::BeginTx));
        assert!(matches!(entries[1].operation, Operation::Put));
        assert!(matches!(entries[2].operation, Operation::Put));
        assert!(matches!(entries[3].operation, Operation::CommitTx));

        Ok(())
    }

    #[test]
    fn test_rollback_operation() -> Result<()> {
        let temp_dir = tempdir()?;
        let log_path = temp_dir.path().join("test.wal");
        let mut wal = WriteAheadLog::new(log_path.to_str().unwrap())?;
        // Test rollback sequence
        wal.append(Operation::BeginTx, "", None)?;
        wal.append(Operation::Put, "key_to_rollback", Some(b"value"))?;
        wal.append(Operation::RollbackTx, "", None)?;

        let entries = wal.recover()?;
        assert_eq!(entries.len(), 3);

        assert!(matches!(entries[0].operation, Operation::BeginTx));
        assert!(matches!(entries[1].operation, Operation::Put));
        assert!(matches!(entries[2].operation, Operation::RollbackTx));

        Ok(())
    }

    #[test]
    fn test_large_values() -> Result<()> {
        let temp_dir = tempdir()?;
        let log_path = temp_dir.path().join("test.wal");
        let mut wal = WriteAheadLog::new(log_path.to_str().unwrap())?;

        // Test with a larger value
        let large_value = vec![0u8; 1024 * 1024]; // 1MB
        wal.append(Operation::Put, "large_key", Some(&large_value))?;

        let entries = wal.recover()?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value.as_ref().unwrap().len(), large_value.len());

        Ok(())
    }

    #[test]
    fn test_invalid_path() {
        let result = WriteAheadLog::new("/nonexistent/directory/test.wal");
        assert!(result.is_err());
    }
}
