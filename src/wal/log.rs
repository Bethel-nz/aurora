use crate::error::Result;
use std::fs::{File, OpenOptions};
use std::io::{Write};
use std::path::Path;

pub struct WriteAheadLog {
    file: File,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(WriteAheadLog { file })
    }

    pub fn append(&mut self, record: &[u8]) -> Result<()> {
        // Write length prefix
        let len = record.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;

        // Write actual record
        self.file.write_all(record)?;
        self.file.flush()?;

        Ok(())
    }
}
