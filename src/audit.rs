use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

/// Type of operation being audited
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditOperation {
    Query,
    Mutation,
    Schema,
    Subscription,
}

/// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: DateTime<Utc>,
    pub operation: AuditOperation,
    pub collection: Option<String>,
    pub query: Option<String>,
    pub affected_rows: Option<usize>,
    pub duration_ms: Option<f64>,
    pub user_id: Option<String>,
    pub success: bool,
    pub error: Option<String>,
}

/// Audit logger trait
pub trait AuditLogger: Send + Sync {
    fn log(&self, entry: AuditEntry);
}

/// File-based audit logger
pub struct FileAuditLogger {
    log_path: PathBuf,
}

impl FileAuditLogger {
    pub fn new(log_path: PathBuf) -> Self {
        Self { log_path }
    }
}

impl AuditLogger for FileAuditLogger {
    fn log(&self, entry: AuditEntry) {
        let json = match serde_json::to_string(&entry) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("Failed to serialize audit entry: {}", e);
                return;
            }
        };

        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open audit log file: {}", e);
                return;
            }
        };

        if let Err(e) = writeln!(file, "{}", json) {
            eprintln!("Failed to write audit log: {}", e);
        }
    }
}

/// Console audit logger (for development)
pub struct ConsoleAuditLogger;

impl AuditLogger for ConsoleAuditLogger {
    fn log(&self, entry: AuditEntry) {
        let status = if entry.success { "SUCCESS" } else { "FAILED" };
        let duration = entry
            .duration_ms
            .map(|d| format!(" ({}ms)", d))
            .unwrap_or_default();

        println!(
            "[AUDIT] {} {:?} collection={} user={} rows={} {}{}",
            status,
            entry.operation,
            entry.collection.as_deref().unwrap_or("N/A"),
            entry.user_id.as_deref().unwrap_or("system"),
            entry.affected_rows.unwrap_or(0),
            entry.timestamp.format("%Y-%m-%d %H:%M:%S"),
            duration
        );

        if let Some(err) = entry.error {
            println!("       Error: {}", err);
        }
    }
}

/// No-op audit logger (default)
pub struct NoOpAuditLogger;

impl AuditLogger for NoOpAuditLogger {
    fn log(&self, _entry: AuditEntry) {
        // Do nothing
    }
}
