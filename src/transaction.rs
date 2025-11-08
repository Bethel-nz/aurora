use crate::error::{AuroraError, Result};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static TRANSACTION_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(u64);

impl Default for TransactionId {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionId {
    pub fn new() -> Self {
        Self(TRANSACTION_ID_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    pub fn from_u64(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct TransactionBuffer {
    pub id: TransactionId,
    pub writes: DashMap<String, Vec<u8>>,
    pub deletes: DashMap<String, ()>,
}

impl TransactionBuffer {
    pub fn new(id: TransactionId) -> Self {
        Self {
            id,
            writes: DashMap::new(),
            deletes: DashMap::new(),
        }
    }

    pub fn write(&self, key: String, value: Vec<u8>) {
        self.deletes.remove(&key);
        self.writes.insert(key, value);
    }

    pub fn delete(&self, key: String) {
        self.writes.remove(&key);
        self.deletes.insert(key, ());
    }

    pub fn is_empty(&self) -> bool {
        self.writes.is_empty() && self.deletes.is_empty()
    }
}

pub struct TransactionManager {
    pub active_transactions: Arc<DashMap<TransactionId, Arc<TransactionBuffer>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(DashMap::new()),
        }
    }

    pub fn begin(&self) -> Arc<TransactionBuffer> {
        let tx_id = TransactionId::new();
        let buffer = Arc::new(TransactionBuffer::new(tx_id));
        self.active_transactions.insert(tx_id, Arc::clone(&buffer));
        buffer
    }

    pub fn commit(&self, tx_id: TransactionId) -> Result<()> {
        if !self.active_transactions.contains_key(&tx_id) {
            return Err(AuroraError::InvalidOperation(
                "Transaction not found or already committed".into(),
            ));
        }

        self.active_transactions.remove(&tx_id);
        Ok(())
    }

    pub fn rollback(&self, tx_id: TransactionId) -> Result<()> {
        if !self.active_transactions.contains_key(&tx_id) {
            return Err(AuroraError::InvalidOperation(
                "Transaction not found or already rolled back".into(),
            ));
        }

        self.active_transactions.remove(&tx_id);
        Ok(())
    }

    pub fn is_active(&self, tx_id: TransactionId) -> bool {
        self.active_transactions.contains_key(&tx_id)
    }

    pub fn active_count(&self) -> usize {
        self.active_transactions.len()
    }
}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            active_transactions: Arc::clone(&self.active_transactions),
        }
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_isolation() {
        let manager = TransactionManager::new();

        let tx1 = manager.begin();
        let tx2 = manager.begin();

        assert_ne!(tx1.id, tx2.id);
        assert_eq!(manager.active_count(), 2);

        tx1.write("key1".to_string(), b"value1".to_vec());
        tx2.write("key1".to_string(), b"value2".to_vec());

        assert_eq!(tx1.writes.get("key1").unwrap().as_slice(), b"value1");
        assert_eq!(tx2.writes.get("key1").unwrap().as_slice(), b"value2");

        manager.commit(tx1.id).unwrap();
        assert_eq!(manager.active_count(), 1);

        manager.rollback(tx2.id).unwrap();
        assert_eq!(manager.active_count(), 0);
    }
}
