use std::collections::HashMap;
use std::sync::Arc;

use gitstratum_core::Oid;
use parking_lot::RwLock;

use super::compute::{Delta, DeltaInstruction};
use crate::error::{ObjectStoreError, Result};

#[derive(Debug, Clone)]
pub struct StoredDelta {
    pub base_oid: Oid,
    pub target_oid: Oid,
    pub data: Vec<u8>,
    pub size: usize,
}

impl StoredDelta {
    pub fn from_delta(delta: &Delta) -> Result<Self> {
        let data = Self::serialize(delta)?;
        let size = data.len();
        Ok(Self {
            base_oid: delta.base_oid,
            target_oid: delta.target_oid,
            data,
            size,
        })
    }

    pub fn to_delta(&self) -> Result<Delta> {
        Self::deserialize(&self.data, self.base_oid, self.target_oid)
    }

    fn serialize(delta: &Delta) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        for instruction in &delta.instructions {
            match instruction {
                DeltaInstruction::Copy { offset, length } => {
                    data.push(0x01);
                    data.extend_from_slice(&offset.to_le_bytes());
                    data.extend_from_slice(&length.to_le_bytes());
                }
                DeltaInstruction::Insert { data: insert_data } => {
                    data.push(0x02);
                    let len = insert_data.len() as u64;
                    data.extend_from_slice(&len.to_le_bytes());
                    data.extend_from_slice(insert_data);
                }
            }
        }
        Ok(data)
    }

    fn deserialize(data: &[u8], base_oid: Oid, target_oid: Oid) -> Result<Delta> {
        const MAX_INSTRUCTIONS: usize = 1_000_000;
        const MAX_INSERT_SIZE: u64 = 100 * 1024 * 1024;

        let mut instructions = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            if instructions.len() >= MAX_INSTRUCTIONS {
                return Err(ObjectStoreError::DeltaDeserializeError(format!(
                    "too many delta instructions: exceeded {} limit",
                    MAX_INSTRUCTIONS
                )));
            }

            let op = data[pos];
            pos += 1;

            match op {
                0x01 => {
                    if pos + 16 > data.len() {
                        return Err(ObjectStoreError::DeltaDeserializeError(
                            "truncated copy instruction".to_string(),
                        ));
                    }
                    let offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let length = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    instructions.push(DeltaInstruction::Copy { offset, length });
                }
                0x02 => {
                    if pos + 8 > data.len() {
                        return Err(ObjectStoreError::DeltaDeserializeError(
                            "truncated insert instruction".to_string(),
                        ));
                    }
                    let len_u64 = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    if len_u64 > MAX_INSERT_SIZE {
                        return Err(ObjectStoreError::DeltaDeserializeError(format!(
                            "insert data too large: {} bytes (max: {} bytes)",
                            len_u64, MAX_INSERT_SIZE
                        )));
                    }
                    let len = len_u64 as usize;
                    pos += 8;
                    if pos + len > data.len() {
                        return Err(ObjectStoreError::DeltaDeserializeError(
                            "truncated insert data".to_string(),
                        ));
                    }
                    let insert_data = data[pos..pos + len].to_vec();
                    pos += len;
                    instructions.push(DeltaInstruction::Insert { data: insert_data });
                }
                _ => {
                    return Err(ObjectStoreError::DeltaDeserializeError(format!(
                        "unknown instruction type: {}",
                        op
                    )));
                }
            }
        }

        Ok(Delta {
            base_oid,
            target_oid,
            instructions,
        })
    }
}

pub struct DeltaStorage {
    deltas: Arc<RwLock<HashMap<(Oid, Oid), StoredDelta>>>,
    max_stored: usize,
}

impl DeltaStorage {
    pub fn new(max_stored: usize) -> Self {
        Self {
            deltas: Arc::new(RwLock::new(HashMap::new())),
            max_stored,
        }
    }

    pub fn store(&self, delta: &Delta) -> Result<()> {
        let stored = StoredDelta::from_delta(delta)?;
        let key = (delta.base_oid, delta.target_oid);

        let mut deltas = self.deltas.write();
        if deltas.len() >= self.max_stored && !deltas.contains_key(&key) {
            if let Some(oldest_key) = deltas.keys().next().cloned() {
                deltas.remove(&oldest_key);
            }
        }
        deltas.insert(key, stored);
        Ok(())
    }

    pub fn get(&self, base_oid: &Oid, target_oid: &Oid) -> Option<StoredDelta> {
        let deltas = self.deltas.read();
        deltas.get(&(*base_oid, *target_oid)).cloned()
    }

    pub fn has(&self, base_oid: &Oid, target_oid: &Oid) -> bool {
        let deltas = self.deltas.read();
        deltas.contains_key(&(*base_oid, *target_oid))
    }

    pub fn remove(&self, base_oid: &Oid, target_oid: &Oid) -> Option<StoredDelta> {
        let mut deltas = self.deltas.write();
        deltas.remove(&(*base_oid, *target_oid))
    }

    pub fn count(&self) -> usize {
        self.deltas.read().len()
    }

    pub fn total_size(&self) -> usize {
        self.deltas.read().values().map(|d| d.size).sum()
    }

    pub fn clear(&self) {
        self.deltas.write().clear();
    }

    pub fn find_deltas_for_target(&self, target_oid: &Oid) -> Vec<StoredDelta> {
        let deltas = self.deltas.read();
        deltas
            .iter()
            .filter(|((_, target), _)| target == target_oid)
            .map(|(_, delta)| delta.clone())
            .collect()
    }
}

impl Default for DeltaStorage {
    fn default() -> Self {
        Self::new(10000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_delta() -> Delta {
        Delta {
            base_oid: Oid::hash(b"base"),
            target_oid: Oid::hash(b"target"),
            instructions: vec![
                DeltaInstruction::Copy {
                    offset: 0,
                    length: 10,
                },
                DeltaInstruction::Insert {
                    data: vec![1, 2, 3, 4, 5],
                },
            ],
        }
    }

    #[test]
    fn test_stored_delta_roundtrip() {
        let delta = create_test_delta();
        let stored = StoredDelta::from_delta(&delta).unwrap();
        let recovered = stored.to_delta().unwrap();

        assert_eq!(recovered.base_oid, delta.base_oid);
        assert_eq!(recovered.target_oid, delta.target_oid);
        assert_eq!(recovered.instructions.len(), delta.instructions.len());
    }

    #[test]
    fn test_delta_storage_store_get() {
        let storage = DeltaStorage::new(100);
        let delta = create_test_delta();

        storage.store(&delta).unwrap();

        let stored = storage.get(&delta.base_oid, &delta.target_oid).unwrap();
        assert_eq!(stored.base_oid, delta.base_oid);
        assert_eq!(stored.target_oid, delta.target_oid);
    }

    #[test]
    fn test_delta_storage_has() {
        let storage = DeltaStorage::new(100);
        let delta = create_test_delta();

        assert!(!storage.has(&delta.base_oid, &delta.target_oid));
        storage.store(&delta).unwrap();
        assert!(storage.has(&delta.base_oid, &delta.target_oid));
    }

    #[test]
    fn test_delta_storage_remove() {
        let storage = DeltaStorage::new(100);
        let delta = create_test_delta();

        storage.store(&delta).unwrap();
        let removed = storage.remove(&delta.base_oid, &delta.target_oid);
        assert!(removed.is_some());
        assert!(!storage.has(&delta.base_oid, &delta.target_oid));
    }

    #[test]
    fn test_delta_storage_count() {
        let storage = DeltaStorage::new(100);
        assert_eq!(storage.count(), 0);

        let delta = create_test_delta();
        storage.store(&delta).unwrap();
        assert_eq!(storage.count(), 1);
    }

    #[test]
    fn test_delta_storage_clear() {
        let storage = DeltaStorage::new(100);
        let delta = create_test_delta();
        storage.store(&delta).unwrap();
        assert_eq!(storage.count(), 1);

        storage.clear();
        assert_eq!(storage.count(), 0);
    }

    #[test]
    fn test_delta_storage_max_stored() {
        let storage = DeltaStorage::new(2);

        for i in 0..3 {
            let delta = Delta {
                base_oid: Oid::hash(&[i]),
                target_oid: Oid::hash(&[i + 10]),
                instructions: vec![],
            };
            storage.store(&delta).unwrap();
        }

        assert_eq!(storage.count(), 2);
    }

    #[test]
    fn test_delta_storage_default() {
        let storage = DeltaStorage::default();
        assert_eq!(storage.max_stored, 10000);
    }

    #[test]
    fn test_find_deltas_for_target() {
        let storage = DeltaStorage::new(100);
        let target_oid = Oid::hash(b"target");

        let delta1 = Delta {
            base_oid: Oid::hash(b"base1"),
            target_oid,
            instructions: vec![],
        };
        let delta2 = Delta {
            base_oid: Oid::hash(b"base2"),
            target_oid,
            instructions: vec![],
        };

        storage.store(&delta1).unwrap();
        storage.store(&delta2).unwrap();

        let found = storage.find_deltas_for_target(&target_oid);
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn test_total_size() {
        let storage = DeltaStorage::new(100);
        let delta = create_test_delta();
        storage.store(&delta).unwrap();
        assert!(storage.total_size() > 0);
    }
}
