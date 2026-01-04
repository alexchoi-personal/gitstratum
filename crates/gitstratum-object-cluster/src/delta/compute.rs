use std::collections::HashMap;

use gitstratum_core::Oid;

use crate::error::{ObjectStoreError, Result};

#[derive(Debug, Clone)]
pub struct Delta {
    pub base_oid: Oid,
    pub target_oid: Oid,
    pub instructions: Vec<DeltaInstruction>,
}

#[derive(Debug, Clone)]
pub enum DeltaInstruction {
    Copy { offset: u64, length: u64 },
    Insert { data: Vec<u8> },
}

const HASH_WINDOW: usize = 16;
const HASH_PRIME: u64 = 31;
const HASH_MOD: u64 = 1_000_000_007;
const MAX_COLLISION_LIST_SIZE: usize = 64;

pub struct DeltaComputer {
    max_delta_size: usize,
    min_match_length: usize,
}

impl DeltaComputer {
    pub fn new() -> Self {
        Self {
            max_delta_size: 1024 * 1024,
            min_match_length: 16,
        }
    }

    pub fn with_config(max_delta_size: usize, min_match_length: usize) -> Self {
        Self {
            max_delta_size,
            min_match_length,
        }
    }

    pub fn compute(&self, base: &[u8], target: &[u8]) -> Result<Delta> {
        if target.len() > self.max_delta_size {
            return Err(ObjectStoreError::DeltaTooLarge {
                size: target.len(),
                max: self.max_delta_size,
            });
        }

        let base_oid = Oid::hash(base);
        let target_oid = Oid::hash(target);

        let instructions = self.compute_instructions(base, target);

        Ok(Delta {
            base_oid,
            target_oid,
            instructions,
        })
    }

    fn compute_instructions(&self, base: &[u8], target: &[u8]) -> Vec<DeltaInstruction> {
        let window = self.min_match_length.min(HASH_WINDOW);

        if base.len() < window || target.len() < window {
            return vec![DeltaInstruction::Insert {
                data: target.to_vec(),
            }];
        }

        let base_index = self.build_hash_index(base, window);

        let estimated_instructions = (target.len() / self.min_match_length).max(1);
        let mut instructions = Vec::with_capacity(estimated_instructions);
        let mut target_pos = 0;

        while target_pos < target.len() {
            if target_pos + window <= target.len() {
                if let Some((offset, length)) =
                    self.find_match_hash(&base_index, base, target, target_pos, window)
                {
                    if length >= self.min_match_length {
                        instructions.push(DeltaInstruction::Copy {
                            offset: offset as u64,
                            length: length as u64,
                        });
                        target_pos += length;
                        continue;
                    }
                }
            }

            let mut insert_end = target_pos + 1;
            while insert_end < target.len() {
                if insert_end + window <= target.len() {
                    if let Some((_, length)) =
                        self.find_match_hash(&base_index, base, target, insert_end, window)
                    {
                        if length >= self.min_match_length {
                            break;
                        }
                    }
                }
                insert_end += 1;
            }

            instructions.push(DeltaInstruction::Insert {
                data: target[target_pos..insert_end].to_vec(),
            });
            target_pos = insert_end;
        }

        instructions
    }

    fn build_hash_index(&self, data: &[u8], window: usize) -> HashMap<u64, Vec<usize>> {
        if data.len() < window {
            return HashMap::new();
        }

        let estimated_entries = (data.len() - window + 1).min(HASH_MOD as usize);
        let mut index: HashMap<u64, Vec<usize>> = HashMap::with_capacity(estimated_entries);

        for i in 0..=data.len() - window {
            let hash = Self::compute_hash(&data[i..i + window]);
            let bucket = index.entry(hash % HASH_MOD).or_default();
            if bucket.len() < MAX_COLLISION_LIST_SIZE {
                bucket.push(i);
            }
        }

        index
    }

    fn find_match_hash(
        &self,
        base_index: &HashMap<u64, Vec<usize>>,
        base: &[u8],
        target: &[u8],
        target_pos: usize,
        window: usize,
    ) -> Option<(usize, usize)> {
        if target_pos + window > target.len() {
            return None;
        }

        let target_hash = Self::compute_hash(&target[target_pos..target_pos + window]);

        let positions = base_index.get(&(target_hash % HASH_MOD))?;

        let mut best_offset = 0;
        let mut best_length = 0;

        for &base_pos in positions {
            if base[base_pos..base_pos + window] != target[target_pos..target_pos + window] {
                continue;
            }

            let mut length = window;
            while target_pos + length < target.len()
                && base_pos + length < base.len()
                && target[target_pos + length] == base[base_pos + length]
            {
                length += 1;
            }

            if length > best_length {
                best_offset = base_pos;
                best_length = length;
            }
        }

        if best_length >= self.min_match_length {
            Some((best_offset, best_length))
        } else {
            None
        }
    }

    fn compute_hash(data: &[u8]) -> u64 {
        let mut hash = 0u64;
        for &byte in data {
            hash = hash.wrapping_mul(HASH_PRIME).wrapping_add(byte as u64);
        }
        hash
    }

    pub fn apply(&self, base: &[u8], delta: &Delta) -> Result<Vec<u8>> {
        let mut result = Vec::new();

        for instruction in &delta.instructions {
            match instruction {
                DeltaInstruction::Copy { offset, length } => {
                    let offset = *offset as usize;
                    let length = *length as usize;
                    if offset + length > base.len() {
                        return Err(ObjectStoreError::DeltaApplyError(
                            "copy instruction out of bounds".to_string(),
                        ));
                    }
                    result.extend_from_slice(&base[offset..offset + length]);
                }
                DeltaInstruction::Insert { data } => {
                    result.extend_from_slice(data);
                }
            }
        }

        Ok(result)
    }

    pub fn estimate_size(&self, delta: &Delta) -> usize {
        let mut size = 0;
        for instruction in &delta.instructions {
            match instruction {
                DeltaInstruction::Copy { .. } => size += 16,
                DeltaInstruction::Insert { data } => size += data.len() + 4,
            }
        }
        size
    }
}

impl Default for DeltaComputer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_computer_new() {
        let computer = DeltaComputer::new();
        assert_eq!(computer.max_delta_size, 1024 * 1024);
        assert_eq!(computer.min_match_length, 16);
    }

    #[test]
    fn test_delta_computer_with_config() {
        let computer = DeltaComputer::with_config(2048, 8);
        assert_eq!(computer.max_delta_size, 2048);
        assert_eq!(computer.min_match_length, 8);
    }

    #[test]
    fn test_compute_identical() {
        let computer = DeltaComputer::with_config(1024, 4);
        let data = b"hello world hello world";
        let delta = computer.compute(data, data).unwrap();
        assert_eq!(delta.base_oid, delta.target_oid);
    }

    #[test]
    fn test_compute_different() {
        let computer = DeltaComputer::with_config(1024, 4);
        let base = b"hello world";
        let target = b"hello universe";
        let delta = computer.compute(base, target).unwrap();
        assert_ne!(delta.base_oid, delta.target_oid);
    }

    #[test]
    fn test_apply_delta() {
        let computer = DeltaComputer::with_config(1024, 4);
        let base = b"the quick brown fox jumps over the lazy dog";
        let target = b"the quick brown cat jumps over the lazy dog";

        let delta = computer.compute(base, target).unwrap();
        let result = computer.apply(base, &delta).unwrap();
        assert_eq!(result, target);
    }

    #[test]
    fn test_apply_delta_with_copy() {
        let computer = DeltaComputer::with_config(1024, 4);
        let base = b"AAAA this is a common substring BBBB";
        let target = b"CCCC this is a common substring DDDD";

        let delta = computer.compute(base, target).unwrap();
        let result = computer.apply(base, &delta).unwrap();
        assert_eq!(result, target);

        let has_copy = delta
            .instructions
            .iter()
            .any(|i| matches!(i, DeltaInstruction::Copy { .. }));
        assert!(has_copy, "Expected at least one Copy instruction");
    }

    #[test]
    fn test_delta_too_large() {
        let computer = DeltaComputer::with_config(10, 4);
        let base = b"short";
        let target = b"this is a very long string that exceeds the limit";
        let result = computer.compute(base, target);
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_out_of_bounds() {
        let computer = DeltaComputer::new();
        let delta = Delta {
            base_oid: Oid::hash(b"base"),
            target_oid: Oid::hash(b"target"),
            instructions: vec![DeltaInstruction::Copy {
                offset: 100,
                length: 50,
            }],
        };
        let result = computer.apply(b"short", &delta);
        assert!(result.is_err());
    }

    #[test]
    fn test_estimate_size() {
        let computer = DeltaComputer::new();
        let delta = Delta {
            base_oid: Oid::hash(b"base"),
            target_oid: Oid::hash(b"target"),
            instructions: vec![
                DeltaInstruction::Copy {
                    offset: 0,
                    length: 10,
                },
                DeltaInstruction::Insert {
                    data: vec![1, 2, 3],
                },
            ],
        };
        let size = computer.estimate_size(&delta);
        assert!(size > 0);
    }

    #[test]
    fn test_delta_computer_default() {
        let computer = DeltaComputer::default();
        assert_eq!(computer.max_delta_size, 1024 * 1024);
    }

    #[test]
    fn test_small_base_and_target() {
        let computer = DeltaComputer::with_config(1024, 4);
        let base = b"ab";
        let target = b"cd";
        let delta = computer.compute(base, target).unwrap();
        let result = computer.apply(base, &delta).unwrap();
        assert_eq!(result, target);
    }

    #[test]
    fn test_empty_target() {
        let computer = DeltaComputer::with_config(1024, 4);
        let base = b"hello world";
        let target = b"";
        let delta = computer.compute(base, target).unwrap();
        let result = computer.apply(base, &delta).unwrap();
        assert_eq!(result, target);
    }

    #[test]
    fn test_hash_collision_handling() {
        let computer = DeltaComputer::with_config(1024, 4);
        let base = b"AAAABBBBCCCCDDDDEEEEFFFFGGGG";
        let target = b"XXXXAAAABBBBCCCCDDDDEEEEYYYYZZZZ";

        let delta = computer.compute(base, target).unwrap();
        let result = computer.apply(base, &delta).unwrap();
        assert_eq!(result, target);
    }
}
