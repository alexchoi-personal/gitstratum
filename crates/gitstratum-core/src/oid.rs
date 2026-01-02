use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

const OID_LEN: usize = 32;
const OID_HEX_LEN: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Oid([u8; OID_LEN]);

impl Oid {
    pub const ZERO: Oid = Oid([0u8; OID_LEN]);

    pub fn from_bytes(bytes: [u8; OID_LEN]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() != OID_LEN {
            return Err(Error::InvalidOid(format!(
                "expected {} bytes, got {}",
                OID_LEN,
                slice.len()
            )));
        }
        let mut bytes = [0u8; OID_LEN];
        bytes.copy_from_slice(slice);
        Ok(Self(bytes))
    }

    pub fn from_hex(hex: &str) -> Result<Self> {
        if hex.len() != OID_HEX_LEN {
            return Err(Error::InvalidOid(format!(
                "expected {} hex chars, got {}",
                OID_HEX_LEN,
                hex.len()
            )));
        }
        let bytes = hex::decode(hex).map_err(|e| Error::InvalidOid(e.to_string()))?;
        Self::from_slice(&bytes)
    }

    pub fn hash(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        let mut bytes = [0u8; OID_LEN];
        bytes.copy_from_slice(&result);
        Self(bytes)
    }

    pub fn hash_object(object_type: &str, data: &[u8]) -> Self {
        let header = format!("{} {}\0", object_type, data.len());
        let mut hasher = Sha256::new();
        hasher.update(header.as_bytes());
        hasher.update(data);
        let result = hasher.finalize();
        let mut bytes = [0u8; OID_LEN];
        bytes.copy_from_slice(&result);
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; OID_LEN] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn prefix(&self) -> u8 {
        self.0[0]
    }

    pub fn prefix_hex(&self) -> String {
        hex::encode(&self.0[..1])
    }

    pub fn short(&self) -> String {
        hex::encode(&self.0[..4])
    }

    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; OID_LEN]
    }
}

impl fmt::Debug for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oid({})", self.short())
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for Oid {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_hex(s)
    }
}

impl AsRef<[u8]> for Oid {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl PartialOrd for Oid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Oid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_hash() {
        let data = b"hello world";
        let oid = Oid::hash(data);
        assert!(!oid.is_zero());
        assert_eq!(oid.to_hex().len(), 64);
    }

    #[test]
    fn test_oid_from_hex() {
        let hex = "a".repeat(64);
        let oid = Oid::from_hex(&hex).unwrap();
        assert_eq!(oid.to_hex(), hex);
    }

    #[test]
    fn test_oid_from_hex_invalid_length() {
        let hex = "abc";
        assert!(Oid::from_hex(hex).is_err());
    }

    #[test]
    fn test_oid_hash_object() {
        let data = b"test content";
        let oid = Oid::hash_object("blob", data);
        assert!(!oid.is_zero());
    }

    #[test]
    fn test_oid_prefix() {
        let hex = "ab".to_string() + &"0".repeat(62);
        let oid = Oid::from_hex(&hex).unwrap();
        assert_eq!(oid.prefix(), 0xab);
        assert_eq!(oid.prefix_hex(), "ab");
    }

    #[test]
    fn test_oid_ordering() {
        let oid1 = Oid::from_hex(&("00".to_string() + &"0".repeat(62))).unwrap();
        let oid2 = Oid::from_hex(&("ff".to_string() + &"0".repeat(62))).unwrap();
        assert!(oid1 < oid2);
    }

    #[test]
    fn test_oid_zero() {
        assert!(Oid::ZERO.is_zero());
        let non_zero = Oid::hash(b"test");
        assert!(!non_zero.is_zero());
    }

    #[test]
    fn test_oid_from_bytes() {
        let bytes = [0xab; 32];
        let oid = Oid::from_bytes(bytes);
        assert_eq!(oid.as_bytes(), &bytes);
    }

    #[test]
    fn test_oid_from_slice_invalid_length() {
        let short_slice = [0u8; 16];
        assert!(Oid::from_slice(&short_slice).is_err());

        let long_slice = [0u8; 64];
        assert!(Oid::from_slice(&long_slice).is_err());
    }

    #[test]
    fn test_oid_short() {
        let hex = "ab".repeat(32);
        let oid = Oid::from_hex(&hex).unwrap();
        assert_eq!(oid.short(), "abababab");
    }

    #[test]
    fn test_oid_debug() {
        let hex = "12345678".to_string() + &"00".repeat(28);
        let oid = Oid::from_hex(&hex).unwrap();
        let debug = format!("{:?}", oid);
        assert!(debug.contains("Oid("));
        assert!(debug.contains("12345678"));
    }

    #[test]
    fn test_oid_display() {
        let hex = "ab".repeat(32);
        let oid = Oid::from_hex(&hex).unwrap();
        let display = format!("{}", oid);
        assert_eq!(display, hex);
    }

    #[test]
    fn test_oid_from_str() {
        let hex = "ab".repeat(32);
        let oid: Oid = hex.parse().unwrap();
        assert_eq!(oid.to_hex(), hex);

        let invalid: std::result::Result<Oid, _> = "invalid".parse();
        assert!(invalid.is_err());
    }

    #[test]
    fn test_oid_as_ref() {
        let hex = "ab".repeat(32);
        let oid = Oid::from_hex(&hex).unwrap();
        let bytes_ref: &[u8] = oid.as_ref();
        assert_eq!(bytes_ref.len(), 32);
        assert_eq!(bytes_ref[0], 0xab);
    }

    #[test]
    fn test_oid_from_hex_invalid_chars() {
        let invalid_hex = "g".repeat(64);
        assert!(Oid::from_hex(&invalid_hex).is_err());
    }
}
