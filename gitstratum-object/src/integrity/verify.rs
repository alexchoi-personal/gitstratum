use std::sync::atomic::{AtomicU64, Ordering};

use gitstratum_core::{Blob, Oid};

use crate::error::{ObjectStoreError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationResult {
    Valid,
    Invalid,
    Missing,
}

pub struct ShaVerifier {
    objects_verified: AtomicU64,
    objects_valid: AtomicU64,
    objects_invalid: AtomicU64,
    objects_missing: AtomicU64,
}

impl ShaVerifier {
    pub fn new() -> Self {
        Self {
            objects_verified: AtomicU64::new(0),
            objects_valid: AtomicU64::new(0),
            objects_invalid: AtomicU64::new(0),
            objects_missing: AtomicU64::new(0),
        }
    }

    pub fn verify(&self, blob: &Blob) -> VerificationResult {
        self.objects_verified.fetch_add(1, Ordering::Relaxed);

        let computed = Oid::hash_object("blob", &blob.data);
        if computed == blob.oid {
            self.objects_valid.fetch_add(1, Ordering::Relaxed);
            VerificationResult::Valid
        } else {
            self.objects_invalid.fetch_add(1, Ordering::Relaxed);
            VerificationResult::Invalid
        }
    }

    pub fn verify_data(&self, expected_oid: &Oid, data: &[u8]) -> VerificationResult {
        self.objects_verified.fetch_add(1, Ordering::Relaxed);

        let computed = Oid::hash(data);
        if computed == *expected_oid {
            self.objects_valid.fetch_add(1, Ordering::Relaxed);
            VerificationResult::Valid
        } else {
            self.objects_invalid.fetch_add(1, Ordering::Relaxed);
            VerificationResult::Invalid
        }
    }

    pub fn record_missing(&self) {
        self.objects_verified.fetch_add(1, Ordering::Relaxed);
        self.objects_missing.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> VerifierStats {
        VerifierStats {
            objects_verified: self.objects_verified.load(Ordering::Relaxed),
            objects_valid: self.objects_valid.load(Ordering::Relaxed),
            objects_invalid: self.objects_invalid.load(Ordering::Relaxed),
            objects_missing: self.objects_missing.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.objects_verified.store(0, Ordering::Relaxed);
        self.objects_valid.store(0, Ordering::Relaxed);
        self.objects_invalid.store(0, Ordering::Relaxed);
        self.objects_missing.store(0, Ordering::Relaxed);
    }
}

impl Default for ShaVerifier {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct VerifierStats {
    pub objects_verified: u64,
    pub objects_valid: u64,
    pub objects_invalid: u64,
    pub objects_missing: u64,
}

impl VerifierStats {
    pub fn valid_rate(&self) -> f64 {
        if self.objects_verified == 0 {
            0.0
        } else {
            self.objects_valid as f64 / self.objects_verified as f64
        }
    }

    pub fn invalid_rate(&self) -> f64 {
        if self.objects_verified == 0 {
            0.0
        } else {
            self.objects_invalid as f64 / self.objects_verified as f64
        }
    }
}

pub struct IntegrityChecker {
    verifier: ShaVerifier,
    stop_on_first_error: bool,
}

impl IntegrityChecker {
    pub fn new(stop_on_first_error: bool) -> Self {
        Self {
            verifier: ShaVerifier::new(),
            stop_on_first_error,
        }
    }

    pub fn check(&self, blob: &Blob) -> Result<VerificationResult> {
        let result = self.verifier.verify(blob);
        if result == VerificationResult::Invalid && self.stop_on_first_error {
            return Err(ObjectStoreError::IntegrityError {
                oid: blob.oid.to_string(),
                expected: blob.oid.to_string(),
                computed: Oid::hash_object("blob", &blob.data).to_string(),
            });
        }
        Ok(result)
    }

    pub fn check_batch(&self, blobs: &[Blob]) -> Result<Vec<(Oid, VerificationResult)>> {
        let mut results = Vec::with_capacity(blobs.len());
        for blob in blobs {
            let result = self.check(blob)?;
            results.push((blob.oid, result));
        }
        Ok(results)
    }

    pub fn stats(&self) -> VerifierStats {
        self.verifier.stats()
    }

    pub fn reset(&self) {
        self.verifier.reset();
    }
}

impl Default for IntegrityChecker {
    fn default() -> Self {
        Self::new(false)
    }
}

pub fn verify_oid(data: &[u8], expected: &Oid) -> bool {
    let computed = Oid::hash(data);
    computed == *expected
}

pub fn verify_blob_oid(data: &[u8], expected: &Oid) -> bool {
    let computed = Oid::hash_object("blob", data);
    computed == *expected
}

pub fn compute_oid(data: &[u8]) -> Oid {
    Oid::hash(data)
}

pub fn compute_blob_oid(data: &[u8]) -> Oid {
    Oid::hash_object("blob", data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_blob(data: &[u8]) -> Blob {
        Blob::new(data.to_vec())
    }

    #[test]
    fn test_verification_result() {
        assert_eq!(VerificationResult::Valid, VerificationResult::Valid);
        assert_ne!(VerificationResult::Valid, VerificationResult::Invalid);
    }

    #[test]
    fn test_sha_verifier_new() {
        let verifier = ShaVerifier::new();
        let stats = verifier.stats();
        assert_eq!(stats.objects_verified, 0);
    }

    #[test]
    fn test_verify_valid() {
        let verifier = ShaVerifier::new();
        let blob = create_test_blob(b"hello world");

        let result = verifier.verify(&blob);
        assert_eq!(result, VerificationResult::Valid);

        let stats = verifier.stats();
        assert_eq!(stats.objects_verified, 1);
        assert_eq!(stats.objects_valid, 1);
    }

    #[test]
    fn test_verify_invalid() {
        let verifier = ShaVerifier::new();
        let mut blob = create_test_blob(b"hello world");
        blob.oid = Oid::hash(b"different data");

        let result = verifier.verify(&blob);
        assert_eq!(result, VerificationResult::Invalid);

        let stats = verifier.stats();
        assert_eq!(stats.objects_invalid, 1);
    }

    #[test]
    fn test_verify_data() {
        let verifier = ShaVerifier::new();
        let data = b"test data";
        let expected = Oid::hash(data);

        let result = verifier.verify_data(&expected, data);
        assert_eq!(result, VerificationResult::Valid);
    }

    #[test]
    fn test_record_missing() {
        let verifier = ShaVerifier::new();
        verifier.record_missing();

        let stats = verifier.stats();
        assert_eq!(stats.objects_verified, 1);
        assert_eq!(stats.objects_missing, 1);
    }

    #[test]
    fn test_reset() {
        let verifier = ShaVerifier::new();
        let blob = create_test_blob(b"test");
        verifier.verify(&blob);

        verifier.reset();
        let stats = verifier.stats();
        assert_eq!(stats.objects_verified, 0);
    }

    #[test]
    fn test_sha_verifier_default() {
        let verifier = ShaVerifier::default();
        let stats = verifier.stats();
        assert_eq!(stats.objects_verified, 0);
    }

    #[test]
    fn test_verifier_stats_rates() {
        let stats = VerifierStats {
            objects_verified: 100,
            objects_valid: 90,
            objects_invalid: 10,
            objects_missing: 0,
        };

        assert_eq!(stats.valid_rate(), 0.9);
        assert_eq!(stats.invalid_rate(), 0.1);
    }

    #[test]
    fn test_verifier_stats_rates_zero() {
        let stats = VerifierStats {
            objects_verified: 0,
            objects_valid: 0,
            objects_invalid: 0,
            objects_missing: 0,
        };

        assert_eq!(stats.valid_rate(), 0.0);
        assert_eq!(stats.invalid_rate(), 0.0);
    }

    #[test]
    fn test_integrity_checker() {
        let checker = IntegrityChecker::default();
        let blob = create_test_blob(b"test data");

        let result = checker.check(&blob).unwrap();
        assert_eq!(result, VerificationResult::Valid);
    }

    #[test]
    fn test_integrity_checker_stop_on_first_error() {
        let checker = IntegrityChecker::new(true);
        let mut blob = create_test_blob(b"test data");
        blob.oid = Oid::hash(b"wrong");

        let result = checker.check(&blob);
        assert!(result.is_err());
    }

    #[test]
    fn test_integrity_checker_batch() {
        let checker = IntegrityChecker::default();
        let blobs: Vec<Blob> = (0..3).map(|i| create_test_blob(&[i])).collect();

        let results = checker.check_batch(&blobs).unwrap();
        assert_eq!(results.len(), 3);
        for (_, result) in results {
            assert_eq!(result, VerificationResult::Valid);
        }
    }

    #[test]
    fn test_verify_oid() {
        let data = b"test data";
        let oid = Oid::hash(data);

        assert!(verify_oid(data, &oid));
        assert!(!verify_oid(b"wrong data", &oid));
    }

    #[test]
    fn test_verify_blob_oid() {
        let data = b"test data";
        let oid = Oid::hash_object("blob", data);

        assert!(verify_blob_oid(data, &oid));
        assert!(!verify_blob_oid(b"wrong data", &oid));
    }

    #[test]
    fn test_compute_oid() {
        let data = b"test data";
        let oid = compute_oid(data);

        assert_eq!(oid, Oid::hash(data));
    }

    #[test]
    fn test_compute_blob_oid() {
        let data = b"test data";
        let oid = compute_blob_oid(data);

        assert_eq!(oid, Oid::hash_object("blob", data));
    }
}
