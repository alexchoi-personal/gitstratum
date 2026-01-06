use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::RngCore;
use sha2::{Digest, Sha256};

const PAT_PREFIX: &str = "gst_";
const TOKEN_BYTES: usize = 32;

pub fn generate_pat() -> (String, String) {
    let mut bytes = [0u8; TOKEN_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);

    let token = format!("{}{}", PAT_PREFIX, URL_SAFE_NO_PAD.encode(bytes));
    let hash = hash_token(&token);

    (token, hash)
}

pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn is_valid_pat_format(token: &str) -> bool {
    if !token.starts_with(PAT_PREFIX) {
        return false;
    }

    let encoded = &token[PAT_PREFIX.len()..];
    if let Ok(decoded) = URL_SAFE_NO_PAD.decode(encoded) {
        decoded.len() == TOKEN_BYTES
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_pat_format() {
        let (token, _hash) = generate_pat();

        assert!(token.starts_with("gst_"));
        assert!(token.len() > 40);
    }

    #[test]
    fn test_generate_pat_unique() {
        let (token1, hash1) = generate_pat();
        let (token2, hash2) = generate_pat();

        assert_ne!(token1, token2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_token_deterministic() {
        let token = "gst_test_token_12345";
        let hash1 = hash_token(token);
        let hash2 = hash_token(token);

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_token_length() {
        let (token, hash) = generate_pat();
        let rehash = hash_token(&token);

        assert_eq!(hash.len(), 64);
        assert_eq!(hash, rehash);
    }

    #[test]
    fn test_is_valid_pat_format_valid() {
        let (token, _) = generate_pat();
        assert!(is_valid_pat_format(&token));
    }

    #[test]
    fn test_is_valid_pat_format_wrong_prefix() {
        assert!(!is_valid_pat_format("ghp_abc123"));
        assert!(!is_valid_pat_format("token_abc123"));
    }

    #[test]
    fn test_is_valid_pat_format_invalid_base64() {
        assert!(!is_valid_pat_format("gst_!!!invalid!!!"));
    }

    #[test]
    fn test_is_valid_pat_format_wrong_length() {
        assert!(!is_valid_pat_format("gst_short"));
        assert!(!is_valid_pat_format("gst_"));
    }

    #[test]
    fn test_hash_different_tokens() {
        let hash1 = hash_token("gst_token_a");
        let hash2 = hash_token("gst_token_b");

        assert_ne!(hash1, hash2);
    }
}
