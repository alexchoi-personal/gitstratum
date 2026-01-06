use crate::types::SignedUrl;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// LFS batch API request
///
/// Clients send this to request upload or download URLs for multiple objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRequest {
    /// Operation type: "upload" or "download"
    pub operation: Operation,
    /// Objects to upload or download
    pub objects: Vec<BatchObject>,
    /// Optional transfer adapters (default: "basic")
    #[serde(default)]
    pub transfers: Vec<String>,
    /// Optional ref being updated (for upload)
    #[serde(rename = "ref")]
    pub git_ref: Option<RefInfo>,
    /// Optional hash algorithm (default: "sha256")
    #[serde(default)]
    pub hash_algo: Option<String>,
}

impl BatchRequest {
    pub fn validate(&self) -> Result<(), crate::error::LfsError> {
        for obj in &self.objects {
            obj.validate()?;
        }
        Ok(())
    }
}

/// LFS batch API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResponse {
    /// Transfer adapter to use (usually "basic")
    pub transfer: String,
    /// Objects with their actions or errors
    pub objects: Vec<BatchResponseObject>,
    /// Optional hash algorithm
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_algo: Option<String>,
}

/// Batch operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Upload,
    Download,
}

const MAX_LFS_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// Object in a batch request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchObject {
    /// SHA-256 OID as hex string
    pub oid: String,
    /// Size in bytes
    pub size: u64,
}

impl BatchObject {
    pub fn validate(&self) -> Result<(), crate::error::LfsError> {
        if self.oid.len() != 64 {
            return Err(crate::error::LfsError::InvalidOid(format!(
                "OID must be 64 hex characters, got {}",
                self.oid.len()
            )));
        }
        if !self.oid.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(crate::error::LfsError::InvalidOid(
                "OID must contain only hex characters".to_string(),
            ));
        }
        if self.size > MAX_LFS_OBJECT_SIZE {
            return Err(crate::error::LfsError::ObjectTooLarge {
                size: self.size,
                limit: MAX_LFS_OBJECT_SIZE,
            });
        }
        Ok(())
    }
}

/// Git ref information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefInfo {
    /// Ref name (e.g., "refs/heads/main")
    pub name: String,
}

/// Object in a batch response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResponseObject {
    /// SHA-256 OID as hex string
    pub oid: String,
    /// Size in bytes
    pub size: u64,
    /// Whether the object was authenticated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authenticated: Option<bool>,
    /// Actions the client can take (upload, download, verify)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions: Option<HashMap<String, Action>>,
    /// Error if the object cannot be processed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ObjectError>,
}

/// Action the client can take on an object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    /// URL to perform the action
    pub href: String,
    /// Headers to include in the request
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub header: HashMap<String, String>,
    /// Seconds until the URL expires
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<u64>,
    /// Absolute expiration time (ISO 8601)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

/// Error for a specific object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectError {
    /// HTTP status code
    pub code: u16,
    /// Error message
    pub message: String,
}

impl BatchResponseObject {
    /// Create a successful response with actions
    pub fn success(oid: String, size: u64, actions: HashMap<String, Action>) -> Self {
        Self {
            oid,
            size,
            authenticated: Some(true),
            actions: Some(actions),
            error: None,
        }
    }

    /// Create an error response
    pub fn error(oid: String, size: u64, code: u16, message: impl Into<String>) -> Self {
        Self {
            oid,
            size,
            authenticated: None,
            actions: None,
            error: Some(ObjectError {
                code,
                message: message.into(),
            }),
        }
    }

    /// Create a response indicating the object already exists (for upload)
    pub fn already_exists(oid: String, size: u64) -> Self {
        Self {
            oid,
            size,
            authenticated: Some(true),
            actions: Some(HashMap::new()),
            error: None,
        }
    }
}

impl From<SignedUrl> for Action {
    fn from(url: SignedUrl) -> Self {
        Self {
            href: url.href,
            header: url.headers.into_iter().collect(),
            expires_in: Some(url.expires_in),
            expires_at: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_request_deserialize() {
        let json = r#"{
            "operation": "upload",
            "objects": [
                {"oid": "abc123", "size": 1024}
            ],
            "ref": {"name": "refs/heads/main"}
        }"#;

        let req: BatchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.operation, Operation::Upload);
        assert_eq!(req.objects.len(), 1);
        assert_eq!(req.objects[0].oid, "abc123");
        assert_eq!(req.objects[0].size, 1024);
        assert_eq!(req.git_ref.unwrap().name, "refs/heads/main");
    }

    #[test]
    fn test_batch_response_serialize() {
        let mut actions = HashMap::new();
        actions.insert(
            "upload".to_string(),
            Action {
                href: "https://example.com/upload".to_string(),
                header: HashMap::new(),
                expires_in: Some(3600),
                expires_at: None,
            },
        );

        let resp = BatchResponse {
            transfer: "basic".to_string(),
            objects: vec![BatchResponseObject::success(
                "abc123".to_string(),
                1024,
                actions,
            )],
            hash_algo: None,
        };

        let json = serde_json::to_string_pretty(&resp).unwrap();
        assert!(json.contains("upload"));
        assert!(json.contains("https://example.com/upload"));
    }

    #[test]
    fn test_batch_object_validate_valid() {
        let obj = BatchObject {
            oid: "a".repeat(64),
            size: 1024,
        };
        assert!(obj.validate().is_ok());
    }

    #[test]
    fn test_batch_object_validate_invalid_oid_length() {
        let obj = BatchObject {
            oid: "abc123".to_string(),
            size: 1024,
        };
        assert!(obj.validate().is_err());
    }

    #[test]
    fn test_batch_object_validate_invalid_oid_chars() {
        let obj = BatchObject {
            oid: "g".repeat(64),
            size: 1024,
        };
        assert!(obj.validate().is_err());
    }

    #[test]
    fn test_batch_object_validate_size_too_large() {
        let obj = BatchObject {
            oid: "a".repeat(64),
            size: 6 * 1024 * 1024 * 1024,
        };
        assert!(obj.validate().is_err());
    }

    #[test]
    fn test_batch_request_validate() {
        let req = BatchRequest {
            operation: Operation::Upload,
            objects: vec![
                BatchObject {
                    oid: "a".repeat(64),
                    size: 1024,
                },
                BatchObject {
                    oid: "b".repeat(64),
                    size: 2048,
                },
            ],
            transfers: vec![],
            git_ref: None,
            hash_algo: None,
        };
        assert!(req.validate().is_ok());
    }
}
