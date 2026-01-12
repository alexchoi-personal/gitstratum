use std::sync::Arc;
use tokio::runtime::Handle;
use tonic::transport::Channel;

use gitstratum_proto::auth_service_client::AuthServiceClient;
use gitstratum_proto::{
    GetUserRequest, UserStatusProto, ValidateSshKeyRequest, ValidateTokenRequest,
};

use super::error::AuthError;
use super::types::{SshKey, StoredToken, TokenScopes, User, UserStatus};
use super::validator::AuthStore;

pub struct GrpcAuthStore {
    client: AuthServiceClient<Channel>,
}

impl GrpcAuthStore {
    pub async fn connect(addr: &str) -> Result<Self, AuthError> {
        let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };

        let channel = Channel::from_shared(endpoint)
            .map_err(|e| AuthError::Internal(format!("invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| AuthError::Internal(format!("failed to connect: {}", e)))?;

        Ok(Self {
            client: AuthServiceClient::new(channel),
        })
    }

    pub async fn validate_token(
        &self,
        token: &str,
    ) -> Result<Option<(String, TokenScopes)>, AuthError> {
        let request = ValidateTokenRequest {
            token: token.to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .validate_token(request)
            .await
            .map_err(|e| AuthError::Internal(format!("grpc error: {}", e)))?
            .into_inner();

        if response.valid {
            Ok(Some((
                response.user_id,
                TokenScopes {
                    read: response.scope_read,
                    write: response.scope_write,
                    admin: response.scope_admin,
                },
            )))
        } else {
            Ok(None)
        }
    }

    pub async fn validate_ssh_key(&self, fingerprint: &str) -> Result<Option<SshKey>, AuthError> {
        let request = ValidateSshKeyRequest {
            fingerprint: fingerprint.to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .validate_ssh_key(request)
            .await
            .map_err(|e| AuthError::Internal(format!("grpc error: {}", e)))?
            .into_inner();

        if response.valid {
            Ok(Some(SshKey {
                key_id: String::new(),
                user_id: response.user_id,
                fingerprint: fingerprint.to_string(),
                public_key: String::new(),
                title: String::new(),
                scopes: TokenScopes {
                    read: response.scope_read,
                    write: response.scope_write,
                    admin: response.scope_admin,
                },
                created_at: 0,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_user(&self, user_id: &str) -> Result<Option<User>, AuthError> {
        let request = GetUserRequest {
            user_id: user_id.to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .get_user(request)
            .await
            .map_err(|e| AuthError::Internal(format!("grpc error: {}", e)))?
            .into_inner();

        if response.found {
            let status = match response.status() {
                UserStatusProto::UserStatusActive => UserStatus::Active,
                UserStatusProto::UserStatusDisabled => UserStatus::Disabled,
                UserStatusProto::UserStatusUnknown => UserStatus::Active,
            };

            Ok(Some(User {
                user_id: response.user_id,
                name: response.name,
                email: response.email,
                password_hash: String::new(),
                status,
                created_at: response.created_at,
            }))
        } else {
            Ok(None)
        }
    }
}

pub struct BlockingGrpcAuthStore {
    inner: Arc<GrpcAuthStore>,
    handle: Handle,
}

impl BlockingGrpcAuthStore {
    pub fn new(inner: Arc<GrpcAuthStore>, handle: Handle) -> Self {
        Self { inner, handle }
    }
}

impl AuthStore for BlockingGrpcAuthStore {
    fn get_token_by_hash(&self, hash: &str) -> Result<Option<StoredToken>, AuthError> {
        let inner = Arc::clone(&self.inner);
        let hash_owned = hash.to_string();

        let result = tokio::task::block_in_place(|| {
            self.handle
                .block_on(async move { inner.validate_token(&hash_owned).await })
        });

        match result {
            Ok(Some((user_id, scopes))) => Ok(Some(StoredToken {
                token_id: String::new(),
                user_id,
                hashed_value: hash.to_string(),
                scopes,
                created_at: 0,
                expires_at: None,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn get_user(&self, user_id: &str) -> Result<Option<User>, AuthError> {
        let inner = Arc::clone(&self.inner);
        let user_id = user_id.to_string();

        tokio::task::block_in_place(|| {
            self.handle
                .block_on(async move { inner.get_user(&user_id).await })
        })
    }

    fn get_ssh_key(&self, fingerprint: &str) -> Result<Option<SshKey>, AuthError> {
        let inner = Arc::clone(&self.inner);
        let fingerprint = fingerprint.to_string();

        tokio::task::block_in_place(|| {
            self.handle
                .block_on(async move { inner.validate_ssh_key(&fingerprint).await })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blocking_grpc_auth_store_get_user() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let handle = Handle::current();

            #[allow(dead_code)]
            struct MockGrpcAuthStore;

            #[allow(dead_code)]
            impl MockGrpcAuthStore {
                async fn validate_token(
                    &self,
                    _token: &str,
                ) -> Result<Option<(String, TokenScopes)>, AuthError> {
                    Ok(None)
                }

                async fn validate_ssh_key(
                    &self,
                    _fingerprint: &str,
                ) -> Result<Option<String>, AuthError> {
                    Ok(None)
                }
            }

            #[allow(dead_code)]
            struct TestBlockingStore {
                handle: Handle,
            }

            impl AuthStore for TestBlockingStore {
                fn get_token_by_hash(&self, _hash: &str) -> Result<Option<StoredToken>, AuthError> {
                    Ok(None)
                }

                fn get_user(&self, user_id: &str) -> Result<Option<User>, AuthError> {
                    Ok(Some(User {
                        user_id: user_id.to_string(),
                        name: String::new(),
                        email: String::new(),
                        password_hash: String::new(),
                        status: UserStatus::Active,
                        created_at: 0,
                    }))
                }

                fn get_ssh_key(&self, _fingerprint: &str) -> Result<Option<SshKey>, AuthError> {
                    Ok(None)
                }
            }

            let store = TestBlockingStore { handle };
            let user = store.get_user("test_user").unwrap();
            assert!(user.is_some());
            assert_eq!(user.unwrap().user_id, "test_user");
        });
    }

    #[test]
    fn test_blocking_grpc_auth_store_get_token_by_hash_none() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            struct TestBlockingStore;

            impl AuthStore for TestBlockingStore {
                fn get_token_by_hash(&self, _hash: &str) -> Result<Option<StoredToken>, AuthError> {
                    Ok(None)
                }

                fn get_user(&self, _user_id: &str) -> Result<Option<User>, AuthError> {
                    Ok(None)
                }

                fn get_ssh_key(&self, _fingerprint: &str) -> Result<Option<SshKey>, AuthError> {
                    Ok(None)
                }
            }

            let store = TestBlockingStore;
            let result = store.get_token_by_hash("some_hash").unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_blocking_grpc_auth_store_get_ssh_key_none() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            struct TestBlockingStore;

            impl AuthStore for TestBlockingStore {
                fn get_token_by_hash(&self, _hash: &str) -> Result<Option<StoredToken>, AuthError> {
                    Ok(None)
                }

                fn get_user(&self, _user_id: &str) -> Result<Option<User>, AuthError> {
                    Ok(None)
                }

                fn get_ssh_key(&self, _fingerprint: &str) -> Result<Option<SshKey>, AuthError> {
                    Ok(None)
                }
            }

            let store = TestBlockingStore;
            let result = store.get_ssh_key("SHA256:test").unwrap();
            assert!(result.is_none());
        });
    }
}
