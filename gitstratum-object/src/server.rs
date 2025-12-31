use async_trait::async_trait;
use gitstratum_core::{Blob as CoreBlob, Oid};
use gitstratum_proto::object_service_server::ObjectService;
use gitstratum_proto::{
    Blob, DeleteBlobRequest, DeleteBlobResponse, GetBlobRequest, GetBlobResponse,
    GetBlobsRequest, GetStatsRequest, GetStatsResponse, HasBlobRequest, HasBlobResponse,
    PutBlobRequest, PutBlobResponse, PutBlobsResponse, ReceiveBlobsResponse, StreamBlobsRequest,
};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, instrument, warn};

use crate::error::ObjectStoreError;
use crate::store::ObjectStore;

pub struct ObjectServiceImpl {
    store: Arc<ObjectStore>,
}

impl ObjectServiceImpl {
    pub fn new(store: Arc<ObjectStore>) -> Self {
        Self { store }
    }

    fn proto_oid_to_core(oid: &gitstratum_proto::Oid) -> Result<Oid, Status> {
        Oid::from_slice(&oid.bytes)
            .map_err(|e| Status::invalid_argument(format!("invalid oid: {}", e)))
    }

    fn core_oid_to_proto(oid: &Oid) -> gitstratum_proto::Oid {
        gitstratum_proto::Oid {
            bytes: oid.as_bytes().to_vec(),
        }
    }

    fn proto_blob_to_core(blob: &Blob) -> Result<CoreBlob, Status> {
        let oid = blob
            .oid
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing oid"))?;
        let oid = Self::proto_oid_to_core(oid)?;
        Ok(CoreBlob::with_oid(oid, blob.data.clone()))
    }

    fn core_blob_to_proto(blob: &CoreBlob, compressed: bool) -> Blob {
        Blob {
            oid: Some(Self::core_oid_to_proto(&blob.oid)),
            data: blob.data.to_vec(),
            compressed,
        }
    }
}

#[async_trait]
impl ObjectService for ObjectServiceImpl {
    #[instrument(skip(self, request))]
    async fn get_blob(
        &self,
        request: Request<GetBlobRequest>,
    ) -> Result<Response<GetBlobResponse>, Status> {
        let req = request.into_inner();
        let oid = req
            .oid
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing oid"))?;
        let oid = Self::proto_oid_to_core(oid)?;

        debug!(%oid, "get_blob");

        match self.store.get(&oid).map_err(ObjectStoreError::from)? {
            Some(blob) => Ok(Response::new(GetBlobResponse {
                blob: Some(Self::core_blob_to_proto(&blob, false)),
                found: true,
            })),
            None => Ok(Response::new(GetBlobResponse {
                blob: None,
                found: false,
            })),
        }
    }

    type GetBlobsStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<Blob, Status>> + Send>>;

    #[instrument(skip(self, request))]
    async fn get_blobs(
        &self,
        request: Request<GetBlobsRequest>,
    ) -> Result<Response<Self::GetBlobsStream>, Status> {
        let req = request.into_inner();
        let store = self.store.clone();

        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            for proto_oid in req.oids {
                let oid = match Self::proto_oid_to_core(&proto_oid) {
                    Ok(oid) => oid,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        continue;
                    }
                };

                match store.get(&oid) {
                    Ok(Some(blob)) => {
                        let proto_blob = Self::core_blob_to_proto(&blob, false);
                        if tx.send(Ok(proto_blob)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(%oid, error = %e, "failed to get blob");
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    #[instrument(skip(self, request))]
    async fn put_blob(
        &self,
        request: Request<PutBlobRequest>,
    ) -> Result<Response<PutBlobResponse>, Status> {
        let req = request.into_inner();
        let blob = req
            .blob
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing blob"))?;
        let core_blob = Self::proto_blob_to_core(blob)?;

        debug!(oid = %core_blob.oid, "put_blob");

        self.store
            .put(&core_blob)
            .map_err(ObjectStoreError::from)?;

        Ok(Response::new(PutBlobResponse {
            success: true,
            error: String::new(),
        }))
    }

    #[instrument(skip(self, request))]
    async fn put_blobs(
        &self,
        request: Request<Streaming<Blob>>,
    ) -> Result<Response<PutBlobsResponse>, Status> {
        let mut stream = request.into_inner();
        let mut success_count = 0u32;
        let mut error_count = 0u32;
        let mut errors = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                Ok(blob) => match Self::proto_blob_to_core(&blob) {
                    Ok(core_blob) => match self.store.put(&core_blob) {
                        Ok(()) => success_count += 1,
                        Err(e) => {
                            error_count += 1;
                            errors.push(e.to_string());
                        }
                    },
                    Err(e) => {
                        error_count += 1;
                        errors.push(e.to_string());
                    }
                },
                Err(e) => {
                    error_count += 1;
                    errors.push(e.to_string());
                }
            }
        }

        Ok(Response::new(PutBlobsResponse {
            success_count,
            error_count,
            errors,
        }))
    }

    #[instrument(skip(self, request))]
    async fn has_blob(
        &self,
        request: Request<HasBlobRequest>,
    ) -> Result<Response<HasBlobResponse>, Status> {
        let req = request.into_inner();
        let oid = req
            .oid
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing oid"))?;
        let oid = Self::proto_oid_to_core(oid)?;

        debug!(%oid, "has_blob");

        let exists = self.store.has(&oid);
        Ok(Response::new(HasBlobResponse { exists }))
    }

    #[instrument(skip(self, request))]
    async fn delete_blob(
        &self,
        request: Request<DeleteBlobRequest>,
    ) -> Result<Response<DeleteBlobResponse>, Status> {
        let req = request.into_inner();
        let oid = req
            .oid
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing oid"))?;
        let oid = Self::proto_oid_to_core(oid)?;

        debug!(%oid, "delete_blob");

        match self.store.delete(&oid) {
            Ok(_) => Ok(Response::new(DeleteBlobResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(DeleteBlobResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    type StreamBlobsStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<Blob, Status>> + Send>>;

    #[instrument(skip(self, request))]
    async fn stream_blobs(
        &self,
        request: Request<StreamBlobsRequest>,
    ) -> Result<Response<Self::StreamBlobsStream>, Status> {
        let req = request.into_inner();
        let store = self.store.clone();
        let from_position = req.from_position;
        let to_position = req.to_position;

        debug!(from_position, to_position, "stream_blobs");

        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            for result in store.iter_range(from_position, to_position) {
                match result {
                    Ok((_, blob)) => {
                        let proto_blob = Self::core_blob_to_proto(&blob, false);
                        if tx.send(Ok(proto_blob)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    #[instrument(skip(self, request))]
    async fn receive_blobs(
        &self,
        request: Request<Streaming<Blob>>,
    ) -> Result<Response<ReceiveBlobsResponse>, Status> {
        let mut stream = request.into_inner();
        let mut received_count = 0u32;

        while let Some(result) = stream.next().await {
            match result {
                Ok(blob) => {
                    let core_blob = Self::proto_blob_to_core(&blob)?;
                    self.store
                        .put(&core_blob)
                        .map_err(ObjectStoreError::from)?;
                    received_count += 1;
                }
                Err(e) => {
                    return Ok(Response::new(ReceiveBlobsResponse {
                        received_count,
                        error: e.to_string(),
                    }));
                }
            }
        }

        Ok(Response::new(ReceiveBlobsResponse {
            received_count,
            error: String::new(),
        }))
    }

    #[instrument(skip(self, _request))]
    async fn get_stats(
        &self,
        _request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        debug!("get_stats");

        let stats = self.store.stats();
        Ok(Response::new(GetStatsResponse {
            total_blobs: stats.total_blobs,
            total_bytes: stats.total_bytes,
            used_bytes: stats.used_bytes,
            available_bytes: stats.available_bytes,
            io_utilization: stats.io_utilization,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_server() -> (ObjectServiceImpl, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(ObjectStore::new(temp_dir.path()).unwrap());
        let server = ObjectServiceImpl::new(store);
        (server, temp_dir)
    }

    #[tokio::test]
    async fn test_put_get_blob() {
        let (server, _dir) = create_test_server().await;

        let blob = CoreBlob::new(b"test data".to_vec());
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);

        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        let put_response = server.put_blob(put_request).await.unwrap();
        assert!(put_response.into_inner().success);

        let get_request = Request::new(GetBlobRequest {
            oid: Some(proto_oid),
        });
        let get_response = server.get_blob(get_request).await.unwrap();
        let inner = get_response.into_inner();
        assert!(inner.found);
        assert_eq!(inner.blob.unwrap().data, b"test data");
    }

    #[tokio::test]
    async fn test_has_blob() {
        let (server, _dir) = create_test_server().await;

        let blob = CoreBlob::new(b"test".to_vec());
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);

        let has_request = Request::new(HasBlobRequest {
            oid: Some(proto_oid.clone()),
        });
        let has_response = server.has_blob(has_request).await.unwrap();
        assert!(!has_response.into_inner().exists);

        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);
        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let has_request = Request::new(HasBlobRequest {
            oid: Some(proto_oid),
        });
        let has_response = server.has_blob(has_request).await.unwrap();
        assert!(has_response.into_inner().exists);
    }

    #[tokio::test]
    async fn test_delete_blob() {
        let (server, _dir) = create_test_server().await;

        let blob = CoreBlob::new(b"to delete".to_vec());
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);

        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let delete_request = Request::new(DeleteBlobRequest {
            oid: Some(proto_oid.clone()),
        });
        let delete_response = server.delete_blob(delete_request).await.unwrap();
        assert!(delete_response.into_inner().success);

        let has_request = Request::new(HasBlobRequest {
            oid: Some(proto_oid),
        });
        let has_response = server.has_blob(has_request).await.unwrap();
        assert!(!has_response.into_inner().exists);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let (server, _dir) = create_test_server().await;

        let stats_request = Request::new(GetStatsRequest {});
        let stats_response = server.get_stats(stats_request).await.unwrap();
        let stats = stats_response.into_inner();
        assert_eq!(stats.total_blobs, 0);

        let blob = CoreBlob::new(b"blob data".to_vec());
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);
        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let stats_request = Request::new(GetStatsRequest {});
        let stats_response = server.get_stats(stats_request).await.unwrap();
        let stats = stats_response.into_inner();
        assert_eq!(stats.total_blobs, 1);
    }

    #[tokio::test]
    async fn test_get_nonexistent_blob() {
        let (server, _dir) = create_test_server().await;

        let oid = Oid::hash(b"nonexistent");
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&oid);

        let get_request = Request::new(GetBlobRequest {
            oid: Some(proto_oid),
        });
        let get_response = server.get_blob(get_request).await.unwrap();
        let inner = get_response.into_inner();
        assert!(!inner.found);
        assert!(inner.blob.is_none());
    }
}
