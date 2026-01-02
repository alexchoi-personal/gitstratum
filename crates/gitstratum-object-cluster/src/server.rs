use async_trait::async_trait;
use gitstratum_core::{Blob as CoreBlob, Oid};
use gitstratum_proto::object_service_server::ObjectService;
use gitstratum_proto::{
    Blob, DeleteBlobRequest, DeleteBlobResponse, GetBlobRequest, GetBlobResponse, GetBlobsRequest,
    GetStatsRequest, GetStatsResponse, HasBlobRequest, HasBlobResponse, PutBlobRequest,
    PutBlobResponse, PutBlobsResponse, ReceiveBlobsResponse, StreamBlobsRequest,
};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, instrument, warn};

use crate::error::ObjectStoreError;
use crate::store::ObjectStorage;
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

        match self.store.get(&oid).await.map_err(ObjectStoreError::from)? {
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

                match store.get(&oid).await {
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
            .await
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
                    Ok(core_blob) => match self.store.put(&core_blob).await {
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

        match self.store.delete(&oid).await {
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

    type StreamBlobsStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<Blob, Status>> + Send>>;

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
            #[cfg(feature = "bucketstore")]
            {
                let mut stream = store.iter_range(from_position, to_position);
                while let Some(result) = std::pin::Pin::new(&mut stream).next().await {
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
            }

            #[cfg(not(feature = "bucketstore"))]
            {
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
                        .await
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

    #[cfg(feature = "bucketstore")]
    async fn create_test_server() -> (ObjectServiceImpl, TempDir) {
        use gitstratum_storage::BucketStoreConfig;
        use std::time::Duration;

        let temp_dir = TempDir::new().unwrap();
        let config = BucketStoreConfig {
            data_dir: temp_dir.path().to_path_buf(),
            bucket_count: 64,
            bucket_cache_size: 16,
            max_data_file_size: 1024 * 1024,
            io_queue_depth: 4,
            io_queue_count: 1,
            compaction: gitstratum_storage::config::CompactionConfig {
                fragmentation_threshold: 0.4,
                check_interval: Duration::from_secs(300),
                max_concurrent: 1,
            },
        };
        let store = Arc::new(ObjectStore::new(config).await.unwrap());
        let server = ObjectServiceImpl::new(store);
        (server, temp_dir)
    }

    #[cfg(not(feature = "bucketstore"))]
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

    #[tokio::test]
    async fn test_get_blob_missing_oid() {
        let (server, _dir) = create_test_server().await;

        let get_request = Request::new(GetBlobRequest { oid: None });
        let result = server.get_blob(get_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_has_blob_missing_oid() {
        let (server, _dir) = create_test_server().await;

        let has_request = Request::new(HasBlobRequest { oid: None });
        let result = server.has_blob(has_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_blob_missing_oid() {
        let (server, _dir) = create_test_server().await;

        let delete_request = Request::new(DeleteBlobRequest { oid: None });
        let result = server.delete_blob(delete_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_blob_missing_blob() {
        let (server, _dir) = create_test_server().await;

        let put_request = Request::new(PutBlobRequest { blob: None });
        let result = server.put_blob(put_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_blob_missing_oid_in_blob() {
        let (server, _dir) = create_test_server().await;

        let put_request = Request::new(PutBlobRequest {
            blob: Some(Blob {
                oid: None,
                data: b"test".to_vec(),
                compressed: false,
            }),
        });
        let result = server.put_blob(put_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_proto_oid_to_core_invalid() {
        let invalid_oid = gitstratum_proto::Oid {
            bytes: vec![0u8; 16],
        };
        let result = ObjectServiceImpl::proto_oid_to_core(&invalid_oid);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_blobs_stream() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let blobs: Vec<CoreBlob> = (0..5)
            .map(|i| CoreBlob::new(format!("blob data {}", i).into_bytes()))
            .collect();

        for blob in &blobs {
            let proto_blob = ObjectServiceImpl::core_blob_to_proto(blob, false);
            let put_request = Request::new(PutBlobRequest {
                blob: Some(proto_blob),
            });
            server.put_blob(put_request).await.unwrap();
        }

        let oids: Vec<gitstratum_proto::Oid> = blobs
            .iter()
            .map(|b| ObjectServiceImpl::core_oid_to_proto(&b.oid))
            .collect();

        let get_blobs_request = Request::new(GetBlobsRequest { oids });
        let response = server.get_blobs(get_blobs_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_get_blobs_with_invalid_oid() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let invalid_oid = gitstratum_proto::Oid {
            bytes: vec![0u8; 16],
        };
        let get_blobs_request = Request::new(GetBlobsRequest {
            oids: vec![invalid_oid],
        });
        let response = server.get_blobs(get_blobs_request).await.unwrap();
        let mut stream = response.into_inner();

        while let Some(result) = stream.next().await {
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_stream_blobs() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let blobs: Vec<CoreBlob> = (0..3)
            .map(|i| CoreBlob::new(format!("stream blob {}", i).into_bytes()))
            .collect();

        for blob in &blobs {
            let proto_blob = ObjectServiceImpl::core_blob_to_proto(blob, false);
            let put_request = Request::new(PutBlobRequest {
                blob: Some(proto_blob),
            });
            server.put_blob(put_request).await.unwrap();
        }

        let stream_request = Request::new(StreamBlobsRequest {
            from_position: 0,
            to_position: u64::MAX,
        });
        let response = server.stream_blobs(stream_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_get_blobs_with_nonexistent_oid() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let oid = Oid::hash(b"nonexistent");
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&oid);
        let get_blobs_request = Request::new(GetBlobsRequest {
            oids: vec![proto_oid],
        });
        let response = server.get_blobs(get_blobs_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(_) = stream.next().await {
            count += 1;
        }
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_get_blobs_receiver_dropped() {
        let (server, _dir) = create_test_server().await;

        let blobs: Vec<CoreBlob> = (0..100)
            .map(|i| CoreBlob::new(format!("blob data {}", i).into_bytes()))
            .collect();

        for blob in &blobs {
            let proto_blob = ObjectServiceImpl::core_blob_to_proto(blob, false);
            let put_request = Request::new(PutBlobRequest {
                blob: Some(proto_blob),
            });
            server.put_blob(put_request).await.unwrap();
        }

        let oids: Vec<gitstratum_proto::Oid> = blobs
            .iter()
            .map(|b| ObjectServiceImpl::core_oid_to_proto(&b.oid))
            .collect();

        let get_blobs_request = Request::new(GetBlobsRequest { oids });
        let response = server.get_blobs(get_blobs_request).await.unwrap();
        drop(response);

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_stream_blobs_receiver_dropped() {
        let (server, _dir) = create_test_server().await;

        let blobs: Vec<CoreBlob> = (0..100)
            .map(|i| CoreBlob::new(format!("stream blob {}", i).into_bytes()))
            .collect();

        for blob in &blobs {
            let proto_blob = ObjectServiceImpl::core_blob_to_proto(blob, false);
            let put_request = Request::new(PutBlobRequest {
                blob: Some(proto_blob),
            });
            server.put_blob(put_request).await.unwrap();
        }

        let stream_request = Request::new(StreamBlobsRequest {
            from_position: 0,
            to_position: u64::MAX,
        });
        let response = server.stream_blobs(stream_request).await.unwrap();
        drop(response);

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_delete_nonexistent_blob() {
        let (server, _dir) = create_test_server().await;

        let oid = Oid::hash(b"nonexistent delete");
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&oid);

        let delete_request = Request::new(DeleteBlobRequest {
            oid: Some(proto_oid),
        });
        let delete_response = server.delete_blob(delete_request).await.unwrap();
        let inner = delete_response.into_inner();
        assert!(inner.success);
    }

    #[tokio::test]
    async fn test_get_blob_invalid_oid() {
        let (server, _dir) = create_test_server().await;

        let invalid_oid = gitstratum_proto::Oid {
            bytes: vec![0u8; 10],
        };

        let get_request = Request::new(GetBlobRequest {
            oid: Some(invalid_oid),
        });
        let result = server.get_blob(get_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_has_blob_invalid_oid() {
        let (server, _dir) = create_test_server().await;

        let invalid_oid = gitstratum_proto::Oid {
            bytes: vec![0u8; 10],
        };

        let has_request = Request::new(HasBlobRequest {
            oid: Some(invalid_oid),
        });
        let result = server.has_blob(has_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_blob_invalid_oid() {
        let (server, _dir) = create_test_server().await;

        let invalid_oid = gitstratum_proto::Oid {
            bytes: vec![0u8; 10],
        };

        let delete_request = Request::new(DeleteBlobRequest {
            oid: Some(invalid_oid),
        });
        let result = server.delete_blob(delete_request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_core_blob_to_proto_compressed_flag() {
        let blob = CoreBlob::new(b"test data".to_vec());

        let proto_uncompressed = ObjectServiceImpl::core_blob_to_proto(&blob, false);
        assert!(!proto_uncompressed.compressed);

        let proto_compressed = ObjectServiceImpl::core_blob_to_proto(&blob, true);
        assert!(proto_compressed.compressed);
    }

    #[tokio::test]
    async fn test_get_blobs_mixed_valid_and_nonexistent() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let blob = CoreBlob::new(b"existing blob".to_vec());
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);
        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let existing_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let nonexistent_oid = ObjectServiceImpl::core_oid_to_proto(&Oid::hash(b"nonexistent"));

        let get_blobs_request = Request::new(GetBlobsRequest {
            oids: vec![existing_oid, nonexistent_oid],
        });
        let response = server.get_blobs(get_blobs_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_stream_blobs_partial_range() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let blobs: Vec<CoreBlob> = (0..5)
            .map(|i| CoreBlob::new(format!("range blob {}", i).into_bytes()))
            .collect();

        for blob in &blobs {
            let proto_blob = ObjectServiceImpl::core_blob_to_proto(blob, false);
            let put_request = Request::new(PutBlobRequest {
                blob: Some(proto_blob),
            });
            server.put_blob(put_request).await.unwrap();
        }

        let stream_request = Request::new(StreamBlobsRequest {
            from_position: u64::MAX / 2,
            to_position: u64::MAX,
        });
        let response = server.stream_blobs(stream_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
            count += 1;
        }
        assert!(count <= 5);
    }

    #[tokio::test]
    async fn test_stream_blobs_empty_range() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let blob = CoreBlob::new(b"test blob".to_vec());
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);
        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let stream_request = Request::new(StreamBlobsRequest {
            from_position: 0,
            to_position: 0,
        });
        let response = server.stream_blobs(stream_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(_) = stream.next().await {
            count += 1;
        }
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_proto_blob_to_core_missing_oid() {
        let blob = Blob {
            oid: None,
            data: b"test".to_vec(),
            compressed: false,
        };
        let result = ObjectServiceImpl::proto_blob_to_core(&blob);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_proto_blob_to_core_success() {
        let core_blob = CoreBlob::new(b"test data".to_vec());
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&core_blob, false);

        let result = ObjectServiceImpl::proto_blob_to_core(&proto_blob);
        assert!(result.is_ok());
        let restored = result.unwrap();
        assert_eq!(restored.oid, core_blob.oid);
        assert_eq!(restored.data.as_ref(), core_blob.data.as_ref());
    }

    #[tokio::test]
    async fn test_proto_blob_to_core_invalid_oid() {
        let blob = Blob {
            oid: Some(gitstratum_proto::Oid {
                bytes: vec![0u8; 5],
            }),
            data: b"test".to_vec(),
            compressed: false,
        };
        let result = ObjectServiceImpl::proto_blob_to_core(&blob);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_blobs_with_mixed_valid_invalid_nonexistent() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let existing_blob = CoreBlob::new(b"existing".to_vec());
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&existing_blob, false);
        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let existing_oid = ObjectServiceImpl::core_oid_to_proto(&existing_blob.oid);
        let nonexistent_oid = ObjectServiceImpl::core_oid_to_proto(&Oid::hash(b"nonexistent"));
        let invalid_oid = gitstratum_proto::Oid {
            bytes: vec![0u8; 5],
        };

        let get_blobs_request = Request::new(GetBlobsRequest {
            oids: vec![existing_oid, nonexistent_oid, invalid_oid],
        });
        let response = server.get_blobs(get_blobs_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut success_count = 0;
        let mut error_count = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
        }
        assert_eq!(success_count, 1);
        assert_eq!(error_count, 1);
    }

    #[tokio::test]
    async fn test_get_stats_with_multiple_blobs() {
        let (server, _dir) = create_test_server().await;

        for i in 0..10 {
            let blob = CoreBlob::new(format!("blob data {}", i).into_bytes());
            let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);
            let put_request = Request::new(PutBlobRequest {
                blob: Some(proto_blob),
            });
            server.put_blob(put_request).await.unwrap();
        }

        let stats_request = Request::new(GetStatsRequest {});
        let stats_response = server.get_stats(stats_request).await.unwrap();
        let stats = stats_response.into_inner();

        assert_eq!(stats.total_blobs, 10);
        assert!(stats.total_bytes > 0);
    }

    #[tokio::test]
    async fn test_delete_blob_and_verify_removed() {
        let (server, _dir) = create_test_server().await;

        let blob = CoreBlob::new(b"to be deleted and verified".to_vec());
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);

        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let get_request = Request::new(GetBlobRequest {
            oid: Some(proto_oid.clone()),
        });
        let response = server.get_blob(get_request).await.unwrap();
        assert!(response.into_inner().found);

        let delete_request = Request::new(DeleteBlobRequest {
            oid: Some(proto_oid.clone()),
        });
        server.delete_blob(delete_request).await.unwrap();

        let get_request = Request::new(GetBlobRequest {
            oid: Some(proto_oid),
        });
        let response = server.get_blob(get_request).await.unwrap();
        assert!(!response.into_inner().found);
    }

    #[tokio::test]
    async fn test_get_blob_data_integrity() {
        let (server, _dir) = create_test_server().await;

        let test_data = b"The quick brown fox jumps over the lazy dog";
        let blob = CoreBlob::new(test_data.to_vec());
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);

        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let get_request = Request::new(GetBlobRequest {
            oid: Some(proto_oid),
        });
        let get_response = server.get_blob(get_request).await.unwrap();
        let inner = get_response.into_inner();

        assert!(inner.found);
        let retrieved_blob = inner.blob.unwrap();
        assert_eq!(retrieved_blob.data, test_data);
        assert!(!retrieved_blob.compressed);
        assert!(retrieved_blob.oid.is_some());
        assert_eq!(retrieved_blob.oid.unwrap().bytes.len(), 32);
    }

    #[tokio::test]
    async fn test_get_blob_empty_data() {
        let (server, _dir) = create_test_server().await;

        let blob = CoreBlob::new(vec![]);
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);

        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let get_request = Request::new(GetBlobRequest {
            oid: Some(proto_oid),
        });
        let get_response = server.get_blob(get_request).await.unwrap();
        let inner = get_response.into_inner();

        assert!(inner.found);
        assert!(inner.blob.unwrap().data.is_empty());
    }

    #[tokio::test]
    async fn test_put_get_large_blob() {
        let (server, _dir) = create_test_server().await;

        let large_data: Vec<u8> = (0..1024 * 100).map(|i| (i % 256) as u8).collect();
        let blob = CoreBlob::new(large_data.clone());
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
        assert_eq!(inner.blob.unwrap().data, large_data);
    }

    #[tokio::test]
    async fn test_get_blobs_empty_request() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let get_blobs_request = Request::new(GetBlobsRequest { oids: vec![] });
        let response = server.get_blobs(get_blobs_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(_) = stream.next().await {
            count += 1;
        }
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_get_blobs_all_nonexistent() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let nonexistent_oids: Vec<gitstratum_proto::Oid> = (0..5)
            .map(|i| {
                ObjectServiceImpl::core_oid_to_proto(&Oid::hash(
                    format!("nonexistent_{}", i).as_bytes(),
                ))
            })
            .collect();

        let get_blobs_request = Request::new(GetBlobsRequest {
            oids: nonexistent_oids,
        });
        let response = server.get_blobs(get_blobs_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(_) = stream.next().await {
            count += 1;
        }
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_multiple_puts_same_blob() {
        let (server, _dir) = create_test_server().await;

        let blob = CoreBlob::new(b"same data".to_vec());
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);

        for _ in 0..5 {
            let put_request = Request::new(PutBlobRequest {
                blob: Some(proto_blob.clone()),
            });
            let put_response = server.put_blob(put_request).await.unwrap();
            assert!(put_response.into_inner().success);
        }

        let stats_request = Request::new(GetStatsRequest {});
        let stats_response = server.get_stats(stats_request).await.unwrap();
        assert_eq!(stats_response.into_inner().total_blobs, 1);

        let has_request = Request::new(HasBlobRequest {
            oid: Some(proto_oid),
        });
        let has_response = server.has_blob(has_request).await.unwrap();
        assert!(has_response.into_inner().exists);
    }

    #[tokio::test]
    async fn test_stream_blobs_with_data() {
        use tokio_stream::StreamExt;

        let (server, _dir) = create_test_server().await;

        let blobs: Vec<CoreBlob> = (0..10)
            .map(|i| CoreBlob::new(format!("stream test blob {}", i).into_bytes()))
            .collect();

        for blob in &blobs {
            let proto_blob = ObjectServiceImpl::core_blob_to_proto(blob, false);
            let put_request = Request::new(PutBlobRequest {
                blob: Some(proto_blob),
            });
            server.put_blob(put_request).await.unwrap();
        }

        let stream_request = Request::new(StreamBlobsRequest {
            from_position: 0,
            to_position: u64::MAX,
        });
        let response = server.stream_blobs(stream_request).await.unwrap();
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_core_oid_to_proto_roundtrip() {
        let original_oid = Oid::hash(b"test data for roundtrip");
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&original_oid);

        assert_eq!(proto_oid.bytes.len(), 32);
        assert_eq!(proto_oid.bytes, original_oid.as_bytes());
    }

    #[test]
    fn test_proto_oid_to_core_valid() {
        let original_oid = Oid::hash(b"valid oid test");
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&original_oid);

        let result = ObjectServiceImpl::proto_oid_to_core(&proto_oid);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), original_oid);
    }

    #[test]
    fn test_proto_oid_to_core_empty() {
        let empty_oid = gitstratum_proto::Oid { bytes: vec![] };
        let result = ObjectServiceImpl::proto_oid_to_core(&empty_oid);
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_oid_to_core_too_short() {
        let short_oid = gitstratum_proto::Oid {
            bytes: vec![0u8; 31],
        };
        let result = ObjectServiceImpl::proto_oid_to_core(&short_oid);
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_oid_to_core_too_long() {
        let long_oid = gitstratum_proto::Oid {
            bytes: vec![0u8; 33],
        };
        let result = ObjectServiceImpl::proto_oid_to_core(&long_oid);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_stats_after_put_and_delete() {
        let (server, _dir) = create_test_server().await;

        let stats_request = Request::new(GetStatsRequest {});
        let stats_response = server.get_stats(stats_request).await.unwrap();
        assert_eq!(stats_response.into_inner().total_blobs, 0);

        let blob = CoreBlob::new(b"stats test blob".to_vec());
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);

        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let stats_request = Request::new(GetStatsRequest {});
        let stats_response = server.get_stats(stats_request).await.unwrap();
        let stats = stats_response.into_inner();
        assert_eq!(stats.total_blobs, 1);
        assert!(stats.total_bytes > 0);

        let delete_request = Request::new(DeleteBlobRequest {
            oid: Some(proto_oid),
        });
        server.delete_blob(delete_request).await.unwrap();

        let stats_request = Request::new(GetStatsRequest {});
        let stats_response = server.get_stats(stats_request).await.unwrap();
        assert_eq!(stats_response.into_inner().total_blobs, 0);
    }

    #[tokio::test]
    async fn test_binary_data_preservation() {
        let (server, _dir) = create_test_server().await;

        let binary_data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let blob = CoreBlob::new(binary_data.clone());
        let proto_oid = ObjectServiceImpl::core_oid_to_proto(&blob.oid);
        let proto_blob = ObjectServiceImpl::core_blob_to_proto(&blob, false);

        let put_request = Request::new(PutBlobRequest {
            blob: Some(proto_blob),
        });
        server.put_blob(put_request).await.unwrap();

        let get_request = Request::new(GetBlobRequest {
            oid: Some(proto_oid),
        });
        let get_response = server.get_blob(get_request).await.unwrap();
        let inner = get_response.into_inner();

        assert!(inner.found);
        assert_eq!(inner.blob.unwrap().data, binary_data);
    }
}
