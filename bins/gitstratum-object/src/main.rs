use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "gitstratum-object")]
#[command(about = "GitStratum Object Store - Blob storage and GC")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:9002")]
    grpc_addr: SocketAddr,

    #[arg(long, default_value = "./data/objects")]
    data_dir: PathBuf,

    #[arg(long)]
    node_id: Option<String>,

    #[arg(long, default_value = "127.0.0.1:9000")]
    control_plane_addr: String,

    #[arg(long, default_value = "0 2 * * *")]
    gc_schedule: String,

    #[arg(long, default_value = "1024")]
    bucket_count: u32,

    #[arg(long, default_value = "1073741824")]
    max_data_file_size: u64,

    #[arg(long, default_value = "64")]
    bucket_cache_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let node_id = args
        .node_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    println!(
        "[INFO] Starting GitStratum Object Store (node_id={}, grpc={}, data_dir={})",
        node_id,
        args.grpc_addr,
        args.data_dir.display()
    );

    std::fs::create_dir_all(&args.data_dir)?;

    let store = create_store(&args).await?;
    let service = gitstratum_object_cluster::ObjectServiceImpl::new(store.clone());

    println!("[INFO] Object store initialized");

    println!(
        "[INFO] Registering with control plane: {}",
        args.control_plane_addr
    );

    println!("[INFO] GC scheduler configured: {}", args.gc_schedule);

    println!("[INFO] Starting gRPC server");
    tonic::transport::Server::builder()
        .add_service(gitstratum_proto::object_service_server::ObjectServiceServer::new(service))
        .serve(args.grpc_addr)
        .await?;

    Ok(())
}

#[cfg(feature = "bucketstore")]
async fn create_store(args: &Args) -> Result<Arc<gitstratum_object_cluster::ObjectStore>> {
    use gitstratum_object_cluster::BucketStoreConfig;

    let config = BucketStoreConfig {
        data_dir: args.data_dir.clone(),
        bucket_count: args.bucket_count,
        bucket_cache_size: args.bucket_cache_size,
        max_data_file_size: args.max_data_file_size,
        io_queue_depth: 64,
        io_queue_count: 2,
        compaction: gitstratum_storage::config::CompactionConfig {
            fragmentation_threshold: 0.4,
            check_interval: Duration::from_secs(300),
            max_concurrent: 1,
        },
    };

    let store = gitstratum_object_cluster::ObjectStore::new(config).await?;
    Ok(Arc::new(store))
}

#[cfg(not(feature = "bucketstore"))]
async fn create_store(args: &Args) -> Result<Arc<gitstratum_object_cluster::ObjectStore>> {
    let store = gitstratum_object_cluster::ObjectStore::new(&args.data_dir)?;
    Ok(Arc::new(store))
}
