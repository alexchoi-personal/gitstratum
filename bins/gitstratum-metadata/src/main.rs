use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "gitstratum-metadata")]
#[command(about = "GitStratum Metadata Store - Refs, repos, and pack cache")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:9001")]
    grpc_addr: SocketAddr,

    #[arg(long, default_value = "./data/metadata")]
    data_dir: PathBuf,

    #[arg(long)]
    node_id: Option<String>,

    #[arg(long, default_value = "127.0.0.1:9000")]
    control_plane_addr: String,

    #[arg(long, default_value = "0.0.0.0:9090")]
    metrics_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let _metrics = gitstratum_metrics::init_metrics(args.metrics_addr)
        .map_err(|e| anyhow::anyhow!("metrics init failed: {}", e))?;
    tracing::info!(%args.metrics_addr, "Metrics server started");

    let node_id = args
        .node_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    tracing::info!(
        node_id,
        grpc_addr = %args.grpc_addr,
        data_dir = %args.data_dir.display(),
        "Starting GitStratum Metadata Store"
    );

    std::fs::create_dir_all(&args.data_dir)?;

    let store = Arc::new(gitstratum_metadata_cluster::MetadataStore::open(
        &args.data_dir,
    )?);
    let service = gitstratum_metadata_cluster::MetadataServiceImpl::new(store);

    tracing::info!("RocksDB store initialized");
    tracing::info!(control_plane = %args.control_plane_addr, "Registering with control plane");

    tracing::info!("Starting gRPC server");
    tonic::transport::Server::builder()
        .add_service(gitstratum_proto::metadata_service_server::MetadataServiceServer::new(service))
        .serve_with_shutdown(args.grpc_addr, shutdown_signal())
        .await?;

    tracing::info!("Server shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("Received SIGTERM"),
        _ = sigint.recv() => tracing::info!("Received SIGINT"),
    }
}
