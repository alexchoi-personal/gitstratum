use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "gitstratum-frontend")]
#[command(about = "GitStratum Frontend - Git protocol server")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:22")]
    ssh_addr: SocketAddr,

    #[arg(long, default_value = "0.0.0.0:443")]
    https_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:9000")]
    control_plane_addr: String,

    #[arg(long, default_value = "127.0.0.1:9001")]
    metadata_addr: String,

    #[arg(long, default_value = "127.0.0.1:9002")]
    object_addr: String,

    #[arg(long)]
    node_id: Option<String>,

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
        ssh_addr = %args.ssh_addr,
        https_addr = %args.https_addr,
        "Starting GitStratum Frontend"
    );

    tracing::info!(
        control_plane = %args.control_plane_addr,
        metadata = %args.metadata_addr,
        object = %args.object_addr,
        "Connecting to cluster services"
    );

    tracing::info!("Frontend server started successfully");

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down frontend server");

    Ok(())
}
