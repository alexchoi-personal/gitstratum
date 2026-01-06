use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Handle;
use tracing_subscriber::EnvFilter;

use gitstratum_frontend_cluster::auth::{
    AuthRateLimiter, BlockingGrpcAuthStore, GrpcAuthStore, LocalValidator,
};
use gitstratum_frontend_cluster::ssh::{SshServer, SshServerConfig};

#[derive(Parser, Debug)]
#[command(name = "gitstratum-frontend")]
#[command(about = "GitStratum Frontend - Git protocol server")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:22")]
    ssh_addr: SocketAddr,

    #[arg(long, default_value = "0.0.0.0:443")]
    https_addr: SocketAddr,

    #[arg(long)]
    ssh_host_key: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1:9000")]
    control_plane_addr: String,

    #[arg(long, default_value = "127.0.0.1:9001")]
    metadata_addr: String,

    #[arg(long, default_value = "127.0.0.1:9002")]
    object_addr: String,

    #[arg(long, default_value = "127.0.0.1:9003")]
    auth_addr: String,

    #[arg(long)]
    node_id: Option<String>,

    #[arg(long, default_value = "0.0.0.0:9090")]
    metrics_addr: SocketAddr,

    #[arg(long, default_value = "5")]
    auth_max_failures: u32,

    #[arg(long, default_value = "300")]
    auth_window_secs: u64,

    #[arg(long, default_value = "900")]
    auth_lockout_secs: u64,
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
        auth = %args.auth_addr,
        "Connecting to cluster services"
    );

    let ssh_handle = if let Some(ref ssh_host_key) = args.ssh_host_key {
        tracing::info!(
            auth_addr = %args.auth_addr,
            "Connecting to auth service"
        );

        let grpc_auth_store = Arc::new(
            GrpcAuthStore::connect(&args.auth_addr)
                .await
                .map_err(|e| anyhow::anyhow!("failed to connect to auth service: {}", e))?,
        );

        let handle = Handle::current();
        let blocking_store = Arc::new(BlockingGrpcAuthStore::new(grpc_auth_store, handle));

        let rate_limiter = Arc::new(AuthRateLimiter::new(
            args.auth_max_failures,
            args.auth_window_secs,
            args.auth_lockout_secs,
        ));

        let validator = Arc::new(LocalValidator::new(blocking_store, rate_limiter));

        let ssh_config = SshServerConfig::new(args.ssh_addr, ssh_host_key.clone());

        let mut ssh_server = SshServer::new(ssh_config, validator);

        tracing::info!(
            ssh_addr = %args.ssh_addr,
            ssh_host_key = %ssh_host_key.display(),
            "Starting SSH server"
        );

        Some(tokio::spawn(async move {
            if let Err(e) = ssh_server.run().await {
                tracing::error!(error = %e, "SSH server error");
            }
        }))
    } else {
        tracing::info!("SSH server disabled (no --ssh-host-key provided)");
        None
    };

    tracing::info!("Frontend server started successfully");

    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down frontend server");

    if let Some(handle) = ssh_handle {
        handle.abort();
    }

    Ok(())
}
