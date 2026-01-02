use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use gitstratum_coordinator::CoordinatorServer;
use gitstratum_proto::coordinator_service_server::CoordinatorServiceServer;
use k8s_operator::raft::{KeyValueStateMachine, RaftConfig, RaftNodeManager};
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "gitstratum-coordinator")]
#[command(about = "GitStratum Coordinator - Raft-based cluster management")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:9000")]
    grpc_addr: SocketAddr,

    #[arg(long, default_value = "0.0.0.0:9001")]
    raft_addr: SocketAddr,

    #[arg(long)]
    node_id: Option<u64>,

    #[arg(long, default_value = "gitstratum-coordinator-headless")]
    service_name: String,

    #[arg(long, default_value = "default")]
    namespace: String,

    #[arg(long)]
    bootstrap: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();

    let node_id = args.node_id.unwrap_or_else(|| {
        RaftNodeManager::<KeyValueStateMachine>::node_id_from_hostname().unwrap_or(1)
    });

    tracing::info!(
        "Starting GitStratum Coordinator (node_id={}, grpc={}, raft={})",
        node_id,
        args.grpc_addr,
        args.raft_addr
    );

    let config = RaftConfig::new("coordinator")
        .node_id(node_id)
        .service_name(&args.service_name)
        .namespace(&args.namespace);

    let raft = RaftNodeManager::<KeyValueStateMachine>::new(config).await?;

    if args.bootstrap {
        tracing::info!("Bootstrapping new cluster");
        raft.bootstrap().await?;
    } else {
        tracing::info!("Joining existing cluster or waiting for bootstrap");
        raft.bootstrap_or_join().await?;
    }

    raft.start_grpc_server(args.raft_addr.port()).await?;

    let server = CoordinatorServer::new(Arc::new(raft));

    tracing::info!("Starting gRPC server on {}", args.grpc_addr);

    Server::builder()
        .add_service(CoordinatorServiceServer::new(server))
        .serve(args.grpc_addr)
        .await?;

    Ok(())
}
