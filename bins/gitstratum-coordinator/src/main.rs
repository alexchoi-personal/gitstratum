use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use gitstratum_coordinator::{
    run_heartbeat_flush_loop, ClusterCommand, CoordinatorConfig, CoordinatorServer,
    GlobalRateLimiter, HeartbeatBatcher, HeartbeatInfo, SerializableHeartbeatInfo,
};
use gitstratum_proto::coordinator_service_server::CoordinatorServiceServer;
use k8s_operator::raft::{KeyValueStateMachine, RaftConfig, RaftNodeManager, RaftRequest};
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

    #[arg(long, default_value = "45")]
    suspect_timeout: u64,

    #[arg(long, default_value = "45")]
    down_timeout: u64,

    #[arg(long, default_value = "1")]
    heartbeat_batch_interval: u64,
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

    let mut config = CoordinatorConfig::default();
    config.suspect_timeout = Duration::from_secs(args.suspect_timeout);
    config.down_timeout = Duration::from_secs(args.down_timeout);
    config.heartbeat_batch_interval = Duration::from_secs(args.heartbeat_batch_interval);

    let batcher = Arc::new(HeartbeatBatcher::new(config.heartbeat_batch_interval));
    let global_limiter = Arc::new(GlobalRateLimiter::new());
    let raft = Arc::new(raft);

    let raft_for_flush = Arc::clone(&raft);
    let batcher_for_flush = Arc::clone(&batcher);
    tokio::spawn(async move {
        run_heartbeat_flush_loop(
            batcher_for_flush,
            |batch: HashMap<String, HeartbeatInfo>| {
                let raft = Arc::clone(&raft_for_flush);
                async move {
                    let serializable_batch: HashMap<String, SerializableHeartbeatInfo> = batch
                        .into_iter()
                        .map(|(node_id, info)| {
                            let serializable = SerializableHeartbeatInfo {
                                known_version: info.known_version,
                                reported_state: info.reported_state,
                                generation_id: info.generation_id,
                                received_at_ms: info
                                    .received_at
                                    .elapsed()
                                    .as_millis()
                                    .try_into()
                                    .unwrap_or(0),
                            };
                            (node_id, serializable)
                        })
                        .collect();

                    let cmd = ClusterCommand::BatchHeartbeat(serializable_batch);
                    let cmd_bytes = serde_json::to_string(&cmd)
                        .map_err(|e| format!("Serialize error: {}", e))?;

                    let request = RaftRequest {
                        key: "topology".to_string(),
                        value: cmd_bytes,
                    };

                    raft.raft()
                        .client_write(request)
                        .await
                        .map_err(|e| format!("Raft write error: {:?}", e))?;

                    Ok::<(), String>(())
                }
            },
        )
        .await;
    });

    let server = Arc::new(CoordinatorServer::new(
        raft,
        config,
        batcher,
        global_limiter,
    ));

    let server_for_detector = Arc::clone(&server);
    tokio::spawn(async move {
        server_for_detector.run_failure_detector().await;
    });

    tracing::info!("Starting gRPC server on {}", args.grpc_addr);

    Server::builder()
        .add_service(CoordinatorServiceServer::from_arc(server))
        .serve(args.grpc_addr)
        .await?;

    Ok(())
}
