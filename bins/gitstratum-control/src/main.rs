use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "gitstratum-control")]
#[command(about = "GitStratum Control Plane - Raft-based cluster coordination")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:9000")]
    grpc_addr: SocketAddr,

    #[arg(long, default_value = "0.0.0.0:9100")]
    raft_addr: SocketAddr,

    #[arg(long)]
    node_id: Option<u64>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long)]
    join: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let node_id = args.node_id.unwrap_or(1);

    let data_dir = args
        .data_dir
        .unwrap_or_else(|| PathBuf::from("./data/control"));

    println!(
        "[INFO] Starting GitStratum Control Plane (node_id={}, grpc={}, raft={}, data_dir={})",
        node_id,
        args.grpc_addr,
        args.raft_addr,
        data_dir.display()
    );

    std::fs::create_dir_all(&data_dir)?;

    let store = gitstratum_control_plane_cluster::ControlPlaneStore::with_db(&data_dir)?;
    let (_log_store, _state_machine) = gitstratum_control_plane_cluster::create_stores(store);

    println!("[INFO] RocksDB store initialized");

    let _raft_config = openraft::Config {
        cluster_name: "gitstratum".to_string(),
        ..Default::default()
    };

    println!("[INFO] Raft configuration created");

    if args.join.is_none() {
        println!("[INFO] This node will bootstrap as single-node cluster");
    } else {
        println!("[INFO] This node will join cluster at: {:?}", args.join);
    }

    println!("[INFO] Starting gRPC server at {}", args.grpc_addr);
    println!("[INFO] Note: Full Raft implementation requires RaftNetwork implementation");

    tokio::signal::ctrl_c().await?;
    println!("[INFO] Shutting down control plane");

    Ok(())
}
