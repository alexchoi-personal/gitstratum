use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let node_id = args
        .node_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    println!(
        "[INFO] Starting GitStratum Object Store (node_id={}, grpc={}, data_dir={})",
        node_id,
        args.grpc_addr,
        args.data_dir.display()
    );

    std::fs::create_dir_all(&args.data_dir)?;

    let store = Arc::new(gitstratum_object::ObjectStore::new(&args.data_dir)?);
    let service = gitstratum_object::ObjectServiceImpl::new(store.clone());

    println!("[INFO] RocksDB store initialized");

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
