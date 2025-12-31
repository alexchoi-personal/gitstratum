use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let node_id = args
        .node_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    println!(
        "[INFO] Starting GitStratum Frontend (node_id={}, ssh={}, https={})",
        node_id, args.ssh_addr, args.https_addr
    );

    println!(
        "[INFO] Connecting to cluster services (control_plane={}, metadata={}, object={})",
        args.control_plane_addr, args.metadata_addr, args.object_addr
    );

    println!("[INFO] Frontend server started successfully");

    tokio::signal::ctrl_c().await?;
    println!("[INFO] Shutting down frontend server");

    Ok(())
}
