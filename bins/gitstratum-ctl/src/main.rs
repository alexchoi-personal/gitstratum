use anyhow::Result;
use clap::{Parser, Subcommand};
use gitstratum_control_plane::{ClusterStateResponse, ControlPlaneClient};
use gitstratum_hashring::NodeState;

#[derive(Parser, Debug)]
#[command(name = "gitstratum-ctl")]
#[command(about = "GitStratum cluster management CLI")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    control_plane: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Status,

    #[command(name = "add-node")]
    AddNode {
        #[arg(long)]
        node_id: String,

        #[arg(long)]
        address: String,

        #[arg(long)]
        port: u16,

        #[arg(long, value_parser = parse_node_type)]
        node_type: gitstratum_control_plane::NodeType,
    },

    #[command(name = "remove-node")]
    RemoveNode {
        #[arg(long)]
        node_id: String,
    },

    #[command(name = "set-node-state")]
    SetNodeState {
        #[arg(long)]
        node_id: String,

        #[arg(long, value_parser = parse_node_state)]
        state: NodeState,
    },

    Rebalance {
        #[arg(long, default_value = "manual")]
        reason: String,
    },

    #[command(name = "rebalance-status")]
    RebalanceStatus {
        #[arg(long)]
        rebalance_id: String,
    },

    #[command(name = "hash-ring")]
    HashRing,
}

fn parse_node_type(s: &str) -> Result<gitstratum_control_plane::NodeType, String> {
    match s.to_lowercase().as_str() {
        "control-plane" | "control" => Ok(gitstratum_control_plane::NodeType::ControlPlane),
        "metadata" => Ok(gitstratum_control_plane::NodeType::Metadata),
        "object" => Ok(gitstratum_control_plane::NodeType::Object),
        "frontend" => Ok(gitstratum_control_plane::NodeType::Frontend),
        _ => Err(format!(
            "invalid node type: {}. Valid types: control-plane, metadata, object, frontend",
            s
        )),
    }
}

fn parse_node_state(s: &str) -> Result<NodeState, String> {
    match s.to_lowercase().as_str() {
        "active" => Ok(NodeState::Active),
        "joining" => Ok(NodeState::Joining),
        "draining" => Ok(NodeState::Draining),
        "down" => Ok(NodeState::Down),
        _ => Err(format!(
            "invalid node state: {}. Valid states: active, joining, draining, down",
            s
        )),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut client = ControlPlaneClient::connect(&args.control_plane).await?;

    match args.command {
        Command::Status => {
            let state: ClusterStateResponse = client.get_cluster_state().await?;
            println!("Cluster State (version {})", state.version);
            println!(
                "Leader: {}",
                if state.leader_id.is_empty() {
                    "none".to_string()
                } else {
                    state.leader_id
                }
            );
            println!();
            println!("Control Plane Nodes: {}", state.control_plane_nodes.len());
            for node in &state.control_plane_nodes {
                println!("  - {} ({}:{})", node.id, node.address, node.port);
            }
            println!();
            println!("Metadata Nodes: {}", state.metadata_nodes.len());
            for node in &state.metadata_nodes {
                println!("  - {} ({}:{})", node.id, node.address, node.port);
            }
            println!();
            println!("Object Nodes: {}", state.object_nodes.len());
            for node in &state.object_nodes {
                println!("  - {} ({}:{})", node.id, node.address, node.port);
            }
            println!();
            println!("Frontend Nodes: {}", state.frontend_nodes.len());
            for node in &state.frontend_nodes {
                println!("  - {} ({}:{})", node.id, node.address, node.port);
            }
        }

        Command::AddNode {
            node_id,
            address,
            port,
            node_type,
        } => {
            let node = gitstratum_control_plane::ExtendedNodeInfo {
                id: node_id.clone(),
                address,
                port,
                state: NodeState::Joining,
                node_type,
            };
            match client.add_node(node).await {
                Ok(()) => println!("Node {} added successfully", node_id),
                Err(e) => println!("Failed to add node {}: {}", node_id, e),
            }
        }

        Command::RemoveNode { node_id } => match client.remove_node(&node_id).await {
            Ok(()) => println!("Node {} removed successfully", node_id),
            Err(e) => println!("Failed to remove node {}: {}", node_id, e),
        },

        Command::SetNodeState { node_id, state } => {
            match client.set_node_state(&node_id, state).await {
                Ok(()) => println!("Node {} state updated successfully", node_id),
                Err(e) => println!("Failed to update node {} state: {}", node_id, e),
            }
        }

        Command::Rebalance { reason } => match client.trigger_rebalance(&reason).await {
            Ok(Some(rebalance_id)) => {
                println!("Rebalance started with ID: {}", rebalance_id);
            }
            Ok(None) => {
                println!("Rebalance did not start");
            }
            Err(e) => {
                println!("Failed to start rebalance: {}", e);
            }
        },

        Command::RebalanceStatus { rebalance_id } => {
            match client.get_rebalance_status(&rebalance_id).await {
                Ok(status) => {
                    if status.in_progress {
                        println!("Rebalance in progress:");
                        println!("  Progress: {:.1}%", status.progress_percent);
                        println!("  Bytes moved: {}", status.bytes_moved);
                        println!("  Bytes remaining: {}", status.bytes_remaining);
                    } else {
                        println!("Rebalance completed");
                    }
                }
                Err(e) => {
                    println!("Failed to get rebalance status: {}", e);
                }
            }
        }

        Command::HashRing => {
            let (entries, replication_factor, version) = client.get_hash_ring().await?;
            println!(
                "Hash Ring (version {}, replication factor {})",
                version, replication_factor
            );
            for (position, node_id) in entries {
                println!("  Position {}: {}", position, node_id);
            }
        }
    }

    Ok(())
}
