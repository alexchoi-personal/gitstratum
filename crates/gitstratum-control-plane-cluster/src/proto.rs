use gitstratum_hashring::NodeState as HashRingNodeState;
use gitstratum_proto::{
    NodeInfo as ProtoNodeInfo, NodeState as ProtoNodeState, NodeType as ProtoNodeType,
};

use crate::membership::{ExtendedNodeInfo, NodeType};

pub fn proto_node_state_to_hashring(state: ProtoNodeState) -> HashRingNodeState {
    match state {
        ProtoNodeState::Active => HashRingNodeState::Active,
        ProtoNodeState::Joining => HashRingNodeState::Joining,
        ProtoNodeState::Draining => HashRingNodeState::Draining,
        ProtoNodeState::Down => HashRingNodeState::Down,
        ProtoNodeState::Unknown => HashRingNodeState::Down,
    }
}

pub fn hashring_node_state_to_proto(state: HashRingNodeState) -> ProtoNodeState {
    match state {
        HashRingNodeState::Active => ProtoNodeState::Active,
        HashRingNodeState::Joining => ProtoNodeState::Joining,
        HashRingNodeState::Draining => ProtoNodeState::Draining,
        HashRingNodeState::Down => ProtoNodeState::Down,
    }
}

impl From<ProtoNodeType> for NodeType {
    fn from(node_type: ProtoNodeType) -> Self {
        match node_type {
            ProtoNodeType::ControlPlane => NodeType::ControlPlane,
            ProtoNodeType::Metadata => NodeType::Metadata,
            ProtoNodeType::Object => NodeType::Object,
            ProtoNodeType::Frontend => NodeType::Frontend,
            ProtoNodeType::Unknown => NodeType::Frontend,
        }
    }
}

impl From<NodeType> for ProtoNodeType {
    fn from(node_type: NodeType) -> Self {
        match node_type {
            NodeType::ControlPlane => ProtoNodeType::ControlPlane,
            NodeType::Metadata => ProtoNodeType::Metadata,
            NodeType::Object => ProtoNodeType::Object,
            NodeType::Frontend => ProtoNodeType::Frontend,
        }
    }
}

impl From<&ExtendedNodeInfo> for ProtoNodeInfo {
    fn from(node: &ExtendedNodeInfo) -> Self {
        ProtoNodeInfo {
            id: node.id.clone(),
            address: node.address.clone(),
            port: node.port as u32,
            state: hashring_node_state_to_proto(node.state).into(),
            r#type: ProtoNodeType::from(node.node_type).into(),
        }
    }
}

impl From<&ProtoNodeInfo> for ExtendedNodeInfo {
    fn from(node: &ProtoNodeInfo) -> Self {
        ExtendedNodeInfo {
            id: node.id.clone(),
            address: node.address.clone(),
            port: node.port as u16,
            state: proto_node_state_to_hashring(
                ProtoNodeState::try_from(node.state).unwrap_or(ProtoNodeState::Unknown),
            ),
            node_type: ProtoNodeType::try_from(node.r#type)
                .unwrap_or(ProtoNodeType::Unknown)
                .into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_state_roundtrip() {
        let states = [
            HashRingNodeState::Active,
            HashRingNodeState::Joining,
            HashRingNodeState::Draining,
            HashRingNodeState::Down,
        ];

        for state in states {
            let proto = hashring_node_state_to_proto(state);
            let back = proto_node_state_to_hashring(proto);
            assert_eq!(state, back);
        }
    }

    #[test]
    fn test_proto_unknown_state_maps_to_down() {
        assert_eq!(
            proto_node_state_to_hashring(ProtoNodeState::Unknown),
            HashRingNodeState::Down
        );
    }

    #[test]
    fn test_node_type_roundtrip() {
        let types = [
            NodeType::ControlPlane,
            NodeType::Metadata,
            NodeType::Object,
            NodeType::Frontend,
        ];

        for node_type in types {
            let proto: ProtoNodeType = node_type.into();
            let back: NodeType = proto.into();
            assert_eq!(node_type, back);
        }
    }

    #[test]
    fn test_proto_unknown_type_maps_to_frontend() {
        let result: NodeType = ProtoNodeType::Unknown.into();
        assert_eq!(result, NodeType::Frontend);
    }

    #[test]
    fn test_extended_node_roundtrip() {
        let node = ExtendedNodeInfo::new("node1", "192.168.1.1", 8080, NodeType::Object);

        let proto: ProtoNodeInfo = (&node).into();
        let back: ExtendedNodeInfo = (&proto).into();

        assert_eq!(node.id, back.id);
        assert_eq!(node.address, back.address);
        assert_eq!(node.port, back.port);
        assert_eq!(node.node_type, back.node_type);
    }
}
