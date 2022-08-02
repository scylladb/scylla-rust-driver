use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use smallvec::SmallVec;

use crate::transport::Node;

const SMALLVEC_SIZE: usize = 4;

#[derive(Clone)]
pub enum TriedNodesSet {
    Small(SmallVec<[SocketAddr; SMALLVEC_SIZE]>),
    Big(HashSet<SocketAddr>),
}

impl TriedNodesSet {
    pub fn new() -> TriedNodesSet {
        TriedNodesSet::Small(SmallVec::new())
    }

    pub fn insert(&mut self, node: &Arc<Node>) {
        match self {
            TriedNodesSet::Small(small_vec) => {
                if small_vec.len() < SMALLVEC_SIZE {
                    small_vec.push(node.address);
                    return;
                }

                // Clippy complains that element type of this HashSet is mutable.
                // Theoretically Arc<Node> is mutable, but its comparator uses only
                // the ip address field which is immutable.
                #[allow(clippy::mutable_key_type)]
                let hash_set: HashSet<SocketAddr> = small_vec.iter().copied().collect();
                *self = TriedNodesSet::Big(hash_set);
                self.insert(node)
            }
            TriedNodesSet::Big(hash_set) => {
                let _ = hash_set.insert(node.address);
            }
        }
    }

    pub fn contains(&self, node: &Arc<Node>) -> bool {
        match self {
            TriedNodesSet::Small(small_vec) => small_vec.contains(&node.address),
            TriedNodesSet::Big(hash_set) => hash_set.contains(&node.address),
        }
    }
}

impl Default for TriedNodesSet {
    fn default() -> TriedNodesSet {
        TriedNodesSet::new()
    }
}
