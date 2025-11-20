//! This crate provides and implementation of consistent hashing consistent with our go
//! implementation to allow members in both to work compatibly.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Node already in ring.")]
pub struct NodeAlreadyExists;

#[derive(Debug, Error)]
#[error("Node with the given node name does not exist within the ring")]
pub struct NodeDoesNotExist;

#[derive(Debug, Error)]
#[error("The ring contains no nodes.")]
pub struct RingEmpty;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord)]
pub struct Node {
    /// The name of the node.
    name: Arc<str>,

    /// The number of replicas for this node.
    num_replicas: u32,
}

impl Node {
    pub fn new(name: impl Into<Arc<str>>, num_replicas: u32) -> Node {
        Node {
            name: name.into(),
            num_replicas,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn name_arc(&self) -> Arc<str> {
        self.name.clone()
    }

    pub fn num_replicas(&self) -> u32 {
        self.num_replicas
    }
}

#[derive(Clone, Debug)]
struct Item {
    // Number is derived from a hash of the node's key
    ring_index: u64,
    // The node name that this item belongs to.
    node_name: Arc<str>,
}

/// Provides consistent hashing for keys to nodes.
#[derive(Debug, Default)]
pub struct HashRing {
    nodes: RingNodes,
    items: RingItems,
}

#[derive(Default, Debug, Clone)]
pub struct RingNodes(BTreeMap<Arc<str>, Node>);

impl RingNodes {
    pub fn with_nodes(nodes: impl IntoIterator<Item = Node>) -> Self {
        Self(nodes.into_iter().map(|n| (n.name.clone(), n)).collect())
    }

    /// Collects the nodes into a vec ordered by node name.
    pub fn collect_nodes(&self) -> Vec<Node> {
        self.0.values().cloned().collect()
    }

    /// Adds a node to the collection, failing if a node with that name already exists.
    pub fn add(&mut self, node: Node) -> Result<(), NodeAlreadyExists> {
        if self.0.contains_key(&node.name) {
            return Err(NodeAlreadyExists);
        }

        self.0.insert(node.name.clone(), node);
        Ok(())
    }

    /// Removes a node from the collection by its name, failing if a node with that
    /// name does not exist.
    pub fn remove(&mut self, node_name: &str) -> Result<(), NodeDoesNotExist> {
        self.0.remove(node_name).ok_or(NodeDoesNotExist)?;
        Ok(())
    }

    fn build_ring_items(&self) -> RingItems {
        let mut items = Vec::with_capacity(self.num_replicas());
        for node in self.0.values() {
            for idx in 0..node.num_replicas {
                items.push(Item {
                    node_name: node.name.clone(),
                    ring_index: hash(format!("{}{}", node.name, idx)),
                });
            }
        }
        items.sort_by_key(|node| node.ring_index);

        RingItems(items.into_boxed_slice())
    }

    /// Computes the sum of the replicas of all nodes in the collection.
    fn num_replicas(&self) -> usize {
        self.0.values().map(|node| node.num_replicas as usize).sum()
    }

    /// Returns the number of nodes in the collection.
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Default, Debug)]
struct RingItems(Box<[Item]>);

impl RingItems {
    /// Finds the node index for the corresponding key.
    ///
    /// The returned index is guaranteed to be a valid index that can be safely used to index into the contained items array.
    ///
    /// Returns an error if the ring is empty - as an empty ring cannot have a valid index returned.
    #[inline(always)]
    fn find_index(&self, key: u64) -> Result<usize, RingEmpty> {
        let len = self.len();
        if len == 0 {
            return Err(RingEmpty);
        }

        let index = match self.0.binary_search_by_key(&key, |item| item.ring_index) {
            // We landed right on the node! What are the odds!?
            // Since we are looking for the *next* node, advance the index by 1.
            Ok(index) => index + 1,
            Err(index) => index,
        };

        // Bounds checking on idx, check if it overflows and wrap around.
        Ok(if index >= len { 0 } else { index })
    }

    /// Finds the corresponding [`Item`] for the given key.
    ///
    /// Returns an error if the ring is empty.
    #[inline(always)]
    fn find_next(&self, key: u64) -> Result<&Item, RingEmpty> {
        let index = self.find_index(key)?;
        // safety: index bounds are checked in `find_index`.
        Ok(unsafe { self.0.get_unchecked(index) })
    }

    /// Returns the length of items within the hash ring items array.
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Computes the next index by adding 1, or wrapping around if the index overflows.
    #[inline(always)]
    fn next_index(&self, mut index: usize) -> usize {
        index += 1;
        if index >= self.len() {
            0
        } else {
            index
        }
    }
}

impl HashRing {
    /// Creates an empty hash ring with no nodes.
    pub fn new() -> Self {
        Default::default()
    }

    /// Constructs a HashRing given an iterator of nodes. If a node is duplicated within the iterator, the
    /// last value will be used.
    pub fn with_nodes(nodes: impl IntoIterator<Item = Node>) -> Self {
        let nodes = RingNodes::with_nodes(nodes);
        let items = nodes.build_ring_items();
        Self { items, nodes }
    }

    /// Returns an immutable reference to the current nodes in the hash ring.
    pub fn nodes(&self) -> &RingNodes {
        &self.nodes
    }

    /// Replaces the nodes in the hash ring.
    pub fn set_nodes(&mut self, nodes: impl IntoIterator<Item = Node>) {
        *self = Self::with_nodes(nodes);
    }

    /// Adds a node to the hash ring.
    pub fn add(&mut self, node: Node) -> Result<(), NodeAlreadyExists> {
        self.nodes.add(node)?;
        self.items = self.nodes.build_ring_items();
        Ok(())
    }

    /// Removes a node from the hash ring.
    pub fn remove(&mut self, node_name: impl AsRef<str>) -> Result<(), NodeDoesNotExist> {
        self.nodes.remove(node_name.as_ref())?;
        self.items = self.nodes.build_ring_items();
        Ok(())
    }

    /// Finds a node for a given key.
    #[inline(always)]
    pub fn find(&self, key: impl AsRef<str>) -> Result<&Arc<str>, RingEmpty> {
        self.items.find_next(hash(key)).map(|item| &item.node_name)
    }

    /// Finds `num` number of nodes for a given key.
    pub fn find_n(&self, key: impl AsRef<str>, num: usize) -> Result<Vec<Arc<str>>, RingEmpty> {
        let num = self.nodes.len().min(num);
        let mut index = self.items.find_index(hash(key.as_ref()))?;

        let mut nodes = Vec::with_capacity(num);
        while nodes.len() < num {
            // safety: bounds checking is done in `find_index` and `next_index`.
            let item = unsafe { self.items.0.get_unchecked(index) };
            index = self.items.next_index(index);

            // Skip Nodes we have already seen.
            if !nodes.contains(&item.node_name) {
                nodes.push(item.node_name.clone());
            }
        }

        Ok(nodes)
    }
}

fn hash(key: impl AsRef<str>) -> u64 {
    let digest = md5::compute(key.as_ref());

    // This may seem sub-optimal, but actually, this is more fast than say doing
    // u64::from_le_bytes((&digest[8..].try_into().unwrap()));
    // or u64::from_ne_bytes([digest[8], digest[9], ..])
    let low = u32::from(digest[11]) << 24
        | u32::from(digest[10]) << 16
        | u32::from(digest[9]) << 8
        | u32::from(digest[8]);
    let high = u32::from(digest[15]) << 24
        | u32::from(digest[14]) << 16
        | u32::from(digest[13]) << 8
        | u32::from(digest[12]);
    let mut key_int = u64::from(high);
    key_int <<= 32;
    key_int &= 0xffff_ffff_0000_0000;
    key_int |= u64::from(low);
    key_int
}
