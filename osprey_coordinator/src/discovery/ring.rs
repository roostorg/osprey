#[cfg(feature = "modify")]
use std::num::NonZeroU32;
use std::{collections::HashMap, sync::Arc};

use crate::etcd::{Action, Client, KeyValueInfo, WatchEvent, Watcher};
use crate::hashring::{HashRing, Node as HashRingNode};
use crate::tokio_utils::AbortOnDrop;
use anyhow::Result;
use arc_swap::ArcSwap;
use log::{debug, error, info, warn};
use tokio::{
    sync::oneshot,
    task::{block_in_place, spawn},
};
use tokio_stream::StreamExt;

use crate::discovery::{
    types::ring::{RingConfig, RingConfigV1},
    DiscoveryError,
};

pub(crate) const DEFAULT_NUM_REPLICAS: u32 = 512;

/// A ring of services used to consistently find a service instance in discovery by a key.
#[derive(Clone)]
pub struct Ring {
    inner: Arc<RingInner>,

    /// When all references are dropped, will terminate the ring watcher.
    _abort_on_drop: Arc<AbortOnDrop<()>>,
}

impl std::fmt::Debug for Ring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ring").field("inner", &self.inner).finish()
    }
}

#[derive(Debug)]
struct RingInner {
    /// Mutable ring data.
    data: ArcSwap<RingData>,
    /// The key in discovery where the ring will be stored.
    key: String,
    /// The ring's config.
    config: Config,
}

#[derive(Debug, Default)]
struct RingData {
    /// Internal hash ring.
    ring: HashRing,
    /// Ring key overrides
    overrides: HashMap<String, Arc<str>>,
}

#[derive(Debug)]
pub struct Config {
    /// Etcd client for setting the ring.
    pub client: Client,
    /// Default num replicas if the node doesn't specify.
    pub default_num_replicas: u32,
}

impl Config {
    pub fn from_environment() -> Result<Self> {
        Ok(Self {
            client: Client::from_etcd_peers()?,
            default_num_replicas: DEFAULT_NUM_REPLICAS,
        })
    }
}

impl Ring {
    /// Create a new `Ring`. Watches the key '/discovery/<name>/ring'.
    ///
    /// # Arguments
    ///
    /// * `name` - the name of the service, used to construct the watched key
    /// * `client` - etcd client
    pub async fn new(name: String) -> Result<Self> {
        let config = Config::from_environment()?;
        Self::from_config(name, config).await
    }

    /// Create a new `Ring` with a specific number of replicas per node.
    ///
    /// # Arguments
    ///
    /// * `name` - the name of the service, used to construct the watched key
    /// * `config` - ring `Config`
    pub async fn from_config(name: String, config: Config) -> Result<Self> {
        let key = format!("/discovery/{}/ring", name);
        Self::from_config_with_key(key, config).await
    }

    /// Create a new `Ring` with a specific number of replicas per node,
    /// specifying the actual key to watch.
    ///
    /// # Arguments
    ///
    /// * `name` - the name of the service
    /// * `key` - the key to watch for ring updates
    /// * `config` - ring `Config`
    pub async fn from_config_with_key(key: String, config: Config) -> Result<Self> {
        let inner = Arc::new(RingInner {
            key,
            config,
            data: Default::default(),
        });

        let (ready_tx, ready_rx) = oneshot::channel();
        let join_handle = spawn(watch(inner.clone(), ready_tx));

        let ring = Ring {
            inner,
            _abort_on_drop: Arc::new(AbortOnDrop::new(join_handle)),
        };

        ready_rx.await?;
        Ok(ring)
    }

    /// Select a single instance from discovery based on a key.
    ///
    /// # Arguments
    ///
    /// * `key` - the key to select for
    pub fn select(&self, key: impl AsRef<str>) -> Option<Arc<str>> {
        let data = self.inner.data.load();
        let key = key.as_ref();

        if let Some(node) = data.overrides.get(key) {
            return Some(node.clone());
        }

        data.ring.find(key).ok().cloned()
    }

    /// Select multiple instances from discovery based on a key.
    ///
    /// # Arguments
    ///
    /// * `key` - the key to select for
    /// * `n` - the number of instances to select, if an override exists return value could be above this limit
    pub fn select_n(&self, key: impl AsRef<str>, n: usize) -> Option<Vec<Arc<str>>> {
        let data = self.inner.data.load();
        let key = key.as_ref();
        let nodes = data.ring.find_n(key, n).ok();

        match data.overrides.get(key) {
            // No override, so we don't need to fiddle with the returned nodes array.
            None => nodes,
            // We have an override, so we'll set that override to the first position returned.
            Some(override_node) => match nodes {
                Some(mut nodes) => {
                    nodes.retain(|node_name| node_name != override_node);
                    nodes.insert(0, override_node.clone());
                    Some(nodes)
                }
                // No matches in the ring, but we've got an override.
                None => Some(vec![override_node.clone()]),
            },
        }
    }

    /// Returns the members of the ring.
    pub fn nodes(&self) -> Vec<HashRingNode> {
        self.inner.data.load().ring.nodes().collect_nodes()
    }

    /// Adds a single node to the ring. Warning: This is supposed to be a manual operation
    /// completed by a human operator. If this is done automatically then some data may be lost.
    ///
    /// # Arguments
    ///
    /// * `name` - name of the instances. Normally hostname:port.
    /// * `num_replicas` - the number of replicas for the instance
    #[cfg(feature = "modify")]
    pub async fn add(&self, name: impl Into<Arc<str>>, num_replicas: NonZeroU32) -> Result<()> {
        let name = name.into();
        debug!("Add {:?} {:?}", name, num_replicas);

        let mut nodes = self.inner.data.load().ring.nodes().clone();
        nodes.add(HashRingNode::new(name, num_replicas.get()))?;
        self.persist_nodes(nodes).await
    }

    /// Removes a single node from the ring. Warning: This is supposed to be a manual operation
    /// completed by a human operator. If this is done automatically then some data may be lost.
    ///
    /// # Arguments
    ///
    /// * `name` - name of the instances. Normally hostname:port.
    #[cfg(feature = "modify")]
    pub async fn remove(&self, name: impl AsRef<str>) -> Result<()> {
        let node_name = name.as_ref();
        debug!("Remove {:?}", node_name);

        let mut nodes = self.inner.data.load().ring.nodes().clone();
        nodes.remove(node_name)?;
        self.persist_nodes(nodes).await
    }

    /// Saves the members of the `Ring` to etcd.
    #[cfg(feature = "modify")]
    pub async fn persist_nodes(&self, nodes: hashring::RingNodes) -> Result<()> {
        let nodes = nodes.collect_nodes();
        match self
            .inner
            .config
            .client
            .set(&*self.inner.key, nodes, None)
            .await
        {
            Ok(_response) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// Watches etcd for changes to the ring.
async fn watch(ring: Arc<RingInner>, ready_tx: oneshot::Sender<()>) {
    let mut watcher = Watcher::new(ring.config.client.clone(), ring.key.clone());
    let mut ready_tx = Some(ready_tx);

    loop {
        let watch_event = match watcher.next().await {
            Some(event) => event,
            None => return,
        };

        let maybe_members = match watch_event {
            WatchEvent::Get(None) => {
                info!("Initialized with no nodes.");
                Some(RingConfigV1::default())
            }
            WatchEvent::Get(Some(info)) | WatchEvent::Update(info) => {
                get_ring_config(info, ring.config.default_num_replicas)
            }
        };

        let new_config = match maybe_members {
            Some(new_config) => new_config,
            None => continue,
        };

        debug!("Updating ring config: {:?}", new_config);

        // Rebuilding the ring can be "expensive", so we'll hint to the tokio executor that we want to run this in the blocking executor to avoid
        // blocking the runtime for other tasks.
        block_in_place(|| {
            let RingConfigV1 { overrides, members } = new_config;
            let ring_data = RingData {
                ring: HashRing::with_nodes(members),
                overrides: overrides
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(key, node_name)| {
                        (
                            // We need to convert the key to string because rust uses a string while the
                            // current configs use u64.
                            key.0.to_string(),
                            node_name.0.into(),
                        )
                    })
                    .collect(),
            };
            // Atomically update ring data.
            ring.data.store(Arc::new(ring_data));
        });

        // Notify waiter that we've read the ring.
        if let Some(ready_tx) = ready_tx.take() {
            ready_tx.send(()).ok();
        }
    }
}

/// Get the members from an etcd watch update.
///
/// # Arguments
///
/// * info - etcd node info
/// * default_num_replicas - the default number of replicas for a node
fn get_ring_config(info: KeyValueInfo, default_num_replicas: u32) -> Option<RingConfigV1> {
    match info.action {
        Action::Get | Action::Set | Action::Create | Action::CompareAndSwap | Action::Update => {
            // Deserializes the members from an etcd node. An etcd node can have two types of
            // data for members due to legacy reasons.
            let config = match &info.node.value {
                None => Err(DiscoveryError::NoNodeValue.into()),
                Some(value) => deserialize_config(value, default_num_replicas),
            };

            match config {
                Ok(new_config) => Some(new_config),
                Err(e) => {
                    error!("Failed to deserialize members. {}", e);
                    None
                }
            }
        }
        Action::Delete | Action::Expire | Action::CompareAndDelete => {
            warn!("All members offline.");
            Some(RingConfigV1::default())
        }
    }
}

fn deserialize_config(value: &str, default_num_replicas: u32) -> Result<RingConfigV1> {
    let ring_config = serde_json::from_str::<RingConfig>(value)?;
    Ok(ring_config.convert_to_v1(default_num_replicas))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assert_nodes(mut actual_nodes: Vec<HashRingNode>, mut expected_nodes: Vec<HashRingNode>) {
        actual_nodes.sort();
        expected_nodes.sort();

        assert_eq!(actual_nodes, expected_nodes);
    }

    #[test]
    fn it_deserializes_structs() {
        let expected_nodes = vec![
            HashRingNode::new("localhost:3000", 10),
            HashRingNode::new("localhost:3001", 20),
        ];
        let value = json!([
            {"name": expected_nodes[0].name(), "num_replicas": expected_nodes[0].num_replicas()},
            {"name": expected_nodes[1].name(), "num_replicas": expected_nodes[1].num_replicas()}
        ])
        .to_string();
        let config = deserialize_config(&value, 30).expect("failed to deserialize");
        assert_nodes(config.members, expected_nodes);
    }

    #[test]
    fn it_deserializes_strings() {
        let expected_nodes = vec![
            HashRingNode::new("localhost:3000", 20),
            HashRingNode::new("localhost:3001", 20),
        ];
        let value = json!([expected_nodes[0].name(), expected_nodes[1].name()]).to_string();
        let config = deserialize_config(&value, 20).expect("failed to deserialize");
        assert_nodes(config.members, expected_nodes);
    }

    #[test]
    fn it_deserializes_mixed() {
        let expected_nodes = vec![
            HashRingNode::new("localhost:3000", 20),
            HashRingNode::new("localhost:3001", 10),
        ];
        let value = json!([
            expected_nodes[0].name(),
            {"name": expected_nodes[1].name(), "num_replicas": expected_nodes[1].num_replicas()}
        ])
        .to_string();

        let config = deserialize_config(&value, 20).expect("failed to deserialize");
        assert_nodes(config.members, expected_nodes);
    }

    #[test]
    fn it_deserializes_v1() {
        let expected_nodes = vec![
            HashRingNode::new("localhost:3000", 20),
            HashRingNode::new("localhost:3001", 10),
        ];

        let value = json!({
            "schema": "v1",
            "members": [
                {"name": expected_nodes[0].name(), "num_replicas": expected_nodes[0].num_replicas()},
                {"name": expected_nodes[1].name(), "num_replicas": expected_nodes[1].num_replicas()}
            ]
        })
        .to_string();

        let config = deserialize_config(&value, 20).expect("failed to deserialize");
        assert_nodes(config.members, expected_nodes);
        assert!(config.overrides.is_none());
    }

    #[test]
    fn it_deserializes_v1_overrides() {
        let expected_nodes = vec![
            HashRingNode::new("localhost:3000", 20),
            HashRingNode::new("localhost:3001", 10),
        ];

        let expected_overrides = vec![(1234u64, &expected_nodes[0]), (5678u64, &expected_nodes[1])];

        let value = json!({
            "schema": "v1",
            "overrides": [
                [expected_overrides[0].0, expected_overrides[0].1.name()],
                [expected_overrides[1].0, expected_overrides[1].1.name()]
            ],
            "members": [
                {"name": expected_nodes[0].name(), "num_replicas": expected_nodes[0].num_replicas()},
                {"name": expected_nodes[1].name(), "num_replicas": expected_nodes[1].num_replicas()}
            ]
        })
        .to_string();

        let config = deserialize_config(&value, 20).expect("failed to deserialize");
        assert_nodes(config.members, expected_nodes.clone());

        let actual_overrides = config
            .overrides
            .expect("override should be found")
            .into_iter()
            .map(|(key, node)| (key.0, node.0))
            .collect::<Vec<_>>();

        assert_eq!(
            actual_overrides,
            expected_overrides
                .into_iter()
                .map(|(key, node)| (key, node.name().to_string()))
                .collect::<Vec<_>>()
        );
    }
}
