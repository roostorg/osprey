use crate::backoff_utils::{AsyncBackoff, Config as BackoffConfig};
use crate::discovery::error::DiscoveryError;
use crate::discovery::ring::{Config as RingConfig, Ring, DEFAULT_NUM_REPLICAS};
use crate::discovery::service::ServiceRegistration;
use crate::etcd::{Action, Client, Node, WatchEvent, Watcher};
use crate::tokio_utils::{AbortOnDrop, UnboundedReceiverChunker};
use anyhow::Result;
use arc_swap::ArcSwap;
use indexmap::IndexMap;
use log::{error, info, trace};
use rand::Rng;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::spawn;
use tokio_stream::StreamExt;

/// Watchers a service's instances in discovery.
#[derive(Clone, Debug)]
pub struct ServiceWatcher {
    /// Key in etcd.
    key: String,
    /// List of instances currently in discovery.
    instances: Arc<ArcSwap<IndexMap<String, ServiceRegistration>>>,
    /// Ring for selecting by key.
    ring: Option<Ring>,
    /// Etcd client.
    client: Client,
    /// Sender that signals a stop once dropped.
    _stop_tx: UnboundedSender<()>,
}

impl ServiceWatcher {
    /// Watch a service's instances.
    ///
    /// # Arguments
    ///
    /// * `name` - the service name
    /// * `client` - etcd client
    pub async fn watch<N: Into<String>>(name: N, client: Client, ring: bool) -> Result<Self> {
        let name: String = name.into();
        let key = format!("/discovery/{}/instances", name);
        let ring = if ring {
            let config = RingConfig {
                client: client.clone(),
                default_num_replicas: DEFAULT_NUM_REPLICAS,
            };
            Some(Ring::from_config(name.clone(), config).await?)
        } else {
            None
        };
        let (_stop_tx, stop_rx) = unbounded_channel();
        let service_watcher = Self {
            key,
            instances: Default::default(),
            client,
            ring,
            _stop_tx,
        };
        let (ready_tx, ready_rx) = oneshot::channel();
        spawn(watch(
            service_watcher.client.clone(),
            service_watcher.key.clone(),
            service_watcher.instances.clone(),
            ready_tx,
            stop_rx,
        ));
        ready_rx.await?;
        Ok(service_watcher)
    }

    /// Select a single random node from the list of instances.
    /// Draining nodes are excluded.
    pub fn select(&self) -> Result<ServiceRegistration> {
        let instances_map = self.instances.load();
        let instances = instances_map
            .iter()
            .filter_map(|(_k, v)| (!v.draining).then_some(v))
            .collect::<Vec<_>>();

        let index = match instances.len() {
            0 => return Err(DiscoveryError::NoServices.into()),
            1 => 0,
            len => rand::thread_rng().gen_range(0..len),
        };

        match instances.get(index) {
            Some(&service) => Ok(service.clone()),
            None => Err(DiscoveryError::NoServices.into()),
        }
    }

    /// Returns a list of all known instances.
    /// Draining nodes are excluded.
    pub fn select_all(&self) -> Vec<ServiceRegistration> {
        let instances_map = self.instances.load();

        instances_map
            .iter()
            .filter_map(|(_k, v)| (!v.draining).then_some(v).cloned())
            .collect::<Vec<_>>()
    }

    /// Select a number of nodes by key.
    pub fn select_key<K: AsRef<str>, E: AsRef<str>>(
        &self,
        key: K,
        num_replicas: usize,
        exclude: Option<E>,
        tolerate_draining: bool,
        instances_to_skip: u16,
    ) -> Result<ServiceRegistration> {
        let ring = match &self.ring {
            Some(ring) => ring,
            None => return Err(DiscoveryError::NoRingConfigured.into()),
        };

        let nodes = match ring.select_n(key, num_replicas) {
            Some(nodes) => nodes,
            None => return Err(DiscoveryError::NoServices.into()),
        };

        let exclude = exclude.as_ref().map(|e| e.as_ref());

        let mut instance_index: u16 = 0;
        for node in nodes {
            if let Some(exclude) = &exclude {
                if &*node == *exclude {
                    continue;
                }
            }

            if let Some(service) = self.get(&node) {
                if service.draining && !tolerate_draining {
                    continue;
                }
                if instance_index == instances_to_skip {
                    return Ok(service);
                } else {
                    instance_index += 1;
                }
            }
        }
        Err(DiscoveryError::NoServices.into())
    }

    /// Get a Service by ID
    pub fn get(&self, id: impl AsRef<str>) -> Option<ServiceRegistration> {
        self.instances.load().get(id.as_ref()).cloned()
    }

    /// Get the number of instances
    pub fn num_instances(&self) -> usize {
        self.instances.load().len()
    }

    /// Get the share of the ring for the given service
    pub fn ring_share(&self, id: impl AsRef<str>) -> f64 {
        let ring = self
            .ring
            .as_ref()
            .expect("Cannot calculate ring_share without a ring");

        let mut node_replicas = 0;
        let mut total_replicas = 0;
        for node in &ring.nodes() {
            total_replicas += node.num_replicas();

            if node.name().eq(id.as_ref()) {
                node_replicas = node.num_replicas();
            }
        }

        if total_replicas > 0 {
            node_replicas as f64 / total_replicas as f64
        } else {
            0.0
        }
    }
}

/// Core loop. This watches etcd for changes in the list of instances.
async fn watch(
    client: Client,
    key: String,
    instances: Arc<ArcSwap<IndexMap<String, ServiceRegistration>>>,
    ready_tx: oneshot::Sender<()>,
    mut stop_rx: UnboundedReceiver<()>,
) {
    let mut watcher = Watcher::new(client, key);
    let mut ready_tx = Some(ready_tx);
    loop {
        let watch_event = tokio::select! {
            Some(watch_event) = watcher.next() => watch_event,
            Some(_) = stop_rx.recv() => return,
            else => return,
        };

        let info = match watch_event {
            WatchEvent::Get(Some(info)) => info,
            WatchEvent::Get(None) => {
                info!("Initialized with no nodes.");
                if let Some(ready_tx) = ready_tx.take() {
                    ready_tx.send(()).ok();
                }
                continue;
            }
            WatchEvent::Update(info) => info,
        };

        match info.action {
            Action::Get => {
                if let Some(nodes) = info.node.nodes {
                    match deserialize_nodes(&nodes) {
                        Ok(services) => {
                            info!("Services loaded. {:?}", services);
                            let new_map = services
                                .into_iter()
                                .map(|service| (service.id(), service))
                                .collect();
                            instances.store(Arc::new(new_map))
                        }
                        Err(e) => {
                            error!("Failed to deserialize nodes. {}", e);
                        }
                    }
                }
            }
            Action::Set | Action::Create | Action::CompareAndSwap | Action::Update => {
                match deserialize_node(&info.node) {
                    Ok(service) => {
                        let service_id = service.id();

                        if !instances.load().contains_key(&service_id) {
                            info!("Service online. {:?}", service);
                        }

                        instances.rcu(move |inner| {
                            let mut map = IndexMap::clone(inner);
                            map.insert(service_id.clone(), service.clone());
                            map
                        });
                    }
                    Err(e) => {
                        error!("Failed to deserialize node. {}", e);
                    }
                }
            }
            Action::Delete | Action::Expire | Action::CompareAndDelete => {
                if let Some(node) = &info.prev_node {
                    match deserialize_node(node) {
                        Ok(service) => {
                            info!("Service offline. {:?}", service);

                            instances.rcu(move |inner| {
                                let mut map = IndexMap::clone(inner);
                                map.remove(&service.id());
                                map
                            });
                        }
                        Err(e) => {
                            error!("Failed to deserialize node. {}", e);
                        }
                    }
                }
            }
        }

        if let Some(ready_tx) = ready_tx.take() {
            ready_tx.send(()).ok();
        }

        trace!("Services. {:?}", instances);
    }
}

/// Deserialize a list of nodes into `Service`s.
fn deserialize_nodes(nodes: &[Node]) -> Result<Vec<ServiceRegistration>> {
    nodes
        .iter()
        .map(deserialize_node)
        .collect::<Result<Vec<ServiceRegistration>>>()
}

/// Deserialize a node into a `Service`.
fn deserialize_node(node: &Node) -> Result<ServiceRegistration> {
    match &node.value {
        Some(value) => Ok(serde_json::from_str(value)?),
        None => Err(DiscoveryError::NoNodeValue.into()),
    }
}
