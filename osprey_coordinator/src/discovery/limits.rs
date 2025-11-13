use crate::discovery::service::ServiceRegistration;
use crate::discovery::watcher::ServiceWatcher;
use crate::etcd::Client;
use anyhow::Result;

#[derive(Clone, Debug)]
pub struct GlobalLimiter<T = Normal> {
    /// service watcher to keep track of cluster size
    service_watcher: ServiceWatcher,
    /// Limiter type is either [`Normal`] or [`Member`].
    limiter_type: T,
}

// type used for unowned limiter, cannot determine self share
#[derive(Clone, Debug)]
pub struct Normal;

// type used for owned limiter, can determine self share
#[derive(Clone, Debug)]
pub struct Member {
    /// service id for this node
    self_id: String,
}

impl GlobalLimiter<Normal> {
    /// Watch a service's instances for the calculation of per-node limits.
    ///
    /// # Arguments
    ///
    /// * `name` - the service name
    /// * `client` - etcd client
    /// * `ring` - wether the service uses a ring config
    pub async fn watch<N: Into<String>>(name: N, client: Client, ring: bool) -> Result<Self> {
        let service_watcher = ServiceWatcher::watch(name, client, ring).await?;

        let global_limiter = Self {
            service_watcher,
            limiter_type: Normal,
        };

        Ok(global_limiter)
    }
}

impl GlobalLimiter<Member> {
    /// Watch this service's instances for the calculation of per-node limits, implies that a ring config is used.
    ///
    /// # Arguments
    ///
    /// * `self_registration` - this node's service registration
    /// * `client` - etcd client
    pub async fn watch_as_member(
        self_registration: &ServiceRegistration,
        client: Client,
    ) -> Result<Self> {
        let service_watcher =
            ServiceWatcher::watch(self_registration.name.clone(), client, true).await?;

        let global_limiter = Self {
            service_watcher,
            limiter_type: Member {
                self_id: self_registration.id(),
            },
        };

        Ok(global_limiter)
    }

    /// Get this node's share, based on the amount of the ring owned by this node
    pub fn get_self_share(&self, global_limit: f64) -> Option<f64> {
        self.get_node_share(global_limit, &self.limiter_type.self_id)
    }
}

impl<T> GlobalLimiter<T> {
    /// Get the equal share, based on the number of instances
    pub fn get_equal_share(&self, global_limit: f64) -> Option<f64> {
        match self.service_watcher.num_instances() {
            num_instances if num_instances > 0 => Some(global_limit / (num_instances as f64)),
            _ => None,
        }
    }

    /// Get the given node's share, based on the amount of the ring owned by that node
    pub fn get_node_share(&self, global_limit: f64, service_id: impl AsRef<str>) -> Option<f64> {
        match self.service_watcher.ring_share(service_id) {
            ring_share if ring_share > 0.0 => Some(global_limit * ring_share),
            _ => None,
        }
    }
}
