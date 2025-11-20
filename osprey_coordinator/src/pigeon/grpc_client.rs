//! A wrapper client for automatically handling etcd based discovery with grpc.

use crate::discovery::ServiceRegistration;
use crate::discovery::ServiceWatcher;
use crate::etcd::Client as EtcdClient;
use anyhow::Result;
use futures::future::try_join_all;
use futures::future::FutureExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Endpoint;

use super::utils::ChunkedRequest;
use super::utils::Connector;
use super::utils::ScalarRequest;

#[derive(Debug)]
struct Inner<C> {
    self_id: String,
    num_replicas: usize,
    // FIXME: Nothing ever remove bad clients from this
    clients: RwLock<HashMap<String, C>>,
    service_watcher: ServiceWatcher,
    endpoint_options: EndpointOptions,
}

#[derive(Clone, Debug)]
pub struct GrpcClient<C> {
    inner: Arc<Inner<C>>,
    peer_service: Option<&'static str>,
}

impl<C> GrpcClient<C>
where
    C: Clone + Connector,
{
    pub async fn new(
        self_id: String,
        service_name: String,
        num_replicas: usize,
        peer_service: Option<&'static str>,
    ) -> Result<Self> {
        let client = EtcdClient::from_etcd_peers()?;
        let service_watcher = ServiceWatcher::watch(service_name, client.clone(), true).await?;
        Self::new_with_watcher(self_id, num_replicas, service_watcher, peer_service).await
    }

    pub async fn new_with_watcher(
        self_id: String,
        num_replicas: usize,
        service_watcher: ServiceWatcher,
        peer_service: Option<&'static str>,
    ) -> Result<Self> {
        Ok(GrpcClient {
            inner: Arc::new(Inner {
                self_id,
                num_replicas,
                clients: RwLock::new(HashMap::new()),
                service_watcher,
                endpoint_options: EndpointOptions::default(),
            }),
            peer_service,
        })
    }

    pub fn with_endpoint_options(self, endpoint_options: EndpointOptions) -> Self {
        GrpcClient {
            inner: Arc::new(Inner {
                self_id: self.inner.self_id.clone(),
                num_replicas: self.inner.num_replicas,
                clients: Default::default(),
                service_watcher: self.inner.service_watcher.clone(),
                endpoint_options,
            }),
            peer_service: self.peer_service,
        }
    }

    fn split_keys<'a, Key, KeyIterator>(
        &self,
        keys: KeyIterator,
        tolerate_draining: bool,
    ) -> Result<HashMap<ServiceInfo, (Vec<usize>, Vec<Key>)>>
    where
        Key: Clone + ToString + Debug + 'a,
        KeyIterator: Iterator<Item = &'a Key> + 'a,
    {
        let mut chunks: HashMap<ServiceInfo, (Vec<usize>, Vec<Key>)> = HashMap::new();
        for (i, key) in keys.enumerate() {
            let service = &self.inner.service_watcher.select_key(
                key.to_string(),
                self.inner.num_replicas,
                Some(&self.inner.self_id),
                tolerate_draining,
                0,
            )?;
            let (index_chunk, key_chunk) = chunks
                .entry(service.into())
                .or_insert((Vec::new(), Vec::new()));
            index_chunk.push(i);
            key_chunk.push(key.clone());
        }
        Ok(chunks)
    }

    fn get_client(&self, service_info: ServiceInfo) -> Result<C, tonic::Status> {
        let key = &service_info.key;

        // Try grabbing client via read lock, just incase:
        let client = self.inner.clients.read().get(key).cloned();
        if let Some(client) = client {
            return Ok(client);
        }

        // Acquire a write lock now, and try to create a client.
        let mut clients = self.inner.clients.write();

        // 1) Did anyone race us in creating the client, check first:
        if let Some(client) = clients.get(key).cloned() {
            return Ok(client);
        }

        // 2) No one did, so let's create a client w/ connect_lazy, so that we don't keep this lock acquired,
        // while the client connects.
        let uri = service_info.uri();
        let endpoint = self.inner.endpoint_options.apply(
            Endpoint::new(uri)
                .map_err(|e| tonic::Status::unavailable(format!("invalid uri: {:?}", e)))?,
        );

        let client = C::connect_lazy(endpoint);
        clients.insert(key.to_owned(), client.clone());

        Ok(client)
    }

    fn create_request<Req>(&self, new_request: Req) -> tonic::Request<Req> {
        let mut grpc_request = tonic::Request::new(new_request);
        if let Some(peer_service) = self.peer_service {
            grpc_request.metadata_mut().insert(
                "service",
                peer_service
                    .parse()
                    .expect("conversion to MetadataValue failed"),
            );
        }
        grpc_request
    }

    pub async fn chunked_unary_unary<K, V, Req, F, FR, Resp>(
        &self,
        request: Req,
        client_fn: F,
        tolerate_draining: bool,
    ) -> Result<Resp, tonic::Status>
    where
        K: Clone + ToString + Debug,
        F: Fn(C, tonic::Request<Req>) -> FR,
        FR: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
        Req: ChunkedRequest<Key = K>,
        Resp: Into<Vec<V>> + From<Vec<V>>,
    {
        let chunks = self
            .split_keys(request.iter_keys(), tolerate_draining)
            .map_err(|e| tonic::Status::unavailable(format!("failed to split keys: {:?}", e)))?;

        let mut futs = Vec::with_capacity(chunks.len());

        for (service_info, (indices, keys)) in chunks {
            let client = self.get_client(service_info)?;
            let new_request = request.with_keys(keys.clone());
            let fut = client_fn(client, self.create_request(new_request))
                // FIXME: We lose the metadata from the Response here :(
                .map(|res| res.map(|resp| indices.into_iter().zip(resp.into_inner().into())));
            futs.push(fut);
        }
        let mut sorted: Vec<(usize, V)> = try_join_all(futs).await?.into_iter().flatten().collect();
        sorted.sort_by_key(|(i, _)| *i);
        Ok(sorted
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<V>>()
            .into())
    }

    pub async fn scalar_unary_unary<K, V, Req: ScalarRequest<Key = K>, F, FR>(
        &self,
        request: Req,
        client_fn: F,
        tolerate_draining: bool,
        instances_to_skip: u16,
    ) -> Result<V, tonic::Status>
    where
        K: Clone + ToString + Debug,
        F: Fn(C, tonic::Request<Req>) -> FR,
        FR: Future<Output = Result<tonic::Response<V>, tonic::Status>>,
    {
        let inner = &self.inner;
        let service = &inner
            .service_watcher
            .select_key(
                request.get_key().to_string(),
                inner.num_replicas,
                Some(&inner.self_id),
                tolerate_draining,
                instances_to_skip,
            )
            .map_err(|e| tonic::Status::unavailable(format!("failed get client. e={:?}", e)))?;

        let client = self.get_client(service.into())?;
        let response = client_fn(client, self.create_request(request))
            .await?
            .into_inner();
        Ok(response)
    }

    pub async fn round_robin_unary_unary<Req, V, F, FR>(
        &self,
        request: Req,
        client_fn: F,
    ) -> Result<V, tonic::Status>
    where
        F: Fn(C, tonic::Request<Req>) -> FR,
        FR: Future<Output = Result<tonic::Response<V>, tonic::Status>>,
    {
        let service = &self
            .inner
            .service_watcher
            .select()
            .map_err(|e| tonic::Status::unavailable(format!("failed get client. e={:?}", e)))?;

        let client = self.get_client(service.into())?;
        let response = client_fn(client, self.create_request(request))
            .await?
            .into_inner();
        Ok(response)
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct ServiceInfo {
    key: String,
}

impl ServiceInfo {
    fn uri(&self) -> String {
        format!("http://{}", self.key)
    }
}

impl From<&ServiceRegistration> for ServiceInfo {
    fn from(service: &ServiceRegistration) -> Self {
        let grpc_port = match service.ports.get("grpc") {
            Some(port) => *port,
            None => service.port + 1,
        };
        let key = match &service.ip {
            Some(ip) => format!("{}:{}", ip, grpc_port),
            None => format!("{}:{}", service.address, grpc_port),
        };
        ServiceInfo { key }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct EndpointOptions {
    pub timeout: Option<Duration>,
    pub connect_timeout: Option<Duration>,
    pub concurrency_limit: Option<usize>,
}

impl EndpointOptions {
    fn apply(&self, mut e: Endpoint) -> Endpoint {
        if let Some(timeout) = self.timeout {
            e = e.timeout(timeout);
        }
        if let Some(connect_timeout) = self.connect_timeout {
            e = e.connect_timeout(connect_timeout);
        }
        if let Some(concurrency_limit) = self.concurrency_limit {
            e = e.concurrency_limit(concurrency_limit);
        }
        e
    }
}
