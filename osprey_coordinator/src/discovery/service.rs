use crate::discovery::error::DiscoveryError;

use anyhow::Result;
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use trust_dns_resolver::TokioAsyncResolver;

/// An advertisement of a network service.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ServiceRegistration {
    pub name: String,
    pub address: String,
    pub port: u16,
    #[serde(default)]
    pub ports: HashMap<String, u16>,
    #[serde(default)]
    pub ip: Option<String>,
    #[serde(default)]
    pub id_override: Option<String>,
    #[serde(default)]
    pub draining: bool,
}

impl ServiceRegistration {
    /// Creates a builder for the service, allowing you to configure the service metadata and address override.
    pub fn builder<S: Into<String>>(name: S, port: u16) -> Builder {
        Builder {
            name: name.into(),
            port,
            ports: HashMap::new(),
            address_override: None,
            ip_override: None,
            id_override: None,
            draining: false,
        }
    }
}

impl ServiceRegistration {
    pub fn id(&self) -> String {
        if let Some(id_override) = self.id_override.as_ref() {
            return format!("{}:{}", id_override.clone(), self.port);
        }
        format!("{}:{}", self.address, self.port)
    }

    pub(crate) fn key(&self) -> String {
        format!("/discovery/{}/instances/{}", self.name, self.id())
    }

    pub async fn socket_address(&self, resolver: TokioAsyncResolver) -> Result<SocketAddr> {
        let response = resolver.lookup_ip(self.address.as_str()).await?;
        let ip_addr =
            response
                .iter()
                .next()
                .ok_or_else(|| DiscoveryError::FailedToResolveHostname {
                    hostname: self.address.clone(),
                })?;
        Ok(SocketAddr::new(ip_addr, self.port))
    }
}

pub struct Builder {
    id_override: Option<String>,
    name: String,
    address_override: Option<String>,
    port: u16,
    ports: HashMap<String, u16>,
    ip_override: Option<String>,
    draining: bool,
}

impl Builder {
    /// Overrides the default id that will be used in the service key.
    ///
    /// The default id will be the address of the service. The port is automatically
    /// appended to the end; i.e. address:port.
    ///
    /// Do not try to override the port with this method! This will panic.
    pub fn with_portless_id_override<S: Into<String>>(mut self, id_override: S) -> Self {
        fn str_contains_port(input: &str) -> bool {
            let segments: Vec<&str> = input.split(':').collect();
            if segments.len() > 1 {
                let last_segment = segments.last().unwrap();
                return last_segment.parse::<u16>().is_ok();
            }
            false
        }
        let id: String = id_override.into();
        assert!(
            !str_contains_port(&id),
            "id override must not contain a port because the port is automatically appended."
        );
        self.id_override = Some(id);
        self
    }

    /// Overrides the default address that will be used with the service.
    ///
    /// The default address will be the current host's hostname, or localhost, if no hostname was able to
    /// be retrieved.
    pub fn with_address_override<S: Into<String>>(mut self, address_override: S) -> Self {
        self.address_override = Some(address_override.into());
        self
    }

    /// Sets the ip address that will be used with the service.
    ///
    /// By default the ip address is not passed with the registration.
    pub fn with_ip_override<S: Into<String>>(mut self, ip_override: S) -> Self {
        self.ip_override = Some(ip_override.into());
        self
    }

    /// Overrides the ports that will be announced alongside the service.
    pub fn with_ports(mut self, ports: HashMap<String, u16>) -> Self {
        self.ports = ports;
        self
    }

    /// Overrides the grpc port that will be used for the service.
    pub fn with_grpc_port(mut self, grpc_port: u16) -> Self {
        self.ports.insert("grpc".into(), grpc_port);
        self
    }

    /// Overrides the http port that will be used for the service.
    pub fn with_http_port(mut self, http_port: u16) -> Self {
        self.ports.insert("http".into(), http_port);
        self
    }

    /// Overrides the drain state that will be used for the service.
    ///
    /// By default the drain state is `false`.
    pub fn with_drain_state(mut self, draining: bool) -> Self {
        self.draining = draining;
        self
    }

    /// Consumes the builder, creating a [`Service`].
    ///
    /// ### Panics:
    ///
    /// Panics if the hostname as was not a utf-8 string.
    pub fn build(self) -> ServiceRegistration {
        ServiceRegistration {
            name: self.name,
            port: self.port,
            ports: self.ports,
            address: compute_address(self.address_override),
            ip: self.ip_override,
            id_override: self.id_override,
            draining: self.draining,
        }
    }
}

fn compute_address(address_override: Option<String>) -> String {
    if let Some(address_override) = address_override {
        return address_override;
    }

    if let Ok(hostname) = hostname::get() {
        let hostname = hostname
            .into_string()
            .expect("hostname was not a valid utf-8 string.");
        return append_hostname_suffix(hostname);
    }

    warn!("couldn't retrieve service host name. falling back localhost.");

    "localhost".to_owned()
}

fn append_hostname_suffix(hostname: String) -> String {
    match std::env::var("DISCOVERY_HOSTNAME_SUFFIX") {
        Ok(suffix) => format!("{}.{}", hostname, suffix),
        Err(_) => hostname,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_builder_address_override() {
        let mut ports = HashMap::new();
        ports.insert("grpc".to_string(), 9001);
        let service = ServiceRegistration::builder("hello", 1234)
            .with_address_override("world")
            .with_ports(ports)
            .build();

        assert_eq!(service.name, "hello");
        assert_eq!(service.id(), "world:1234");
        assert_eq!(service.port, 1234);
        assert_eq!(*service.ports.get("grpc").unwrap(), 9001);
    }

    #[test]
    fn test_builder_id_override() {
        let mut ports = HashMap::new();
        ports.insert("grpc".to_string(), 9001);
        let service = ServiceRegistration::builder("hello", 1234)
            .with_portless_id_override("id-override")
            .with_ports(ports)
            .build();

        assert_eq!(service.name, "hello");
        assert_eq!(service.id(), "id-override:1234");
        assert_eq!(service.port, 1234);
        assert_eq!(*service.ports.get("grpc").unwrap(), 9001);
    }

    #[test]
    fn test_serialization() -> Result<()> {
        let service = ServiceRegistration {
            name: "test_service".into(),
            address: "test_address".into(),
            port: 18000,
            ports: HashMap::new(),
            ip: Some("123.123.123".into()),
            id_override: None,
            draining: false,
        };
        let serialized_string = serde_json::to_string(&service).unwrap();
        let deserialized_service: ServiceRegistration =
            serde_json::from_str(&serialized_string).unwrap();
        assert_eq!(deserialized_service.name, "test_service");
        assert_eq!(deserialized_service.address, "test_address");
        assert_eq!(deserialized_service.port, 18000);
        assert_eq!(deserialized_service.ports, HashMap::new());
        assert_eq!(deserialized_service.ip.unwrap(), "123.123.123");
        assert!(deserialized_service.id_override.is_none());
        Ok(())
    }

    #[test]
    fn test_serialization_with_optional_fields() -> Result<()> {
        let service = ServiceRegistration {
            name: "test_service".into(),
            address: "test_address".into(),
            port: 18000,
            ports: HashMap::new(),
            ip: None,
            id_override: None,
            draining: false,
        };
        let serialized_string = serde_json::to_string(&service).unwrap();
        let deserialized_service: ServiceRegistration =
            serde_json::from_str(&serialized_string).unwrap();
        assert_eq!(deserialized_service.name, "test_service");
        assert_eq!(deserialized_service.address, "test_address");
        assert_eq!(deserialized_service.port, 18000);
        assert_eq!(deserialized_service.ports, HashMap::new());
        assert!(deserialized_service.ip.is_none());
        assert!(deserialized_service.id_override.is_none());
        Ok(())
    }

    #[test]
    fn test_deserialization_with_optional_fields() -> Result<()> {
        let serialized_string = r#"
            {
                "name": "test_service",
                "address": "test_address",
                "port": 18000,
                "ports": {}
            }
        "#;
        let deserialized_service: ServiceRegistration =
            serde_json::from_str(&serialized_string).unwrap();
        assert_eq!(deserialized_service.name, "test_service");
        assert_eq!(deserialized_service.address, "test_address");
        assert_eq!(deserialized_service.port, 18000);
        assert_eq!(deserialized_service.ports, HashMap::new());
        assert!(deserialized_service.ip.is_none());
        assert!(deserialized_service.id_override.is_none());
        Ok(())
    }
}
