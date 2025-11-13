use crate::hashring::Node as HashRingNode;
use serde::Deserialize;

#[derive(Deserialize, Debug, Default)]
pub struct RingKey(pub u64);

#[derive(Deserialize, Debug, Clone, Default)]
pub struct RingNodeName(pub String);

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum RingConfig {
    LegacyMixed(Vec<LegacyEntry>),
    Versioned(VersionedRingConfig),
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(untagged)]
pub enum LegacyEntry {
    Named(String),
    Full(HashRingNode),
}

#[derive(Deserialize, Debug)]
#[serde(tag = "schema", rename_all = "lowercase")]
pub enum VersionedRingConfig {
    V1(RingConfigV1),
}

#[derive(Deserialize, Debug, Default)]
pub struct RingConfigV1 {
    pub overrides: Option<Vec<(RingKey, RingNodeName)>>,
    pub members: Vec<HashRingNode>,
}

impl RingConfig {
    pub fn convert_to_v1(self, default_num_replicas: u32) -> RingConfigV1 {
        match self {
            RingConfig::LegacyMixed(members) => RingConfigV1 {
                members: members
                    .into_iter()
                    .map(|entry| match entry {
                        LegacyEntry::Full(hash_ring_node) => hash_ring_node,
                        LegacyEntry::Named(name) => HashRingNode::new(name, default_num_replicas),
                    })
                    .collect(),
                overrides: None,
            },
            RingConfig::Versioned(VersionedRingConfig::V1(config)) => config,
        }
    }
}

impl AsRef<str> for RingNodeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
