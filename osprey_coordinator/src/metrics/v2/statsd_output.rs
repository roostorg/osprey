use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::ops::{Deref, DerefMut};

#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) struct StatsdOutput(HashMap<Vec<u8>, u64>);

impl StatsdOutput {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_packets(packets: impl IntoIterator<Item = Vec<u8>>) -> Self {
        packets.into_iter().fold(Self::new(), |mut m, packet| {
            for line in packet.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                m.entry(Vec::from(line))
                    .and_modify(|n| *n += 1)
                    .or_insert(1);
            }
            m
        })
    }
}

impl Deref for StatsdOutput {
    type Target = HashMap<Vec<u8>, u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StatsdOutput {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Debug for StatsdOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut entries: Vec<(&[u8], u64)> =
            self.0.iter().map(|(k, v)| (k.as_slice(), *v)).collect();
        entries.sort_by(|a, b| a.cmp(b));
        f.debug_map()
            .entries(
                entries
                    .into_iter()
                    .map(|(k, v)| (String::from_utf8_lossy(k), v)),
            )
            .finish()
    }
}

impl<const N: usize> From<[(Vec<u8>, u64); N]> for StatsdOutput {
    fn from(arr: [(Vec<u8>, u64); N]) -> Self {
        StatsdOutput(HashMap::from(arr))
    }
}
