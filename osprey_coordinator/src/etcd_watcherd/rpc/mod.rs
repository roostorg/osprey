mod proto {
    pub mod v1 {
        tonic::include_proto!("discord_common.etcd_watcherd.v1");
    }
}

pub mod client;
pub mod server;
