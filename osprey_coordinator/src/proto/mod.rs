#![allow(unused_imports)]

pub mod discord_smite_rpc {
    pub mod actions {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/discord_smite_rpc.actions.v1.rs"));
        }
    }

    pub mod common {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/discord_smite_rpc.common.v1.rs"));
        }
    }

    pub mod labels {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/discord_smite_rpc.labels.v1.rs"));
        }
    }

    pub mod smite_coordinator {
        pub mod bidirectional_stream {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/discord_smite_rpc.smite_coordinator.bidirectional_stream.v1.rs"
                ));
            }
        }

        pub mod sync_action {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/discord_smite_rpc.smite_coordinator.sync_action.v1.rs"
                ));
            }
        }
    }
}

pub mod discord_protos {
    pub mod discord_authentication {
        pub mod v1 {
            include!(concat!(
                env!("OUT_DIR"),
                "/discord_protos.discord_authentication.v1.rs"
            ));
        }
    }

    pub mod users {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/discord_protos.users.v1.rs"));
        }
    }
}

pub use discord_smite_rpc::actions::v1::*;
pub use discord_smite_rpc::common::v1::*;
pub use discord_smite_rpc::labels::v1::*;
pub use discord_smite_rpc::smite_coordinator::bidirectional_stream::v1::*;
pub use discord_smite_rpc::smite_coordinator::sync_action::v1 as smite_coordinator_sync_action;

pub const PB_DESCRIPTOR_BYTES: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));
