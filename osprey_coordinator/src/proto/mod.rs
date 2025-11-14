#![allow(unused_imports)]

pub mod osprey {
    pub mod rpc {
        pub mod actions {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/osprey.rpc.actions.v1.rs"));
            }
        }
        pub mod common {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/osprey.rpc.common.v1.rs"));
            }
        }

        pub mod labels {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/osprey.rpc.labels.v1.rs"));
            }
        }

        pub mod osprey_coordinator {
            pub mod bidirectional_stream {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/osprey.rpc.osprey_coordinator.bidirectional_stream.v1.rs"
                    ));
                }
            }

            pub mod sync_action {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/osprey.rpc.osprey_coordinator.sync_action.v1.rs"
                    ));
                }
            }
        }
    }
}


pub use osprey::rpc::actions::v1::*;
pub use osprey::rpc::common::v1::*;
pub use osprey::rpc::labels::v1::*;
pub use osprey::rpc::osprey_coordinator::bidirectional_stream::v1::*;
pub use osprey::rpc::osprey_coordinator::sync_action::v1 as osprey_coordinator_sync_action;
pub const PB_DESCRIPTOR_BYTES: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));
