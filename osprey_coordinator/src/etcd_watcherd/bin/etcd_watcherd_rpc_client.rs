use anyhow::Result;

use crate::etcd_watcherd::rpc::client::*;
use clap::{Parser, Subcommand};
use watch_key_recursive_response::Event;

#[derive(Subcommand)]
enum Command {
    /// Watches a single key for changes.
    WatchKey {
        #[arg(long)]
        key: String,
    },
    /// Recursively watches a directory for changes.
    WatchKeyRecursive {
        #[arg(long)]
        key: String,
    },
}

/// A cli client for `etcd_watcherd`.
///
/// Prints out events from watched keys in real time.
#[derive(Parser)]
struct Opt {
    /// The address of the watcherd server to connect to.
    #[arg(long, default_value = "http://localhost:2375")]
    watcherd_server: String,

    #[command(subcommand)]
    command: Command,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    let mut client = EtcdWatcherdServiceClient::connect(opt.watcherd_server).await?;
    match opt.command {
        Command::WatchKey { key } => {
            let response = client
                .watch_key(WatchKeyRequest { key: key.clone() })
                .await?;

            let mut response = response.into_inner();
            while let Some(message) = response.message().await? {
                println!("{:?} -> {:?}", key, message.value);
            }
        }
        Command::WatchKeyRecursive { key } => {
            let response = client
                .watch_key_recursive(WatchKeyRecursiveRequest { key })
                .await?;

            let mut response = response.into_inner();
            while let Some(message) = response.message().await? {
                match message.event.unwrap() {
                    Event::FullSync(s) => {
                        println!("full-sync: {:#?}", s.items)
                    }
                    Event::SyncOne(s) => {
                        println!("sync-one: {:?} -> {:?}", s.key, s.value)
                    }
                    Event::DeleteOne(s) => {
                        println!("delete-one: {:?} (prev: {:?})", s.key, s.prev_value)
                    }
                }
            }
        }
    }

    Ok(())
}
