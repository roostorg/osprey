use crate::etcd_watcherd::{KeyWatchEvents, RecursiveKeyWatchEvents, Watcher};
use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Subcommand)]
enum Command {
    /// Watches a single key for changes, using [`Watcher::watch_key`]
    WatchKey {
        #[arg(long)]
        key: String,
    },
    /// Recursively watches a directory for changes, using [`Watcher::watch_key_recursive`].
    WatchKeyRecursive {
        #[arg(long)]
        key: String,
    },
}

/// A cli that uses the Watcher provided by etcd_watcherd. Different than `rpc_client`,
/// which connects to an already running server.
///
/// Prints out events from watched keys in real time. This cli should really not be used,
/// and more or less serves as an example of using the [`Watcher`] API.
#[derive(Parser)]
struct Opt {
    /// The etcd peers to connect to. If not provided, will look in the `ETCD_PEERS` environment
    /// variable.
    #[arg(long)]
    etcd_peers: Vec<String>,

    #[command(subcommand)]
    command: Command,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    let etcd_client = if opt.etcd_peers.is_empty() {
        crate::etcd::Client::from_etcd_peers()?
    } else {
        crate::etcd::Client::new(&opt.etcd_peers)?
    };
    let watcher = Watcher::new(etcd_client);

    match opt.command {
        Command::WatchKey { key } => {
            let handle = watcher.watch_key(key.clone()).await?;

            let mut events = handle.events();
            loop {
                let KeyWatchEvents::FullSync { value } = events.next().await;
                println!("{:?} -> {:?}", key, value);
            }
        }
        Command::WatchKeyRecursive { key } => {
            let response = watcher.watch_key_recursive(key.clone()).await?;
            let mut events = response.events();

            loop {
                let event = events.next().await;
                match event {
                    RecursiveKeyWatchEvents::FullSync { items } => {
                        println!("full-sync: {:#?}", items)
                    }
                    RecursiveKeyWatchEvents::SyncOne { key, value } => {
                        println!("sync-one: {:?} -> {:?}", key, value)
                    }
                    RecursiveKeyWatchEvents::DeleteOne { key, prev_value } => {
                        println!("delete-one: {:?} (prev: {:?})", key, prev_value)
                    }
                }
            }
        }
    }
}
