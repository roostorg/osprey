use std::{net::SocketAddr, time::Duration};

use crate::etcd::Client;
use crate::etcd_watcherd::rpc::server::{EtcdWatcherdServiceImpl, EtcdWatcherdServiceServer};
use crate::etcd_watcherd::Watcher;
use anyhow::Result;
use clap::Parser;
use tonic::transport::Server;

/// `etcd_watcherd` is an etcd watch multiplexer, establishing a single watcher to etcd for each key
/// requested to be watched by the clients.
///
/// This server is meant to be deployed on the same nodes those Python processes run on, either as a side-car,
/// or a daemon-set.
///
/// The watcher will listen on the provided port, and multiplex connections to the etcd cluster whose peers
/// are specified in the environmental variable `ETCD_PEERS`.
#[derive(Parser)]
struct Opt {
    /// The address that the `etcd_watcherd` server should bind to.
    #[arg(long, default_value = "127.0.0.1:2375")]
    bind_addr: SocketAddr,

    /// If provided, overrides the peers specified by `ETCD_PEERS`.
    #[arg(long)]
    etcd_peers: Vec<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    let client = if opt.etcd_peers.is_empty() {
        Client::from_etcd_peers()?
    } else {
        Client::new(&opt.etcd_peers)?
    };

    let metrics = metrics::new_client("etcd_watcherd.rpc_server")?;
    let watcher = Watcher::new(client);
    let _metrics_guard = watcher.emit_metrics(metrics);
    let watcherd_impl = EtcdWatcherdServiceImpl::new(watcher);

    println!("[etcd_watcherd] serving on {:?}", opt.bind_addr);

    Server::builder()
        .tcp_keepalive(Some(Duration::from_secs(10)))
        .add_service(EtcdWatcherdServiceServer::new(watcherd_impl))
        .serve(opt.bind_addr)
        .await?;

    Ok(())
}
