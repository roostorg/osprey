#![cfg(feature = "test_server")]

use std::convert::TryInto;
use std::ffi::OsStr;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use etcd::Client as InnerClient;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tempfile::TempDir;
use tokio::net::TcpSocket;
use tokio::process::{Child, Command};

use crate::{Client, EtcdError};

/// Builder for [`Server`].
#[derive(Clone, Debug)]
pub struct ServerBuilder<'a> {
    exe: &'a OsStr,
    data_dir: Option<&'a Path>,
}

impl<'a> Default for ServerBuilder<'a> {
    fn default() -> Self {
        ServerBuilder {
            exe: OsStr::new("etcd"),
            data_dir: None,
        }
    }
}

impl<'a> ServerBuilder<'a> {
    /// Sets the etcd binary to use.
    /// By default, the etcd from `PATH` will be used.
    pub fn executable_path(&'a mut self, path: &'a (impl AsRef<Path> + ?Sized)) -> &'a mut Self {
        self.exe = path.as_ref().as_os_str();
        self
    }

    /// Sets the path to the data directory to use.
    /// By default, a temporary directory will be created for the process
    /// and deleted when the [`Server`] is `Drop`ped.
    pub fn data_directory(&'a mut self, path: &'a (impl AsRef<Path> + ?Sized)) -> &'a mut Self {
        self.data_dir = Some(path.as_ref());
        self
    }

    /// Starts a new etcd process, waits for it to be ready,
    /// then returns its [`Server`] handle.
    pub fn start(&self) -> impl Future<Output = Result<Server, EtcdError>> + Send + 'static {
        let mut command = Command::new(self.exe);
        command.stdin(Stdio::null());
        command.stdout(Stdio::null());
        command.kill_on_drop(true);

        let result = (|| -> Result<Server, EtcdError> {
            let port = pick_unused_port()?;
            let peer_port = pick_unused_port()?;
            let endpoint_url = format!("http://localhost:{}", port);
            command.arg(format!("--listen-client-urls={}", &endpoint_url));
            command.arg(format!("--listen-peer-urls=http://localhost:{}", peer_port));
            command.arg(format!(
                "--advertise-client-urls=http://localhost:{}",
                peer_port
            ));
            let data_dir = if let Some(data_dir) = self.data_dir {
                command.arg(format!("--data-dir={}", data_dir.display()));
                None
            } else {
                let dir = tempfile::tempdir()?;
                command.arg(format!("--data-dir={}", dir.path().display()));
                Some(dir)
            };
            Ok(Server {
                endpoint_url,
                child: command.spawn()?,
                _data_dir: data_dir,
            })
        })();

        async move {
            let mut server = result?;

            let client = InnerClient::new(&[server.endpoint_url()]);
            let wait_future = server.child.wait();
            let healthy_future = wait_for_etcd_healthy(&client);

            tokio::select! {
                wait_result = wait_future => {
                    match wait_result {
                        Ok(status) => Err(EtcdError::IoError { error: std::io::Error::new(std::io::ErrorKind::Other, format!("etcd exited with {}", status)) }),
                        Err(error) => Err(EtcdError::IoError { error })
                    }
                },
                _ = healthy_future => {
                    Ok(server)
                },
            }
        }
    }
}

/// A running etcd server.
///
/// Dropping the `Server` will send a kill signal to the subprocess
/// but not wait on it to exit.
/// You should try to call [`Server::stop`] whenever possible.
#[derive(Debug)]
pub struct Server {
    endpoint_url: String,
    child: Child,
    _data_dir: Option<TempDir>, // deleted on Drop
}

impl Server {
    /// Creates a new builder.
    #[inline]
    pub fn builder<'a>() -> ServerBuilder<'a> {
        ServerBuilder::default()
    }

    /// Sends an interrupt signal to the etcd process,
    /// then returns a [`Future`] that resolves when the subprocess has exited.
    ///
    /// # Errors
    ///
    /// If signalling the subprocess fails, then an error will be returned.
    pub fn stop(mut self) -> impl Future<Output = Result<(), EtcdError>> + Send + 'static {
        let pid = Pid::from_raw(
            self.child
                .id()
                .expect("child already stopped")
                .try_into()
                .expect("invalid pid"),
        );
        let kill_result = signal::kill(pid, Signal::SIGTERM);
        async move {
            if let Err(errno) = kill_result {
                Err(std::io::Error::from(errno).into())
            } else {
                self.child.wait().await.ok();
                Ok(())
            }
        }
    }

    /// Returns the server's etcd endpoint URL.
    #[inline]
    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    /// Create a new client with the server as its sole endpoint.
    #[inline]
    pub fn new_client(&self) -> anyhow::Result<Client> {
        Client::new(&[&self.endpoint_url])
    }
}

async fn wait_for_etcd_healthy(client: &InnerClient) {
    loop {
        if client.health().await.iter().all(Result::is_ok) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn pick_unused_port() -> std::io::Result<u16> {
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0))?;
    Ok(socket.local_addr()?.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn startup() {
        if let Err(err) = which::which("etcd") {
            eprintln!("etcd not found (skipping): {}", err);
            return;
        }
        let server = Server::builder().start().await.unwrap();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn basic_read() {
        if let Err(err) = which::which("etcd") {
            eprintln!("etcd not found (skipping): {}", err);
            return;
        }
        let server = Server::builder().start().await.unwrap();
        // Use raw client to avoid conflating with potential client bugs.
        let client = InnerClient::new(&[server.endpoint_url()]);
        etcd::kv::create(&client, "/foo", "bar", None)
            .await
            .unwrap();
        let got = etcd::kv::get(&client, "/foo", Default::default())
            .await
            .unwrap();
        assert_eq!(
            got.data.node.value.as_ref().map(String::as_str),
            Some("bar")
        );
        server.stop().await.unwrap();
    }
}
