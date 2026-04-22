//! A lightweight L4 (TCP) load balancer for testing custom routing / NLB setups.
//!
//! [`NlbFrontend`] listens on a single address and randomly distributes incoming
//! connections across a set of backend addresses. It operates purely at the
//! TCP level -- no CQL frame awareness -- which matches the behaviour of
//! real cloud NLBs (AWS NLB, GCP TCP LB, etc.).
//!
//! The original NLBs used in the Cloud have multiple ports, each routing traffic
//! to given set of backend addresses. Hence, [`NlbFrontend`] corresponds to
//! a single NLB's port. Some `NlbFrontend`s will route to a single node
//! (the per-node NLB ports), and one `NlbFrontend` will load balance among
//! all nodes in a group (normally, a DC).
//!
//! # Example
//!
//! ```rust,no_run
//! # async fn example() {
//! use scylla_proxy::nlb::NlbFrontend;
//! use std::net::SocketAddr;
//!
//! let nlb = NlbFrontend::builder()
//!     .listen_addr("127.0.0.1:0".parse().unwrap())
//!     .backend("127.0.0.1:9042".parse().unwrap())
//!     .backend("127.0.0.2:9042".parse().unwrap())
//!     .backend("127.0.0.3:9042".parse().unwrap())
//!     .build();
//!
//! let running = nlb.run().await.unwrap();
//! println!("NLB listening on {}", running.listen_addr());
//! // ... use the NLB ...
//! running.finish().await;
//! # }
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;

use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{debug, error, info};

/// Builder for [`NlbFrontend`].
pub struct NlbFrontendBuilder {
    listen_addr: Option<SocketAddr>,
    backends: Vec<SocketAddr>,
}

impl NlbFrontendBuilder {
    /// Set the address the NLB will listen on.
    ///
    /// Use port 0 to let the OS pick an available port; retrieve the actual
    /// bound address via [`RunningNlbFrontend::listen_addr`].
    pub fn listen_addr(mut self, addr: SocketAddr) -> Self {
        self.listen_addr = Some(addr);
        self
    }

    /// Add a backend address. Connections are distributed across backends
    /// randomly.
    pub fn backend(mut self, addr: SocketAddr) -> Self {
        self.backends.push(addr);
        self
    }

    /// Add multiple backend addresses at once.
    pub fn backends(mut self, addrs: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.backends.extend(addrs);
        self
    }

    /// Build the [`NlbFrontend`].
    ///
    /// # Panics
    ///
    /// Panics if no listen address or no backends have been set.
    pub fn build(self) -> NlbFrontend {
        assert!(
            self.listen_addr.is_some(),
            "NlbFrontend requires a listen address"
        );
        assert!(
            !self.backends.is_empty(),
            "NlbFrontend requires at least one backend"
        );
        NlbFrontend {
            listen_addr: self.listen_addr.unwrap(),
            backends: self.backends,
        }
    }
}

/// A lightweight L4 TCP load balancer that randomly distributes connections across backends.
///
/// See the [module-level documentation](self) for details.
pub struct NlbFrontend {
    listen_addr: SocketAddr,
    backends: Vec<SocketAddr>,
}

impl NlbFrontend {
    /// Create a new [`NlbFrontendBuilder`].
    pub fn builder() -> NlbFrontendBuilder {
        NlbFrontendBuilder {
            listen_addr: None,
            backends: vec![],
        }
    }

    /// Start the NLB, binding to the configured listen address.
    ///
    /// Returns a [`RunningNlbFrontend`] handle for shutdown and address queries.
    pub async fn run(self) -> Result<RunningNlbFrontend, io::Error> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        let listen_addr = listener.local_addr()?;
        info!("NLB frontend listening on {}", listen_addr);

        let shared = Arc::new(NlbShared {
            backends: RwLock::new(self.backends),
            connection_tasks: tokio::sync::Mutex::new(JoinSet::new()),
        });

        let shared_for_handle = shared.clone();

        let handle = tokio::task::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        let connection_shared = shared.clone();
                        shared.connection_tasks.lock().await.spawn(async move {
                            let result =
                                Self::handle_connection(stream, peer_addr, &connection_shared)
                                    .await;

                            if let Err(e) = result {
                                debug!("NLB connection from {} ended: {}", peer_addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("NLB accept error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(RunningNlbFrontend {
            listen_addr,
            handle,
            shared: shared_for_handle,
        })
    }

    async fn handle_connection(
        driver_stream: TcpStream,
        driver_addr: SocketAddr,
        shared: &NlbShared,
    ) -> Result<(), io::Error> {
        // Pick a random backend.
        // Acquire the read lock only long enough to clone a SocketAddr.
        let backend_addr = {
            let backends = shared
                .backends
                .read()
                .expect("NlbShared backends RwLock poisoned");
            if backends.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "NLB has no backends configured",
                ));
            }
            let idx = rand::random_range(0..backends.len());
            backends[idx]
        };

        debug!(
            "NLB routing connection from {} to backend {}",
            driver_addr, backend_addr
        );

        let (mut driver_read, mut driver_write) = driver_stream.into_split();

        let backend_tcp = TcpStream::connect(backend_addr).await?;
        let (mut backend_read, mut backend_write) = backend_tcp.into_split();

        // --- Bidirectional copy via two concurrent tasks ---
        //
        // When one direction's `io::copy` completes (the read side sent EOF),
        // we must `shutdown()` the write side of the *other* direction so that
        // the remote peer sees EOF too.  Without this, a backend FIN (e.g.
        // when a per-node NLB shuts down) would leave the driver-side socket
        // open indefinitely -- the driver's control connection would appear
        // alive even though the backend is gone.
        let d2b = async {
            let r = io::copy(&mut driver_read, &mut backend_write).await;
            let _ = backend_write.shutdown().await;
            r
        };
        let b2d = async {
            let r = io::copy(&mut backend_read, &mut driver_write).await;
            let _ = driver_write.shutdown().await;
            r
        };

        match tokio::try_join!(d2b, b2d) {
            Ok((to_backend, to_driver)) => {
                debug!(
                    "NLB connection {} -> {} finished: {} bytes to backend, {} bytes to driver",
                    driver_addr, backend_addr, to_backend, to_driver
                );
            }
            Err(e) => {
                debug!(
                    "NLB connection {} -> {} error: {}",
                    driver_addr, backend_addr, e
                );
            }
        }

        Ok(())
    }
}

/// Shared state for the NLB frontend, accessed by all connection handlers.
struct NlbShared {
    /// Backend addresses, protected by a `RwLock` to allow dynamic updates
    /// via [`RunningNlbFrontend::set_backends`]. Uses `std::sync::RwLock`
    /// (not `tokio::sync::RwLock`) because the lock is held only long enough
    /// to clone a `SocketAddr` -- no async work under the lock.
    backends: RwLock<Vec<SocketAddr>>,
    connection_tasks: tokio::sync::Mutex<JoinSet<()>>,
}

/// Handle for a running NLB frontend. Allows querying the listen address,
/// dynamically updating backends, and shutting down the NLB.
pub struct RunningNlbFrontend {
    listen_addr: SocketAddr,
    handle: tokio::task::JoinHandle<()>,
    /// Shared state, retained so callers can update backends at runtime.
    shared: Arc<NlbShared>,
}

impl RunningNlbFrontend {
    /// Returns the address the NLB is listening on.
    ///
    /// This is particularly useful when the NLB was started with port 0,
    /// since the OS will have assigned an actual port.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Replace the set of backend addresses at runtime.
    ///
    /// New connections will be routed to the updated backends. Existing
    /// connections are unaffected (they are already piping data to the
    /// backend they were assigned at connection time).
    ///
    /// # Panics
    ///
    /// Panics if the `RwLock` is poisoned (should never happen).
    pub fn set_backends(&self, backends: Vec<SocketAddr>) {
        info!(
            "NLB on {} updating backends to: {:?}",
            self.listen_addr, backends
        );
        let mut guard = self
            .shared
            .backends
            .write()
            .expect("NlbShared backends RwLock poisoned");
        *guard = backends;
    }

    /// Shut down the NLB frontend and wait for all connection handlers to
    /// finish (or be cancelled).
    pub async fn finish(self) {
        // First stop and wait for the main task. This guarantees no new connection tasks will be spawned.
        self.handle.abort();
        let _ = self.handle.await;
        // Now we can abort and wait for all the connection tasks.
        self.shared.connection_tasks.lock().await.shutdown().await;
        info!("NLB frontend on {} has shut down", self.listen_addr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Smoke test: NLB distributes connections across two backends.
    #[tokio::test]
    async fn test_nlb_random_balancing() {
        // Start two "backend" echo servers.
        let backend1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b1_addr = backend1.local_addr().unwrap();
        let b2_addr = backend2.local_addr().unwrap();

        // Echo server that prefixes responses with a tag byte.
        fn spawn_echo(listener: TcpListener, tag: u8) {
            tokio::spawn(async move {
                loop {
                    let (mut stream, _) = match listener.accept().await {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let tag = tag;
                    tokio::spawn(async move {
                        let mut buf = [0u8; 64];
                        loop {
                            let n = match stream.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            let mut resp = vec![tag];
                            resp.extend_from_slice(&buf[..n]);
                            if stream.write_all(&resp).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });
        }

        spawn_echo(backend1, b'1');
        spawn_echo(backend2, b'2');

        // Start NLB.
        let nlb = NlbFrontend::builder()
            .listen_addr("127.0.0.1:0".parse().unwrap())
            .backend(b1_addr)
            .backend(b2_addr)
            .build();
        let running = nlb.run().await.unwrap();
        let nlb_addr = running.listen_addr();

        // Make enough connections that both backends are hit with
        // overwhelming probability (p(all same) = 2 * 0.5^20 ≈ 2e-6).
        let mut saw_backend = [false; 2];
        for _ in 0..20 {
            let mut conn = TcpStream::connect(nlb_addr).await.unwrap();
            conn.write_all(b"hi").await.unwrap();
            let mut buf = [0u8; 3];
            conn.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf[1..], b"hi");
            match buf[0] {
                b'1' => saw_backend[0] = true,
                b'2' => saw_backend[1] = true,
                other => panic!("Unexpected tag byte: {}", other),
            }
        }

        assert!(
            saw_backend[0] && saw_backend[1],
            "Expected both backends to be hit, but saw_backend = {:?}",
            saw_backend
        );

        // Clean up.
        running.finish().await;
    }
}
