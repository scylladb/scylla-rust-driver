mod lib;

mod authenticate;
#[cfg(feature = "unstable-client-routes")]
mod client_routes;
mod example;
#[cfg(all(scylla_unstable, feature = "unstable-host-listener"))]
mod host_listener;

#[cfg(all(feature = "openssl-010", feature = "rustls-023"))]
mod tls;

mod result_metadata_extension;
