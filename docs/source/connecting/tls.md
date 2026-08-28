# TLS

Driver uses either the
[`openssl`](https://github.com/sfackler/rust-openssl) crate or the
[`rustls`](https://github.com/rustls/rustls) crate for TLS functionality.

Both of this features are behind their respective feature flag.

## Hostname verification

For both implementations we provide node IP address for purposes of hostname verification.
Our assumption is that certificates on nodes will have node IP address in the subject alternative name.

Implementation details (might change in the future):
For openssl we use `set_ip` method on `X509VerifyParamRef`, which corresponds to `X509_VERIFY_PARAM_set1_ip` openssl function.
For rustls, we use `ServerName::IpAddress`, which is passed to `ClientConnection::new_with_alpn` (by `tokio_rustls`).


### Enabling feature

**_NOTE:_** `openssl` is not a pure Rust library, so you need to **both** enable a feature **and** install the proper package.

To enable use of TLS using `openssl`, add in `Cargo.toml`:

```toml
scylla = { version = "1.8", features = ["openssl-010"] }
openssl = "0.10.70"
```

Then install the package with `openssl`:
* Debian/Ubuntu:
    ```bash
    apt install libssl-dev pkg-config
    ```
* Fedora:
    ```bash
    dnf install openssl-devel
    ```
<!--
 scylla-rust-driver doesn't build on Alpine, some strange cc linker errors in proc-macro-hack 0_o
 TODO: try building and add the section

 * Alpine:
    ```bash
    apk add openssl-dev
    ```
-->
* Arch:
    ```bash
    pacman -S openssl pkg-config
    ```

### Using TLS
To use TLS you will have to create a `TlsContext`. For the openssl backend, build an
[`SslConnector`](https://docs.rs/openssl/0.10/openssl/ssl/struct.SslConnector.html)
builder and wrap it in an `OpenSsl010Config`; for rustls, an `Arc` of a
[`ClientConfig`](https://docs.rs/rustls/latest/rustls/client/struct.ClientConfig.html)
is automatically converted to a `TlsContext` when passing it to `SessionBuilder`.

**_NOTE:_** `SslConnector` is the recommended openssl API, because it has safe defaults
(most notably, it verifies the peer's certificate). If you really need full control, you
can build the context from a raw `SslContextBuilder` with
`OpenSsl010Config::from_dangerous_builder`, which has no such defaults.

**_NOTE:_** `SslConnector` also trusts the system's CA store, because it calls
`set_default_verify_paths`. If your cluster has its own CA, that may be wider than you want: a
certificate chaining to any public root would be accepted as long as it carries the node's IP
address in its subject alternative name. Replace that trust with `set_cert_store`, as below.
Beware that `set_ca_file` *adds* to the trusted set rather than replacing it, so it does not
narrow anything down on its own.

**_NOTE:_** Passing an already-built `SslContext` (`TlsContext::OpenSsl010`) still works, but is
deprecated since 1.9.0: the driver cannot configure a context it did not build, so such a context
cannot support TLS session tickets.

For example, if database certificate is in the file `ca.crt`:
```rust
# extern crate scylla;
# extern crate openssl;
use scylla::client::session::{OpenSsl010Config, Session};
use scylla::client::session_builder::SessionBuilder;
use openssl::ssl::{SslConnector, SslMethod};
use openssl::x509::X509;
use openssl::x509::store::X509StoreBuilder;

# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
let mut builder = SslConnector::builder(SslMethod::tls())?;

// Trust the cluster's CA, and nothing else.
let mut ca_store = X509StoreBuilder::new()?;
for cert in X509::stack_from_pem(&std::fs::read("ca.crt")?)? {
    ca_store.add_cert(cert)?;
}
builder.set_cert_store(ca_store.build());

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9142") // The port is now 9142
    .tls_context(Some(OpenSsl010Config::new(builder)))
    .build()
    .await?;

# Ok(())
# }
```

### TLS session tickets

A context built through `OpenSsl010Config` resumes TLS sessions by default: the driver banks the
session tickets the cluster issues (per node, respecting the TLS 1.3 single-use rule) and offers
them on later connections. A TLS 1.2 ticket is reusable, so every connection after the first
resumes; a TLS 1.3 ticket is spent on one connection, so only as many resume as the server issued
tickets. The rustls backend does this natively, with no configuration.

To turn it off:

```rust
# extern crate scylla;
# extern crate openssl;
use scylla::client::session::OpenSsl010Config;
use openssl::ssl::{SslConnector, SslMethod};

# use std::error::Error;
# fn check_only_compiles() -> Result<(), Box<dyn Error>> {
let builder = SslConnector::builder(SslMethod::tls())?;
let context = OpenSsl010Config::new(builder).set_use_tls_tickets(false);
# Ok(())
# }
```

The deprecated `TlsContext::OpenSsl010` never resumes: the driver cannot install a session
callback on a context it did not build.

See the full [openssl example](https://github.com/scylladb/scylla-rust-driver/blob/main/examples/tls_openssl.rs) and [rustls example](https://github.com/scylladb/scylla-rust-driver/blob/main/examples/tls_rustls.rs) for more details.
