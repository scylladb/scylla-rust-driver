# TLS

Driver uses either the
[`openssl`](https://github.com/sfackler/rust-openssl) crate or the
[`rustls`](https://github.com/rustls/rustls) crate for TLS functionality.

Both of this features are under a their respective feaure flag.


### Enabling feature
To enable use of TLS using `openssl`, add in `Cargo.toml`:

```toml
scylla = { version = "0.4", features = ["openssl"] }
openssl = "0.10.32"
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
To use TLS you will have to a `TlsContext`. For convenience, both an
openssl
[`SslContext`](https://docs.rs/openssl/0.10.33/openssl/ssl/struct.SslContext.html)
and a rustls
[`ClientConfig`](https://docs.rs/rustls/latest/rustls/client/struct.ClientConfig.html)
can be automatically converted to a `TlsContext` when passing to
`SessionBuilder`.

For example, if database certificate is in the file `ca.crt`:
```rust
# extern crate scylla;
# extern crate openssl;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use openssl::ssl::{SslContextBuilder, SslMethod, SslVerifyMode};
use std::path::PathBuf;

# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
let mut context_builder = SslContextBuilder::new(SslMethod::tls())?;
context_builder.set_ca_file("ca.crt")?;
context_builder.set_verify(SslVerifyMode::PEER);

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9142") // The the port is now 9142
    .tls_context(Some(context_builder.build()))
    .build()
    .await?;

# Ok(())
# }
```

See the full [example](https://github.com/scylladb/scylla-rust-driver/blob/main/examples/tls.rs) for more details
