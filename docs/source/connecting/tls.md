# TLS

Driver uses the [`openssl`](https://github.com/sfackler/rust-openssl) crate for TLS functionality.\
It was chosen because [`rustls`](https://github.com/ctz/rustls) doesn't support certificates for ip addresses
(see [issue](https://github.com/briansmith/webpki/issues/54)), which is a common use case for Scylla.


### Enabling feature
`openssl` is not a pure Rust library so you need enable a feature and install the proper package.

To enable the `tls` feature add in `Cargo.toml`:
```toml
scylla = { version = "0.2.0", features = ["ssl"] }
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
To use tls you will have to create an openssl 
[`SslContext`](https://docs.rs/openssl/0.10.33/openssl/ssl/struct.SslContext.html)
and pass it to `SessionBuilder`

For example, if database certificate is in the file `ca.crt`:
```rust
# extern crate scylla;
# extern crate openssl;
use scylla::{Session, SessionBuilder};
use openssl::ssl::{SslContextBuilder, SslMethod, SslVerifyMode};
use std::path::PathBuf;

# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
let mut context_builder = SslContextBuilder::new(SslMethod::tls())?;
context_builder.set_ca_file("ca.crt")?;
context_builder.set_verify(SslVerifyMode::PEER);

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9142") // The the port is now 9142
    .ssl_context(Some(context_builder.build()))
    .build()
    .await?;

# Ok(())
# }
```

See the full [example](https://github.com/scylladb/scylla-rust-driver/blob/main/examples/tls.rs) for more details
