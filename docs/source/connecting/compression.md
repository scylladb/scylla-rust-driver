# Compression

By default the driver does not use any compression on connections.\
It's possible to specify a preferred compression algorithm. \
The driver will try using it, but if the database doesn't support it, it will fall back to no compression.

Available compression algorithms:
* Snappy
* LZ4

An example enabling `Snappy` compression algorithm:
```rust
# extern crate scylla;
# extern crate tokio;
use scylla::{Session, SessionBuilder};
use scylla::transport::Compression;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .compression(Some(Compression::Snappy))
        .build()
        .await?;

    Ok(())
}
```