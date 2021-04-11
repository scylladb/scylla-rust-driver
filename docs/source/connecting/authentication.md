# Authentication

Driver supports authentication by username and password.

To specify credentials use the `user` method in `SessionBuilder`:

```rust
# extern crate scylla;
# extern crate tokio;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .user("myusername", "mypassword")
    .build()
    .await?;

# Ok(())
# }
```