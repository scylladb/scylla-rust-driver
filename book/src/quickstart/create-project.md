# Creating a project

To create a new project run:
```shell
cargo new myproject
```

In `Cargo.toml` add required dependencies:
```toml
[dependencies]
scylla = { git = "https://github.com/scylladb/scylla-rust-driver", branch = "main" }
tokio = { version = "1.1.0", features = ["full"] }
futures = "0.3.6"
```
> Note that when specifying a dependency as a git link, updates will not be automatically pulled.
> Running `cargo update` will update the git dependency manually.

In `main.rs` put:
```rust
# extern crate scylla;
# extern crate tokio;
use scylla::Session;

#[tokio::main]
async fn main() {
    println!("Hello scylla!");
}
```

Now running `cargo run` should print:
```shell
Hello scylla!
```