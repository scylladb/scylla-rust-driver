# Creating a project

To create a new project run:
```shell
cargo new myproject
```

In `Cargo.toml` add useful dependencies:
```toml
[dependencies]
scylla = "1.3"
tokio = { version = "1.12", features = ["full"] }
futures = "0.3.6"
uuid = "1.0"
bigdecimal = "0.4"
num-bigint = "0.3"
tracing = "0.1.36"
tracing-subscriber = { version = "0.3.14", features = ["env-filter"] }
```

In `main.rs` put:
```rust
# extern crate scylla;
# extern crate tokio;
use scylla::client::session::Session;

#[tokio::main]
async fn main() {
    println!("Hello scylla!");
}
```

Now running `cargo run` should print:
```shell
Hello scylla!
```
