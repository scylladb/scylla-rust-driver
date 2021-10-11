# Creating a project

To create a new project run:
```shell
cargo new myproject
```

In `Cargo.toml` add useful dependencies:
```toml
[dependencies]
scylla = "0.2.0"
tokio = { version = "1.1.0", features = ["full"] }
futures = "0.3.6"
uuid = "0.8.1"
bigdecimal = "0.2.0"
num-bigint = "0.3"
tracing = "0.1.25"
tracing-subscriber = "0.2.16"
```

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