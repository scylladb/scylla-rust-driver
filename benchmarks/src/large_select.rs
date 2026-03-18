use crate::parametrized_select::parametrized_select_benchmark;

mod common;
mod parametrized_select;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    parametrized_select_benchmark(5000).await
}
