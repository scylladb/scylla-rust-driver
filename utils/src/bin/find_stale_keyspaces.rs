use anyhow::anyhow;
use futures::{stream, StreamExt};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;
    session.refresh_metadata().await.unwrap();
    let state = session.get_cluster_state();
    let keyspaces_stream = stream::iter(state.keyspaces_iter());
    let nr_stale = keyspaces_stream
        .filter_map(|(name, _ks)| {
            let session = &session;
            async move {
                if !name.starts_with("test_rust_") {
                    return None;
                }
                let describe_result = session
                    .query_unpaged(format!("DESCRIBE {name}"), &[])
                    .await
                    .unwrap()
                    .into_rows_result()
                    .unwrap();
                println!("Found stale keyspace: {name}");
                println!("Schema:");
                for (_, _, _, describe_text) in describe_result
                    .rows::<(&str, &str, &str, &str)>()
                    .unwrap()
                    .map(|r| r.unwrap())
                {
                    println!("{describe_text}");
                }

                Some(())
            }
        })
        .count()
        .await;

    if nr_stale > 0 {
        return Err(anyhow!("Found stale keyspaces"));
    }

    Ok(())
}
