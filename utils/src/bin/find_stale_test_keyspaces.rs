use futures::{StreamExt, stream};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

#[tokio::main]
async fn main() {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build()
        .await
        .expect("Can't connect to cluster");
    session
        .refresh_metadata()
        .await
        .expect("Can't refresh metadata");
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
                    .expect("Can't perform DESCRIBE request")
                    .into_rows_result()
                    .expect("Wrong response type for DESCRIBE request");
                println!("Found stale keyspace: {name}");
                println!("Schema:");
                for (_, _, _, describe_text) in describe_result
                    .rows::<(&str, &str, &str, &str)>()
                    .expect("Wrong response metadata for DESCRIBE request")
                    .map(|r| r.expect("Failed to deserialize a row of RESPONSE request"))
                {
                    println!("{describe_text}");
                }

                Some(())
            }
        })
        .count()
        .await;

    if nr_stale > 0 {
        panic!("Found stale keyspaces");
    }
}
