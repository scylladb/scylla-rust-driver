use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::Row;
use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use tower::Service;

struct SessionService {
    session: Arc<Session>,
}

// A trivial service implementation for sending parameterless simple string requests to Scylla.
impl Service<scylla::statement::query::Query> for SessionService {
    type Response = scylla::response::query_result::QueryResult;
    type Error = scylla::errors::ExecutionError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: scylla::statement::query::Query) -> Self::Future {
        let session = self.session.clone();
        Box::pin(async move { session.query_unpaged(req, &[]).await })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);
    let mut session: SessionService = SessionService {
        session: Arc::new(SessionBuilder::new().known_node(uri).build().await?),
    };

    let rows_result = session
        .call("SELECT keyspace_name, table_name FROM system_schema.tables;".into())
        .await?
        .into_rows_result()?;

    let print_text = |t: &Option<scylla::value::CqlValue>| {
        t.as_ref()
            .unwrap_or(&scylla::value::CqlValue::Text("<null>".to_string()))
            .as_text()
            .unwrap_or(&"<null>".to_string())
            .clone()
    };

    println!(
        "Tables:\n{}",
        rows_result
            .rows::<Row>()?
            .map(|r| r.map(|r| format!(
                "\t{}.{}",
                print_text(&r.columns[0]),
                print_text(&r.columns[1])
            )))
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
            .join("\n")
    );
    Ok(())
}
