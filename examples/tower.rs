use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use tower::Service;

struct SessionService {
    session: Arc<scylla::Session>,
}

// A trivial service implementation for sending parameterless simple string requests to Scylla.
impl Service<scylla::query::Query> for SessionService {
    type Response = scylla::QueryResult;
    type Error = scylla::execution::errors::QueryError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: scylla::query::Query) -> Self::Future {
        let session = self.session.clone();
        Box::pin(async move { session.query(req, &[]).await })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);
    let mut session: SessionService = SessionService {
        session: Arc::new(
            scylla::SessionBuilder::new()
                .known_node(uri)
                .build()
                .await?,
        ),
    };

    let resp = session
        .call("SELECT keyspace_name, table_name FROM system_schema.tables;".into())
        .await?;

    let print_text = |t: &Option<scylla::frame::response::result::CqlValue>| {
        t.as_ref()
            .unwrap_or(&scylla::frame::response::result::CqlValue::Text(
                "<null>".to_string(),
            ))
            .as_text()
            .unwrap_or(&"<null>".to_string())
            .clone()
    };

    println!(
        "Tables:\n{}",
        resp.rows()?
            .into_iter()
            .map(|r| format!(
                "\t{}.{}",
                print_text(&r.columns[0]),
                print_text(&r.columns[1])
            ))
            .collect::<Vec<String>>()
            .join("\n")
    );
    Ok(())
}
