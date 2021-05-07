use anyhow::Result;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use scylla::transport::Compression;
use scylla::{QueryResult, Session, SessionBuilder};
use std::env;

fn print_result(result: &QueryResult) {
    if result.rows.is_none() {
        println!("OK");
        return;
    }
    for row in result.rows.as_ref().unwrap() {
        for column in &row.columns {
            print!("|");
            print!(
                " {:16}",
                match column {
                    None => "null".to_owned(),
                    Some(value) => format!("{:?}", value),
                }
            );
        }
        println!("|")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .compression(Some(Compression::Lz4))
        .build()
        .await?;

    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }
                rl.add_history_entry(line.as_str());
                let maybe_res = session.query(line, &[]).await;
                match maybe_res {
                    Err(err) => println!("Error: {}", err),
                    Ok(res) => print_result(&res),
                }
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(err) => println!("Error: {}", err),
        }
    }
    Ok(())
}
