use anyhow::Result;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::{CompletionType, Config, Context, Editor};
use rustyline_derive::{Helper, Highlighter, Hinter, Validator};
use scylla::frame::response::result::Row;
use scylla::session::Session;
use scylla::transport::query_result::IntoRowsResultError;
use scylla::transport::Compression;
use scylla::QueryResult;
use scylla::SessionBuilder;
use std::env;

#[derive(Helper, Highlighter, Validator, Hinter)]
struct CqlHelper;

const CQL_KEYWORDS: &[&str] = &[
    "ADD",
    "AGGREGATE",
    "ALL",
    "ALLOW",
    "ALTER",
    "AND",
    "ANY",
    "APPLY",
    "AS",
    "ASC",
    "ASCII",
    "AUTHORIZE",
    "BATCH",
    "BEGIN",
    "BIGINT",
    "BLOB",
    "BOOLEAN",
    "BY",
    "CLUSTERING",
    "COLUMNFAMILY",
    "COMPACT",
    "CONSISTENCY",
    "COUNT",
    "COUNTER",
    "CREATE",
    "CUSTOM",
    "DECIMAL",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "EACH_QUORUM",
    "ENTRIES",
    "EXISTS",
    "FILTERING",
    "FLOAT",
    "FROM",
    "FROZEN",
    "FULL",
    "GRANT",
    "IF",
    "IN",
    "INDEX",
    "INET",
    "INFINITY",
    "INSERT",
    "INT",
    "INTO",
    "KEY",
    "KEYSPACE",
    "KEYSPACES",
    "LEVEL",
    "LIMIT",
    "LIST",
    "LOCAL_ONE",
    "LOCAL_QUORUM",
    "MAP",
    "MATERIALIZED",
    "MODIFY",
    "NAN",
    "NORECURSIVE",
    "NOSUPERUSER",
    "NOT",
    "OF",
    "ON",
    "ONE",
    "ORDER",
    "PARTITION",
    "PASSWORD",
    "PER",
    "PERMISSION",
    "PERMISSIONS",
    "PRIMARY",
    "QUORUM",
    "RENAME",
    "REVOKE",
    "SCHEMA",
    "SELECT",
    "SET",
    "STATIC",
    "STORAGE",
    "SUPERUSER",
    "TABLE",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TIMEUUID",
    "THREE",
    "TO",
    "TOKEN",
    "TRUNCATE",
    "TTL",
    "TUPLE",
    "TWO",
    "TYPE",
    "UNLOGGED",
    "UPDATE",
    "USE",
    "USER",
    "USERS",
    "USING",
    "UUID",
    "VALUES",
    "VARCHAR",
    "VARINT",
    "VIEW",
    "WHERE",
    "WITH",
    "WRITETIME",
    // Scylla-specific
    "BYPASS",
    "CACHE",
    "SERVICE",
    "LEVEL",
    "LEVELS",
    "ATTACH",
    "ATTACHED",
    "DETACH",
    "TIMEOUT",
    "FOR",
    "PER",
    "PARTITION",
    "LIKE",
];

impl Completer for CqlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        if !line.is_empty() {
            let start: usize = line[..pos].rfind(' ').map_or(0, |p| p + 1);
            let prefix = &line[start..pos].to_uppercase();
            // NOTICE: yes, linear, but still fast enough for a cli
            // TODO:
            //  * completion from schema information
            //  * completion with context - e.g. INTO only comes after INSERT
            //  * completion for internal commands (once implemented)
            if !prefix.is_empty() {
                let mut matches: Vec<Pair> = Vec::new();
                for keyword in CQL_KEYWORDS {
                    if keyword.starts_with(prefix) {
                        matches.push(Pair {
                            display: keyword.to_string(),
                            replacement: format!("{} ", keyword),
                        })
                    }
                }
                if !matches.is_empty() {
                    return Ok((start, matches));
                }
            }
        }
        Ok((0, vec![]))
    }
}

fn print_result(result: QueryResult) -> Result<(), IntoRowsResultError> {
    match result.into_rows_result() {
        Ok(rows_result) => {
            for row in rows_result.rows::<Row>().unwrap() {
                let row = row.unwrap();
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
                println!("|");
            }
            Ok(())
        }
        Err(IntoRowsResultError::ResultNotRows(_)) => {
            println!("OK");
            Ok(())
        }
        Err(e) => Err(e),
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

    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .build();
    let mut rl = Editor::with_config(config);
    rl.set_helper(Some(CqlHelper {}));
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }
                rl.add_history_entry(line.as_str());
                let maybe_res = session.query_unpaged(line, &[]).await;
                match maybe_res {
                    Err(err) => println!("Error: {}", err),
                    Ok(res) => print_result(res)?,
                }
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(err) => println!("Error: {}", err),
        }
    }
    Ok(())
}
