#[macro_use]
extern crate anyhow;

use anyhow::Result;
use getopts::Options;
use scylla::frame::response::result::CQLValue;
use scylla::transport::session::Session;
use scylla::transport::Compression;
use std::env;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Workload {
    Reads,
    Writes,
    ReadsAndWrites,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("", "node", "cluster contact node", "ADDRESS");
    opts.optopt("", "concurrency", "workload concurrency", "COUNT");
    opts.optopt("", "tasks", "task count", "COUNT");
    opts.optopt("", "replication-factor", "replication factor", "FACTOR");
    opts.optopt("", "consistency", "consistency level", "CONSISTENCY");
    opts.optopt(
        "",
        "compression",
        "compression algorithm to use (none, lz4 or snappy)",
        "ALGORITHM",
    );

    opts.optopt(
        "",
        "workload",
        "workload type (write, read or mixed)",
        "TYPE",
    );

    opts.optflag("", "prepare", "prepare keyspace and table before the bench");
    opts.optflag("", "run", "run the benchmark");

    opts.optflag("h", "help", "print this help menu");
    let matches = opts.parse(&args[..]).unwrap();

    if matches.opt_present("prepare") == matches.opt_present("run") {
        return Err(anyhow!(
            "please specify exactly one of the following flags: --prepare, --run"
        ));
    }

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return Ok(());
    }

    let node = matches
        .opt_str("node")
        .unwrap_or_else(|| "127.0.0.1:9042".to_owned());
    let concurrency = matches.opt_get_default("concurrency", 256)?;
    let tasks = matches.opt_get_default("tasks", 1_000_000u64)?;
    let replication_factor = matches.opt_get_default("replication-factor", 3)?;
    // TODO: consistency
    let compression = match matches.opt_str("compression").as_deref() {
        Some("lz4") => Some(Compression::LZ4),
        Some("snappy") => Some(Compression::Snappy),
        Some("none") => None,
        None => None,
        Some(c) => return Err(anyhow!("bad compression: {}", c)),
    };
    let workload = match matches.opt_str("workload").as_deref() {
        Some("write") => Workload::Writes,
        Some("read") => Workload::Reads,
        Some("mixed") => Workload::ReadsAndWrites,
        None => Workload::Writes,
        Some(c) => return Err(anyhow!("bad workload type: {}", c)),
    };

    let session = Session::connect(node, compression).await?;

    if matches.opt_present("prepare") {
        setup_schema(session, replication_factor).await?;
    } else {
        run_bench(session, concurrency, tasks, workload).await?;
    }

    Ok(())
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}

async fn setup_schema(session: Session, replication_factor: u32) -> Result<()> {
    use std::time::Duration;

    let create_keyspace_text = format!("CREATE KEYSPACE IF NOT EXISTS ks_rust_scylla_bench WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {}}}", replication_factor);
    session.query(create_keyspace_text, &[]).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    session
        .query("DROP TABLE IF EXISTS ks_rust_scylla_bench.t", &[])
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    session
        .query(
            "CREATE TABLE ks_rust_scylla_bench.t (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
            &[],
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("Schema set up!");
    Ok(())
}

async fn run_bench(
    session: Session,
    concurrency: u64,
    mut tasks: u64,
    workload: Workload,
) -> Result<()> {
    if workload == Workload::ReadsAndWrites {
        tasks /= 2;
    }

    let sem = Arc::new(Semaphore::new(concurrency as usize));
    let session = Arc::new(session);

    let mut prev_percent = -1;

    let stmt_read = session
        .prepare("SELECT v1, v2 FROM ks_rust_scylla_bench.t WHERE pk = ?")
        .await?;
    let stmt_write = session
        .prepare("INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)")
        .await?;

    for i in 0..tasks {
        let curr_percent = (100 * i) / tasks;
        if prev_percent < curr_percent as i32 {
            prev_percent = curr_percent as i32;
            println!("Progress: {}%", curr_percent);
        }
        let session = session.clone();
        let permit = sem.clone().acquire_owned().await;

        let stmt_read = stmt_read.clone();
        let stmt_write = stmt_write.clone();
        tokio::task::spawn(async move {
            let i = i;

            if workload == Workload::Writes || workload == Workload::ReadsAndWrites {
                session
                    .execute(&stmt_write, &scylla::values!(i, 2 * i, 3 * i))
                    .await
                    .unwrap();
            }

            if workload == Workload::Reads || workload == Workload::ReadsAndWrites {
                let row = session
                    .execute(&stmt_read, &scylla::values!(i))
                    .await
                    .unwrap()
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap();

                assert_eq!(
                    row.columns,
                    vec![
                        Some(CQLValue::BigInt(2 * i as i64)),
                        Some(CQLValue::BigInt(3 * i as i64))
                    ]
                )
            }

            let _permit = permit;
        });
    }

    // Wait for all in-flight requests to finish
    for _ in 0..concurrency {
        sem.acquire().await.forget();
    }

    println!("Done!");
    Ok(())
}
