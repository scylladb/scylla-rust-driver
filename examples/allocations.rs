use anyhow::Result;
use scylla::{statement::prepared_statement::PreparedStatement, LegacySession, SessionBuilder};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Barrier;

use stats_alloc::{Stats, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;

use clap::{ArgEnum, Parser};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(arg_enum, value_parser, default_value = "all")]
    mode: Mode,

    #[clap(short, long, value_parser, default_value = "127.0.0.1:9042")]
    node: String,

    #[clap(short, long, value_parser, default_value_t = 256usize)]
    parallelism: usize,

    #[clap(short, long, value_parser, default_value_t = 100_000usize)]
    requests: usize,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ArgEnum)]
enum Mode {
    All,
    Insert,
    Select,
}

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn print_stats(stats: &stats_alloc::Stats, reqs: f64) {
    println!(
        "allocs/req:            {:9.2}",
        stats.allocations as f64 / reqs
    );
    println!(
        "reallocs/req:          {:9.2}",
        stats.reallocations as f64 / reqs
    );
    println!(
        "frees/req:             {:9.2}",
        stats.deallocations as f64 / reqs
    );
    println!(
        "bytes allocated/req:   {:9.2}",
        stats.bytes_allocated as f64 / reqs
    );
    println!(
        "bytes reallocated/req: {:9.2}",
        stats.bytes_reallocated as f64 / reqs
    );
    println!(
        "bytes freed/req:       {:9.2}",
        stats.bytes_deallocated as f64 / reqs
    );
}

async fn measure(
    session: Arc<LegacySession>,
    prepared: Arc<PreparedStatement>,
    reqs: usize,
    parallelism: usize,
) -> Stats {
    let barrier = Arc::new(Barrier::new(parallelism + 1));
    let counter = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let session = session.clone();
        let prepared = prepared.clone();
        let barrier = barrier.clone();
        let counter = counter.clone();
        tasks.push(tokio::task::spawn(async move {
            barrier.wait().await;
            barrier.wait().await;

            loop {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                if i >= reqs {
                    break;
                }
                if i % 10000 == 0 {
                    print!(".");
                    std::io::stdout().flush().unwrap();
                }
                session
                    .execute_unpaged(&prepared, (i as i32, 2 * i as i32))
                    .await
                    .unwrap();
            }

            barrier.wait().await;
            barrier.wait().await;
        }));
    }

    barrier.wait().await;
    let initial_stats = GLOBAL.stats();
    barrier.wait().await;

    barrier.wait().await;
    let final_stats = GLOBAL.stats();
    barrier.wait().await;

    // Wait until all tasks are cleaned up
    for t in tasks {
        t.await.unwrap();
    }

    println!();

    final_stats - initial_stats
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!("{:?}", args);

    println!("Connecting to {} ...", args.node);

    let session: LegacySession = SessionBuilder::new()
        .known_node(args.node)
        .build_legacy()
        .await?;
    let session = Arc::new(session);

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;
    session.await_schema_agreement().await.unwrap();

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.allocations (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session.await_schema_agreement().await.unwrap();

    let prepared_inserts = Arc::new(
        session
            .prepare("INSERT INTO examples_ks.allocations (a, b, c) VALUES (?, ?, 'abc')")
            .await?,
    );

    let prepared_selects = Arc::new(
        session
            .prepare("SELECT * FROM examples_ks.allocations WHERE a = ? and b = ?")
            .await?,
    );

    if args.mode == Mode::All || args.mode == Mode::Insert {
        print!("Sending {} inserts, hold tight ", args.requests);
        let write_stats = measure(
            session.clone(),
            prepared_inserts.clone(),
            args.requests,
            args.parallelism,
        )
        .await;
        println!("----------");
        println!("Inserts:");
        println!("----------");
        print_stats(&write_stats, args.requests as f64);
        println!("----------");
    }

    if args.mode == Mode::All || args.mode == Mode::Select {
        print!("Sending {} selects, hold tight ", args.requests);
        let read_stats = measure(
            session.clone(),
            prepared_selects.clone(),
            args.requests,
            args.parallelism,
        )
        .await;
        println!("----------");
        println!("Selects:");
        println!("----------");
        print_stats(&read_stats, args.requests as f64);
        println!("----------");
    }

    Ok(())
}
