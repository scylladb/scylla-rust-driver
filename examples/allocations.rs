use anyhow::Result;
use scylla::{statement::prepared_statement::PreparedStatement, Session, SessionBuilder};
use std::io::Write;
use std::sync::Arc;

use tokio::sync::Semaphore;

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
    session: Arc<Session>,
    prepared: Arc<PreparedStatement>,
    sem: Arc<Semaphore>,
    reqs: usize,
    parallelism: usize,
) -> Stats {
    let initial_stats = GLOBAL.stats();

    for i in 0..reqs {
        if i % 10000 == 0 {
            print!(".");
            std::io::stdout().flush().unwrap();
        }
        let session = session.clone();
        let prepared = prepared.clone();
        let permit = sem.clone().acquire_owned().await;
        tokio::task::spawn(async move {
            let i = i;
            session
                .execute(&prepared, (i as i32, 2 * i as i32))
                .await
                .unwrap();

            let _permit = permit;
        });
    }
    println!();

    // Wait for all in-flight requests to finish
    for _ in 0..parallelism {
        sem.acquire().await.unwrap().forget();
    }

    GLOBAL.stats() - initial_stats
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!("{:?}", args);

    println!("Connecting to {} ...", args.node);

    let session: Session = SessionBuilder::new().known_node(args.node).build().await?;
    let session = Arc::new(session);

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;
    session.await_schema_agreement().await.unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.alloc_test (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session.await_schema_agreement().await.unwrap();

    let prepared_inserts = Arc::new(
        session
            .prepare("INSERT INTO ks.alloc_test (a, b, c) VALUES (?, ?, 'abc')")
            .await?,
    );

    let prepared_selects = Arc::new(
        session
            .prepare("SELECT * FROM ks.alloc_test WHERE a = ? and b = ?")
            .await?,
    );

    let sem = Arc::new(Semaphore::new(args.parallelism));

    if args.mode == Mode::All || args.mode == Mode::Insert {
        print!("Sending {} inserts, hold tight ", args.requests);
        let write_stats = measure(
            session.clone(),
            prepared_inserts.clone(),
            sem.clone(),
            args.requests,
            args.parallelism,
        )
        .await;
        println!("----------");
        println!("Inserts:");
        println!("----------");
        print_stats(&write_stats, args.requests as f64);
        println!("----------");
        sem.add_permits(args.parallelism);
    }

    if args.mode == Mode::All || args.mode == Mode::Select {
        print!("Sending {} selects, hold tight ", args.requests);
        let read_stats = measure(
            session.clone(),
            prepared_selects.clone(),
            sem.clone(),
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
