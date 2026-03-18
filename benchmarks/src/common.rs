use std::{
    env,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
};

use chrono::Local;
use scylla::{
    DeserializeValue, SerializeValue,
    client::{caching_session::CachingSession, session::Session, session_builder::SessionBuilder},
    value::{CqlDuration, CqlTimeuuid},
};
use uuid::Uuid;

const DEFAULT_CACHE_SIZE: u32 = 512;

#[allow(unused)]
pub(crate) const SIMPLE_INSERT_QUERY: &str = "INSERT INTO benchmarks.basic (id, val) VALUES (?, ?)";
#[allow(unused)]
pub(crate) const DESER_INSERT_QUERY: &str = "INSERT INTO benchmarks.basic (id, val, tuuid, ip, date, time, tuple, udt, set1, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

async fn init_common(schema: &str) -> Result<Session, Box<dyn std::error::Error>> {
    let uri: String = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    let session = SessionBuilder::new().known_node(uri).build().await?;

    session
    .query_unpaged(
        "CREATE KEYSPACE IF NOT EXISTS benchmarks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }", 
        &[],
    )
    .await?;

    session
        .query_unpaged("DROP TABLE IF EXISTS benchmarks.basic", &[])
        .await?;

    session.query_unpaged(schema, &[]).await?;
    Ok(session)
}

// This may be imported by binaries that use only one of the helpers
#[allow(dead_code)]
pub(crate) async fn init_simple_table_caching() -> Result<CachingSession, Box<dyn std::error::Error>>
{
    Ok(CachingSession::from(
        init_common("CREATE TABLE benchmarks.basic (id uuid, val int, PRIMARY KEY(id))").await?,
        DEFAULT_CACHE_SIZE as usize,
    ))
}

#[allow(dead_code)]
pub(crate) async fn init_simple_table() -> Result<Session, Box<dyn std::error::Error>> {
    init_common("CREATE TABLE benchmarks.basic (id uuid, val int, PRIMARY KEY(id))").await
}

// This may be imported by binaries that use only one of the helpers
#[allow(dead_code)]
pub(crate) async fn init_deser_table() -> Result<Session, Box<dyn std::error::Error>> {
    let session =
        init_common("CREATE TYPE IF NOT EXISTS benchmarks.udt1 (field1 text, field2 int)").await;
    if let Ok(s) = &session {
        s.query_unpaged("CREATE TABLE benchmarks.basic (id uuid, val int, tuuid timeuuid, ip inet, date date, time time, tuple frozen<tuple<text, int>>, udt frozen<udt1>, set1 set<int>, duration duration, PRIMARY KEY(id))", &[]).await?;
    }

    session
}

#[derive(Debug, DeserializeValue, SerializeValue)]
pub(crate) struct Udt1 {
    field1: String,
    field2: i32,
}

type DeserTuple = (
    Uuid,
    i32,
    CqlTimeuuid,
    IpAddr,
    chrono::NaiveDate,
    chrono::NaiveTime,
    (&'static str, i32),
    Udt1,
    Vec<i32>,
    CqlDuration,
);

// This may be imported by binaries that use only one of the helpers
#[allow(dead_code)]
pub(crate) fn get_deser_data() -> DeserTuple {
    let id = Uuid::new_v4();
    let tuuid = CqlTimeuuid::from_str("8e14e760-7fa8-11eb-bc66-000000000001").unwrap();
    let ip: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1));
    let now = Local::now();
    let date = now.date_naive();
    let time = now.time();
    let tuple = (
        "Litwo! Ojczyzno moja! ty jesteś jak zdrowie: Ile cię trzeba cenić, ten tylko się dowie, Kto cię stracił. Dziś piękność twą w całej ozdobie Widzę i opisuję, bo tęsknię po tobie.",
        1,
    );
    let udt = Udt1{ field1: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis congue egestas sapien id maximus eget.".to_owned(), field2: 4321 };
    let set = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 11];
    let duration = CqlDuration {
        months: 1,
        days: 2,
        nanoseconds: 3,
    };
    (id, 100, tuuid, ip, date, time, tuple, udt, set, duration)
}

// This may be imported by binaries that use only one of the helpers
#[allow(dead_code)]
pub(crate) fn get_cnt() -> i32 {
    env::var("CNT")
        .ok()
        .and_then(|s: String| s.parse::<i32>().ok())
        .expect("CNT parameter is required.")
}

// This may be imported by binaries that use only one of the helpers
#[allow(dead_code)]
pub(crate) async fn check_row_cnt(
    session: &Session,
    n: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let select_query = "SELECT COUNT(1) FROM benchmarks.basic USING TIMEOUT 120s;";

    assert_eq!(
        session
            .query_unpaged(select_query, &[])
            .await?
            .into_rows_result()?
            .first_row::<(i64,)>()?
            .0,
        n.into()
    );
    Ok(())
}
