use anyhow::Result;
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    response::rows_displayer::ByteDisplaying,
};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    // prepare the session
    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.basic (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic (a, b, c) VALUES (?, ?, ?)",
            (3, 4, "lorem Ipsum jest tekstem stosowanym jako przykładowy wypełniacz w przemyśle poligraficznym. Został po raz pierwszy użyty w XV w. przez nieznanego drukarza do wypełnienia tekstem próbnej książki. Pięć wieków później zaczął być używany przemyśle elektronicznym,"),
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic (a, b, c) VALUES (1, 2, 'abc')",
            &[],
        )
        .await?;

    let prepared = session
        .prepare("INSERT INTO examples_ks.basic (a, b, c) VALUES (?, 7, ?)")
        .await?;
    session
        .execute_unpaged(&prepared, (42_i32, "I'm prepared!"))
        .await?;
    session
        .execute_unpaged(&prepared, (43_i32, "I'm prepared 2!"))
        .await?;
    session
        .execute_unpaged(&prepared, (44_i32, "I'm prepared 3!"))
        .await?;

    // example 1 - basic table

    // fetch the data

    // in real life scenario you should always fetch data in pages for maximum performance
    let result = session
        .query_unpaged("SELECT a, b, c FROM examples_ks.basic", &[])
        .await?
        .into_rows_result()?;

    let displayer = result.rows_displayer();
    println!("\nlong text and special characters:");
    println!("{}", displayer);

    // example 2 - blob, double, float, time, timestamp
    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.basic4 (a int, b int, c text, d int, timest timestamp, bytes blob, fl float, db double, time1 time, primary key (a, c))",
        &[],
    )
    .await?;

    session
    .query_unpaged(
        "INSERT INTO examples_ks.basic4 
        (a, b, c, d, timest, bytes, fl, db, time1) 
        VALUES 
        (1, 10, 'example text', 3, toTimestamp(now()), textAsBlob('sample bytes'), 3.14, 2.718281, '14:30:00');",
        &[],
    )
    .await?;

    // fetch the data

    // in real life scenario you should always fetch data in pages for maximum performance
    let result = session
        .query_unpaged("SELECT * FROM examples_ks.basic4", &[])
        .await?
        .into_rows_result()?;

    let mut displayer = result.rows_displayer();
    displayer.set_blob_displaying(ByteDisplaying::Ascii);
    println!("\nblob, double, float, time, timestamp:");
    println!("{}", displayer);

    // example 3 - date, duration, ip address, timeuuid

    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.basic6 (a int, timud timeuuid, date1 date, ipaddr inet, dur duration, primary key (a))",
        &[],
    )
    .await?;

    // insert some data
    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic6 
        (a, timud, date1, ipaddr, dur) 
        VALUES 
        (1, now(), '2021-01-01', '3.14.15.9', 1h);",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic6 
        (a, timud, date1, ipaddr, dur) 
        VALUES
        (3, NOW(), '2024-01-15', '128.0.0.1', 89h4m48s137ms);", // cqlsh prints this as 89.08003805555556h4.8022833333333335m48.137s137.0ms
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic6 
        (a, timud, date1, ipaddr, dur) 
        VALUES
        (4, NOW(), '2024-01-15', '192.168.0.14', 13y2w89h4m48s137ms);",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic6 (a, timud, date1, ipaddr, dur) 
        VALUES (2, NOW(), '2024-02-20', '2001:0db8:0:0::1428:57ab', 5d2h);",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.basic6 (a, timud, date1, ipaddr, dur) 
        VALUES (5, NOW(), '-250000-02-20', '2001:db8::1428:57ab', 1y1mo1w1d1h1m1s700ms);",
            &[],
        )
        .await?;

    // fetch the data

    // in real life scenario you should always fetch data in pages for maximum performance
    let result = session
        .query_unpaged("SELECT * FROM examples_ks.basic6", &[])
        .await?
        .into_rows_result()?;

    let mut displayer = result.rows_displayer();
    println!("\ndate, duration, ip address, timeuuid:");
    println!("{}", displayer);

    displayer.set_terminal_width(80);
    displayer.use_color(false);
    println!("\nno color, width = 80:");
    println!("{}", displayer);

    // example 4 - List
    // Create a table with a list of text
    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.upcoming_calendar ( year int, month int, events list<text>, PRIMARY KEY ( year, month) )",
        &[],
    )
    .await?;

    // Insert some data
    session
    .query_unpaged(
        "INSERT INTO examples_ks.upcoming_calendar(year, month, events) VALUES (2015, 6, ['e1', 'e2', 'e3'])",
        &[],
    )
    .await?;

    // fetch the data

    // in real life scenario you should always fetch data in pages for maximum performance
    let result = session
        .query_unpaged("SELECT * FROM examples_ks.upcoming_calendar", &[])
        .await?
        .into_rows_result()?;

    let displayer = result.rows_displayer();
    println!("\nList:");
    println!("{}", displayer);

    // example 5 - map
    // Create a table with a list of text
    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.cyclist_teams ( id UUID PRIMARY KEY, lastname text, firstname text, teams map<int,text> )",
        &[],
    )
    .await?;

    // Insert some data
    session
    .query_unpaged(
        "INSERT INTO examples_ks.cyclist_teams (id, lastname, firstname, teams) 
        VALUES (
        5b6962dd-3f90-4c93-8f61-eabfa4a803e2,
        'VOS', 
        'Marianne', 
        {2015 : 'Rabobank-Liv Woman Cycling Team', 2014 : 'Rabobank-Liv Woman Cycling Team', 2013 : 'Rabobank-Liv Giant', 
            2012 : 'Rabobank Women Team', 2011 : 'Nederland bloeit' })",
                &[],
            )
    .await?;

    // fetch the data

    // in real life scenario you should always fetch data in pages for maximum performance
    let result = session
        .query_unpaged("SELECT * FROM examples_ks.cyclist_teams", &[])
        .await?
        .into_rows_result()?;

    let displayer = result.rows_displayer();
    println!("\nMap:");
    println!("{}", displayer);

    // example 6 - set
    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.cyclist_career_teams ( id UUID PRIMARY KEY, lastname text, teams set<text> );",
        &[],
    )
    .await?;

    // Insert some data
    session
    .query_unpaged(
        "INSERT INTO examples_ks.cyclist_career_teams (id,lastname,teams) 
        VALUES (5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 'VOS', 
        { 'Rabobank-Liv Woman Cycling Team','Rabobank-Liv Giant','Rabobank Women Team','Nederland bloeit' } )",
                &[],
            )
    .await?;

    // fetch the data

    // in real life scenario you should always fetch data in pages for maximum performance
    let result = session
        .query_unpaged("SELECT * FROM examples_ks.cyclist_career_teams", &[])
        .await?
        .into_rows_result()?;

    let displayer = result.rows_displayer();
    println!("\nSet:");
    println!("{}", displayer);

    // example 7 - user defined type
    session
        .query_unpaged(
            "CREATE TYPE IF NOT EXISTS examples_ks.basic_info (
        birthday timestamp,
        nationality text,
        weight text,
        height text
        )",
            &[],
        )
        .await?;

    // make table
    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.cyclist_stats ( id uuid PRIMARY KEY, lastname text, basics FROZEN<basic_info>)",
        &[],
    )
    .await?;

    // Insert some data
    session
        .query_unpaged(
            "INSERT INTO examples_ks.cyclist_stats (id, lastname, basics) VALUES (
        e7ae5cf3-d358-4d99-b900-85902fda9bb0, 
        'FRAME', 
        { birthday : '1993-06-18', nationality : 'New Zealand', weight : null, height : null }
        )",
            &[],
        )
        .await?;

    // fetch the data

    // in real life scenario you should always fetch data in pages for maximum performance
    let result = session
        .query_unpaged("SELECT * FROM examples_ks.cyclist_stats", &[])
        .await?
        .into_rows_result()?;

    let displayer = result.rows_displayer();
    println!("\nUser defined type:");
    println!("{}", displayer);

    // example 8 - tuples

    // make table
    session
    .query_unpaged(
        "CREATE TABLE IF NOT EXISTS examples_ks.route (race_id int, race_name text, point_id int, lat_long tuple<text, tuple<float,float>>, PRIMARY KEY (race_id, point_id))",
        &[],
    )
    .await?;

    // Insert some data
    session
    .query_unpaged(
        "INSERT INTO examples_ks.route (race_id, race_name, point_id, lat_long) VALUES (500, '47th Tour du Pays de Vaud', 2, ('Champagne', (46.833, 6.65)))",
                &[],
            )
    .await?;

    // fetch the data

    // in real life scenario you should always fetch data in pages for maximum performance
    let result = session
        .query_unpaged("SELECT * FROM examples_ks.route", &[])
        .await?
        .into_rows_result()?;

    let displayer = result.rows_displayer();
    println!("\nTuples:");
    println!("{}", displayer);

    Ok(())
}
