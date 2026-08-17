use anyhow::Result;
use scylla::DeserializeRow;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::ColumnType;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.custom_deserialization (pk int primary key, v text)",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.custom_deserialization (pk, v) VALUES (1, 'asdf')",
            (),
        )
        .await?;

    // You can implement DeserializeValue for your own types
    #[derive(PartialEq, Eq, Debug)]
    struct MyType<'a>(&'a str);

    // The value may borrow for any lifetime shorter than the response frame.
    impl<'frame, 'metadata, 'a> DeserializeValue<'frame, 'metadata> for MyType<'a>
    where
        'frame: 'a,
    {
        fn type_check(
            typ: &scylla::frame::response::result::ColumnType,
        ) -> std::result::Result<(), scylla::deserialize::TypeCheckError> {
            <&str as DeserializeValue<'frame, 'metadata>>::type_check(typ)
        }

        fn deserialize(
            typ: &'metadata ColumnType<'metadata>,
            v: Option<scylla::deserialize::FrameSlice<'frame>>,
        ) -> std::result::Result<Self, scylla::deserialize::DeserializationError> {
            let s = <&str as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;

            Ok(Self(s))
        }
    }

    let rows_result = session
        .query_unpaged(
            "SELECT v FROM examples_ks.custom_deserialization WHERE pk = 1",
            (),
        )
        .await?
        .into_rows_result()?;

    // DeserializeRow can be derived for a struct representing a whole row.
    #[derive(Debug, DeserializeRow)]
    struct MyRow<'a> {
        // Field name must correspond to the CQL column's name.
        v: MyType<'a>,
    }

    let row = rows_result.single_row::<MyRow>()?;
    assert_eq!(row.v, MyType("asdf"));

    println!("Ok.");

    Ok(())
}
