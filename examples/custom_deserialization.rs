use anyhow::Result;
use scylla::cql_to_rust::{FromCqlVal, FromCqlValError};
use scylla::frame::response::result::CqlValue;
use scylla::macros::impl_from_cql_value_from_method;
use scylla::{LegacySession, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: LegacySession = SessionBuilder::new().known_node(uri).build_legacy().await?;

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

    // You can implement FromCqlVal for your own types
    #[derive(PartialEq, Eq, Debug)]
    struct MyType(String);

    impl FromCqlVal<CqlValue> for MyType {
        fn from_cql(cql_val: CqlValue) -> Result<Self, FromCqlValError> {
            Ok(Self(
                cql_val.into_string().ok_or(FromCqlValError::BadCqlType)?,
            ))
        }
    }

    let (v,) = session
        .query_unpaged(
            "SELECT v FROM examples_ks.custom_deserialization WHERE pk = 1",
            (),
        )
        .await?
        .single_row_typed::<(MyType,)>()?;
    assert_eq!(v, MyType("asdf".to_owned()));

    // If you defined an extension trait for CqlValue then you can use
    // the `impl_from_cql_value_from_method` macro to turn it into
    // a FromCqlValue impl
    #[derive(PartialEq, Eq, Debug)]
    struct MyOtherType(String);

    trait CqlValueExt {
        fn into_my_other_type(self) -> Option<MyOtherType>;
    }

    impl CqlValueExt for CqlValue {
        fn into_my_other_type(self) -> Option<MyOtherType> {
            Some(MyOtherType(self.into_string()?))
        }
    }

    impl_from_cql_value_from_method!(MyOtherType, into_my_other_type);

    let (v,) = session
        .query_unpaged(
            "SELECT v FROM examples_ks.custom_deserialization WHERE pk = 1",
            (),
        )
        .await?
        .single_row_typed::<(MyOtherType,)>()?;
    assert_eq!(v, MyOtherType("asdf".to_owned()));

    println!("Ok.");

    Ok(())
}
