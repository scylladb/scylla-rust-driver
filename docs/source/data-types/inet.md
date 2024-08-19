# Inet
`Inet` is represented as `std::net::IpAddr`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use std::net::{IpAddr, Ipv4Addr};

// Insert some ip address into the table
let to_insert: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read inet from the table
let result = session.query_unpaged("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows_typed::<(IpAddr,)>()?;
while let Some((inet_value,)) = iter.next().transpose()? {
    println!("{:?}", inet_value);
}
# Ok(())
# }
```