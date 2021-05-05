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
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read inet from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(IpAddr,)>() {
        let (inet_value,): (IpAddr,) = row?;
    }
}
# Ok(())
# }
```