# Priorities of execution settings

You always have a default execution profile set for the `Session`, either the default one or overridden upon `Session` creation. Moreover, you can set a profile for specific statements, in which case the statement's profile has higher priority. Some options are also available for specific statements to be set directly on them, such as request timeout and consistency. In such case, the directly set options are preferred over those specified in execution profiles.

> **Recap**\
> Priorities are as follows:\
> `Session`'s default profile < Statement's profile < options set directly on a Statement


### Example
Priorities of execution profiles and directly set options:
```rust
# extern crate scylla;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};
use scylla::query::Query;
use scylla::statement::Consistency;
use scylla::transport::ExecutionProfile;

let session_profile = ExecutionProfile::builder()
    .consistency(Consistency::One)
    .build();

let query_profile = ExecutionProfile::builder()
    .consistency(Consistency::Two)
    .build();

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .default_execution_profile_handle(session_profile.into_handle())
    .build()
    .await?;

let mut query = Query::from("SELECT * FROM ks.table");

// Query is not assigned any specific profile, so session's profile is applied.
// Therefore, the query will be executed with Consistency::One.
session.query(query.clone(), ()).await?;

query.set_execution_profile_handle(Some(query_profile.into_handle()));
// Query's profile is applied.
// Therefore, the query will be executed with Consistency::Two.
session.query(query.clone(), ()).await?;

query.set_consistency(Consistency::Three);
// An option is set directly on the query.
// Therefore, the query will be executed with Consistency::Three.
session.query(query, ()).await?;

# Ok(())
# }
```