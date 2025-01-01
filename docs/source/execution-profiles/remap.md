# Remapping execution profile handles

`ExecutionProfileHandle`s can be remapped to another `ExecutionProfile`, and the change affects all sessions and statements that have been assigned that handle. This enables quick workload switches.

Example mapping:
* session1 -> handle1 -> profile1
* statement1 -> handle1 -> profile1
* statement2 -> handle2 -> profile2

We can now remap handle2 to profile1, so that the mapping for statement2 becomes as follows:
* statement2 -> handle2 -> profile1

We can also change statement1's handle to handle2, and remap handle1 to profile2, yielding:
* session1 -> handle1 -> profile2
* statement1 -> handle2 -> profile1
* statement2 -> handle2 -> profile1

As you can see, profiles are a powerful and convenient way to define and modify your workloads.

### Example
Below, the remaps described above are followed in code.
```rust
# extern crate scylla;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};
use scylla::query::Query;
use scylla::statement::Consistency;
use scylla::execution::ExecutionProfile;

let profile1 = ExecutionProfile::builder()
    .consistency(Consistency::One)
    .build();

let profile2 = ExecutionProfile::builder()
    .consistency(Consistency::Two)
    .build();

let mut handle1 = profile1.clone().into_handle();
let mut handle2 = profile2.clone().into_handle();

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .default_execution_profile_handle(handle1.clone())
    .build()
    .await?;

let mut query1 = Query::from("SELECT * FROM ks.table");
let mut query2 = Query::from("SELECT pk FROM ks.table WHERE pk = ?");

query1.set_execution_profile_handle(Some(handle1.clone()));
query2.set_execution_profile_handle(Some(handle2.clone()));

// session1 -> handle1 -> profile1
// query1 -> handle1 -> profile1
// query2 -> handle2 -> profile2

// We can now remap handle2 to profile1:
handle2.map_to_another_profile(profile1);
// ...so that the mapping for query2 becomes as follows:
// query2 -> handle2 -> profile1

// We can also change query1's handle to handle2:
query1.set_execution_profile_handle(Some(handle2.clone()));
// ...and remap handle1 to profile2:
handle1.map_to_another_profile(profile2);
// ...yielding:
// session1 -> handle1 -> profile2
// query1 -> handle2 -> profile1
// query2 -> handle2 -> profile1

# Ok(())
# }
```