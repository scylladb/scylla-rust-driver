//! Verifies that the `serde` feature of this crate really reaches the CQL value
//! types. Nothing in the driver itself uses those derives, so without this the
//! feature could silently stop enabling them.

use crate::value::{Counter, CqlDuration};

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Stats {
    hits: Counter,
    uptime: CqlDuration,
}

#[test]
fn value_types_are_serializable() {
    let stats = Stats {
        hits: Counter(42),
        uptime: CqlDuration {
            months: 1,
            days: 2,
            nanoseconds: 3,
        },
    };

    let json = serde_json::to_string(&stats).unwrap();
    assert_eq!(
        json,
        r#"{"hits":42,"uptime":{"months":1,"days":2,"nanoseconds":3}}"#
    );
    assert_eq!(serde_json::from_str::<Stats>(&json).unwrap(), stats);
}
