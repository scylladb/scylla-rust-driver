use darling::FromMeta;

pub(crate) mod cql;
pub(crate) mod row;

#[derive(Copy, Clone, PartialEq, Eq)]
enum Flavor {
    MatchByName,
    EnforceOrder,
}

impl FromMeta for Flavor {
    fn from_string(value: &str) -> darling::Result<Self> {
        match value {
            "match_by_name" => Ok(Self::MatchByName),
            "enforce_order" => Ok(Self::EnforceOrder),
            _ => Err(darling::Error::unknown_value(value)),
        }
    }
}
