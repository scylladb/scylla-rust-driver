use std::collections::HashMap;

const RATE_LIMIT_ERROR_EXTENSION: &str = "SCYLLA_RATE_LIMIT_ERROR";

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProtocolFeatures {
    pub rate_limit_error: Option<i32>,
}

// TODO: Log information about options which failed to parse

impl ProtocolFeatures {
    pub fn parse_from_supported(supported: &HashMap<String, Vec<String>>) -> Self {
        Self {
            rate_limit_error: Self::maybe_parse_rate_limit_error(supported),
        }
    }

    fn maybe_parse_rate_limit_error(supported: &HashMap<String, Vec<String>>) -> Option<i32> {
        let vals = supported.get(RATE_LIMIT_ERROR_EXTENSION)?;
        let code_str = Self::get_cql_extension_field(vals.as_slice(), "ERROR_CODE")?;
        code_str.parse::<i32>().ok()
    }

    // Looks up a field which starts with `key=` and returns the rest
    fn get_cql_extension_field<'a>(vals: &'a [String], key: &str) -> Option<&'a str> {
        vals.iter()
            .find_map(|v| v.as_str().strip_prefix(key)?.strip_prefix('='))
    }

    pub fn add_startup_options(&self, options: &mut HashMap<String, String>) {
        if self.rate_limit_error.is_some() {
            options.insert(RATE_LIMIT_ERROR_EXTENSION.to_string(), String::new());
        }
    }
}
