use std::collections::HashMap;

const RATE_LIMIT_ERROR_EXTENSION: &str = "SCYLLA_RATE_LIMIT_ERROR";
pub const SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION: &str = "SCYLLA_LWT_ADD_METADATA_MARK";
pub const LWT_OPTIMIZATION_META_BIT_MASK_KEY: &str = "LWT_OPTIMIZATION_META_BIT_MASK";
const TABLETS_ROUTING_V1_KEY: &str = "TABLETS_ROUTING_V1";

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProtocolFeatures {
    pub rate_limit_error: Option<i32>,
    pub lwt_optimization_meta_bit_mask: Option<u32>,
    pub tablets_v1_supported: bool,
}

// TODO: Log information about options which failed to parse

impl ProtocolFeatures {
    pub fn parse_from_supported(supported: &HashMap<String, Vec<String>>) -> Self {
        Self {
            rate_limit_error: Self::maybe_parse_rate_limit_error(supported),
            lwt_optimization_meta_bit_mask: Self::maybe_parse_lwt_optimization_meta_bit_mask(
                supported,
            ),
            tablets_v1_supported: Self::check_tablets_routing_v1_support(supported),
        }
    }

    fn maybe_parse_rate_limit_error(supported: &HashMap<String, Vec<String>>) -> Option<i32> {
        let vals = supported.get(RATE_LIMIT_ERROR_EXTENSION)?;
        let code_str = Self::get_cql_extension_field(vals.as_slice(), "ERROR_CODE")?;
        code_str.parse::<i32>().ok()
    }

    fn maybe_parse_lwt_optimization_meta_bit_mask(
        supported: &HashMap<String, Vec<String>>,
    ) -> Option<u32> {
        let vals = supported.get(SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION)?;
        let mask_str =
            Self::get_cql_extension_field(vals.as_slice(), LWT_OPTIMIZATION_META_BIT_MASK_KEY)?;
        mask_str.parse::<u32>().ok()
    }

    fn check_tablets_routing_v1_support(supported: &HashMap<String, Vec<String>>) -> bool {
        supported.contains_key(TABLETS_ROUTING_V1_KEY)
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
        if let Some(mask) = self.lwt_optimization_meta_bit_mask {
            options.insert(
                SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION.to_string(),
                format!("{}={}", LWT_OPTIMIZATION_META_BIT_MASK_KEY, mask),
            );
        }

        if self.tablets_v1_supported {
            options.insert(TABLETS_ROUTING_V1_KEY.to_string(), String::new());
        }
    }

    pub fn prepared_flags_contain_lwt_mark(&self, flags: u32) -> bool {
        self.lwt_optimization_meta_bit_mask
            .map(|mask| (flags & mask) == mask)
            .unwrap_or(false)
    }
}
