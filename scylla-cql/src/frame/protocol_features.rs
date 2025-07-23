//! Implementation and negotiation of extensions to the CQL protocol.

use std::borrow::Cow;
use std::collections::HashMap;

const RATE_LIMIT_ERROR_EXTENSION: &str = "SCYLLA_RATE_LIMIT_ERROR";
/// The extension used to add metadata for LWT optimization.
/// See [ProtocolFeatures::lwt_optimization_meta_bit_mask] and
/// [related issue](https://github.com/scylladb/scylla-rust-driver/issues/100)
/// for more details.
pub const SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION: &str = "SCYLLA_LWT_ADD_METADATA_MARK";
/// The key of the single entry of the LWT optimization extension,
/// which entry is a bit mask for the frame flags used to mark LWT frames.
pub const LWT_OPTIMIZATION_META_BIT_MASK_KEY: &str = "LWT_OPTIMIZATION_META_BIT_MASK";
const TABLETS_ROUTING_V1_KEY: &str = "TABLETS_ROUTING_V1";

/// Which protocol extensions are supported by the server.
///
/// This is used to inform the server about the features that the client supports,
/// so that the server can adjust its behavior accordingly.
///
/// So to draw the picture:
/// - server responds to an `OPTIONS` frame with a `SUPPORTED` frame with the list of
///   protocol features it supports;
/// - client parses the `SUPPORTED` frame by extracting extensions it recognizes (supports)
///   and creates a `ProtocolFeatures` instance;
/// - from now on, client uses this instance to determine how to handle certain frames,
///   e.g. whether to expect a rate limit error or how to handle LWT operations;
/// - client also adds the features it supports to the `STARTUP` frame and sends it to
///   the server, which finishes the extensions negotiation process.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProtocolFeatures {
    /// The error code to use for rate limit errors, if negotiated.
    pub rate_limit_error: Option<i32>,

    /// The bit mask used for the LWT optimization, if negotiated.
    /// This is used to mark PREPARED response frames as related to LWT operations
    /// in order to to optimize the handling of LWT requests.
    ///
    /// The mask is ANDed with the flags of the PREPARED response frame,
    /// and if the result is equal to the mask, it means that the frame is related
    /// to an LWT operation.
    pub lwt_optimization_meta_bit_mask: Option<u32>,

    /// Whether the server supports tablets routing v1.
    pub tablets_v1_supported: bool,
}

// TODO: Log information about options which failed to parse

impl ProtocolFeatures {
    /// Parses the supported protocol features from the `supported` map.
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

    /// Adds the protocol features as STARTUP options.
    pub fn add_startup_options(&self, options: &mut HashMap<Cow<'_, str>, Cow<'_, str>>) {
        if self.rate_limit_error.is_some() {
            options.insert(Cow::Borrowed(RATE_LIMIT_ERROR_EXTENSION), Cow::Borrowed(""));
        }
        if let Some(mask) = self.lwt_optimization_meta_bit_mask {
            options.insert(
                Cow::Borrowed(SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION),
                Cow::Owned(format!("{LWT_OPTIMIZATION_META_BIT_MASK_KEY}={mask}")),
            );
        }

        if self.tablets_v1_supported {
            options.insert(Cow::Borrowed(TABLETS_ROUTING_V1_KEY), Cow::Borrowed(""));
        }
    }

    /// Checks if the given flags of a PREPARED response contain the LWT optimization mark.
    ///
    /// If the extension was not negotiated, it conservatively returns `false`.
    pub fn prepared_flags_contain_lwt_mark(&self, flags: u32) -> bool {
        self.lwt_optimization_meta_bit_mask
            .map(|mask| (flags & mask) == mask)
            .unwrap_or(false)
    }
}
