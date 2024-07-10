use crate::frame::frame_errors::ParseError;

use crate::frame::request::{RequestOpcode, SerializableRequest};

pub struct Options;

impl SerializableRequest for Options {
    const OPCODE: RequestOpcode = RequestOpcode::Options;

    fn serialize(&self, _buf: &mut Vec<u8>) -> Result<(), ParseError> {
        Ok(())
    }
}

/* Key names for options in SUPPORTED/STARTUP */
pub const SCYLLA_SHARD_AWARE_PORT: &str = "SCYLLA_SHARD_AWARE_PORT";
pub const SCYLLA_SHARD_AWARE_PORT_SSL: &str = "SCYLLA_SHARD_AWARE_PORT_SSL";

pub const COMPRESSION: &str = "COMPRESSION";
pub const CQL_VERSION: &str = "CQL_VERSION";
pub const DRIVER_NAME: &str = "DRIVER_NAME";
pub const DRIVER_VERSION: &str = "DRIVER_VERSION";

/* Value names for options in SUPPORTED/STARTUP */
pub const DEFAULT_CQL_PROTOCOL_VERSION: &str = "4.0.0";
pub const DEFAULT_DRIVER_NAME: &str = "ScyllaDB Rust Driver";
pub const DEFAULT_DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");
