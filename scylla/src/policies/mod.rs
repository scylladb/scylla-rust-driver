//! This module holds policies, which are entities that allow configuring
//! the driver's behaviour in various aspects. The common feature of all policies
//! is that users can implement a policy on their own (because they simply need
//! to implement a certain trait), allowing flexible customizability of the driver.
//!
//! This includes:
//! - HostFilter, which is a way to filter out some nodes and thus
//!   not contact them at all on any condition.
//! - AddressTranslator, which allows contacting a node through a different address
//!   than its broadcast address (e.g., when it's behind a NAT).
//! - LoadBalancingPolicy, which decides which nodes and shards to contact for each
//!   request.
//! - SpeculativeExecutionPolicy, which decides if the driver will send speculative
//!   requests to the next hosts when the current host takes too long to respond.
//! - RetryPolicy, which decides whether and how to retry a request.
//! - TODO

pub mod address_translator;
pub mod host_filter;
#[cfg(all(scylla_unstable, feature = "unstable-host-listener"))]
pub mod host_listener;
#[cfg(not(all(scylla_unstable, feature = "unstable-host-listener")))]
pub(crate) mod host_listener;
pub mod load_balancing;
#[cfg(all(scylla_unstable, feature = "unstable-reconnect-policy"))]
pub mod reconnect;
#[cfg(not(all(scylla_unstable, feature = "unstable-reconnect-policy")))]
pub(crate) mod reconnect;
pub mod retry;
pub mod speculative_execution;
pub mod timestamp_generator;

/// Returns the name of `T` with the leading module path of the outermost type stripped.
///
/// Supplies the `name` that the driver configuration report sends for policies
/// the driver does not recognise, so it must be allocation-free and never empty.
pub(crate) fn simple_type_name<T: ?Sized>() -> &'static str {
    let full = std::any::type_name::<T>();
    // Only the outermost type's path may be stripped: in `a::b::Foo<some::Bar>`
    // the `::` inside the generic arguments must be left alone.
    let head = match full.find('<') {
        Some(idx) => &full[..idx],
        None => full,
    };
    match head.rfind("::") {
        // The `name` field is declared non-empty by the schema, so fall back to
        // the unstripped name rather than returning "".
        Some(idx) if idx + 2 < full.len() => &full[idx + 2..],
        _ => full,
    }
}

#[cfg(test)]
mod tests {
    use super::simple_type_name;
    use crate::policies::load_balancing::DefaultPolicy;
    use crate::policies::retry::{DefaultRetryPolicy, RetryPolicy};

    #[test]
    fn test_simple_type_name() {
        // Names without a module path are returned as they are.
        assert_eq!(simple_type_name::<u32>(), "u32");
        assert_eq!(simple_type_name::<&str>(), "&str");

        // The leading path of the outermost type is stripped...
        assert_eq!(simple_type_name::<String>(), "String");
        assert_eq!(
            simple_type_name::<DefaultRetryPolicy>(),
            "DefaultRetryPolicy"
        );
        assert_eq!(simple_type_name::<DefaultPolicy>(), "DefaultPolicy");

        // ...including for the trait objects the policy traits pass in as `Self`.
        assert_eq!(simple_type_name::<dyn RetryPolicy>(), "RetryPolicy");

        // ...but the paths inside the generic arguments are left alone.
        assert_eq!(
            simple_type_name::<Vec<String>>(),
            "Vec<alloc::string::String>"
        );
    }
}
