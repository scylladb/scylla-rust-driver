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
pub mod load_balancing;
pub mod retry;
pub mod speculative_execution;
