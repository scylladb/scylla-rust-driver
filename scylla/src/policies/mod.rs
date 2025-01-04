//! This module holds policies, which are entities that allow configuring
//! the driver's behaviour in various aspects. The common feature of all policies
//! is that users can implement a policy on their own (because they simply need
//! to implement a certain trait), allowing flexible customizability of the driver.
//!
//! This includes:
//! - HostFilter, which is a way to filter out some nodes and thus
//!   not contact them at all on any condition.
//! - TODO

pub mod host_filter;
